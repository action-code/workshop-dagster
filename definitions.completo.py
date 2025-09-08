import os
import time
import json
import base64
import boto3
import requests
import subprocess
import tempfile
from openai import OpenAI
from google import genai
from dotenv import load_dotenv
from google.genai import types
from utils_boto3 import s3, paths, apipath, openai_client, genai_client
from dagster import asset, Definitions, define_asset_job, ScheduleDefinition


# Carrega as senhas da Openai e do Google Cloud do arquivo .env
load_dotenv()

# --------------------------------------------------------
# Classe para nomeação de arquivos no S3
# --------------------------------------------------------
class S3PathBuilder:
    def __init__(self, bucket_name: str, project_name: str):
        self.bucket_name = bucket_name
        self.project_name = project_name

    def theme_base(self, theme_id: str) -> str:
        return f"{self.project_name}/{theme_id}"

    def theme_json(self, theme_id: str) -> str:
        return f"{self.theme_base(theme_id)}/theme.json"

    def phrase_txt(self, theme_id: str) -> str:
        return f"{self.theme_base(theme_id)}/phrase.txt"

    def audio_file(self, theme_id: str) -> str:
        return f"{self.theme_base(theme_id)}/audio.mp3"

    def video_file(self, theme_id: str) -> str:
        return f"{self.theme_base(theme_id)}/video.mp4"

    def merged_file(self, theme_id: str) -> str:
        return f"{self.theme_base(theme_id)}/final.mp4"

    def metadata_file(self, theme_id: str) -> str:
        return f"{self.theme_base(theme_id)}/metadata.json"

# --------------------------------------------------------
# Configuração global
# --------------------------------------------------------
# Nosso Dashboard de temas
apipath = "https://vmaker.actioncode.com.br/themes/sample"

# Openai
openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# AWS
BUCKET_NAME = "workshopmlops"
PROJECT_NAME = "vmaker"
s3 = boto3.client("s3")
paths = S3PathBuilder(BUCKET_NAME, PROJECT_NAME)

# GCP
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
genai_client = genai.Client(api_key=GOOGLE_API_KEY)


# --------------------------------------------------------
# Assets
# TODO: Falta criar Metadata para cada asset, para visualizar no painel do Dagster UI!
# --------------------------------------------------------
@asset
def theme() -> dict:
    # Busca tema na API externa
    resp = requests.get(apipath)
    data = resp.json()
    theme_id = str(data["id"])

    s3.put_object(
        Bucket=paths.bucket_name,
        Key=paths.theme_json(theme_id),
        Body=json.dumps(data, ensure_ascii=False).encode("utf-8")
    )
    return data


@asset
def phrase(theme: dict) -> str:
    # Gera frase curta sobre o tema usando OpenAI
    theme_id = str(theme["id"])
    tema = theme["name"]
    
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": f"crie uma frase curta sobre o tema: {tema}"}],
        max_tokens=100,
        temperature=0.7
    )
    
    frase = response.choices[0].message.content.strip()

    s3.put_object(
        Bucket=paths.bucket_name,
        Key=paths.phrase_txt(theme_id),
        Body=frase.encode("utf-8")
    )
    return frase


@asset
def audio(theme: dict, phrase: str) -> str:
    # Gera áudio a partir da frase usando OpenAI TTS e salva no S3
    theme_id = str(theme["id"])
    
    response = openai_client.audio.speech.create(
        model="tts-1", 
        voice="onyx",
        input=phrase
    )
    
    # Dependendo da versão do SDK, pode ser response.read() ou response.stream
    audio_content = response.read() if hasattr(response, "read") else response.content
    
    s3.put_object(
        Bucket=paths.bucket_name,
        Key=paths.audio_file(theme_id),
        Body=audio_content,
        ContentType="audio/mpeg"
    )
    return paths.audio_file(theme_id)


@asset
def video(theme: dict, phrase: str) -> str:
    timeout = 300 # tempo máximo de espera em segundos (default: 300s).
    interval = 10 # intervalo entre polls em segundos (default: 10s).
    theme_id = str(theme["id"])

    # Inicia geração de vídeo
    operation = genai_client.models.generate_videos(
        model="veo-3.0-generate-preview",
        prompt=phrase,
        config=types.GenerateVideosConfig(),
    )

    # Polling até o job finalizar ou dar timeout
    elapsed = 0
    while not operation.done:
        if elapsed >= timeout:
            raise TimeoutError(f"Geração de vídeo excedeu {timeout}s")
        time.sleep(interval)
        elapsed += interval
        operation = genai_client.operations.get(operation)

    # Extrai o primeiro vídeo gerado
    try:
        generated_video = operation.result.generated_videos[0]
        video_file = genai_client.files.download(file=generated_video.video)
    except (KeyError, IndexError, AttributeError):
        raise RuntimeError(f"Resposta inesperada da API de vídeo: {operation}")

    # Salva no S3
    s3.put_object(
        Bucket=paths.bucket_name,
        Key=paths.video_file(theme_id),
        Body=video_file,
        ContentType="video/mp4"
    )

    return paths.video_file(theme_id)


@asset
def merged_video(theme: dict, audio: str, video: str) -> str:
    # Baixa áudio e vídeo do S3, faz merge com ffmpeg e envia resultado para o S3
    theme_id = str(theme["id"])

    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = os.path.join(tmpdir, "audio.mp3")
        video_path = os.path.join(tmpdir, "video.mp4")
        output_path = os.path.join(tmpdir, "merged.mp4")

        # Baixar do S3
        s3.download_file(paths.bucket_name, audio, audio_path)
        s3.download_file(paths.bucket_name, video, video_path)

        # ffmpeg com -map para garantir streams corretos
        cmd = [
            "ffmpeg",
            "-y",
            "-i", video_path,
            "-i", audio_path,
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-c:v", "copy",
            "-c:a", "aac",
            "-shortest",
            output_path
        ]
        subprocess.run(cmd, check=True)

        with open(output_path, "rb") as f:
            merged_content = f.read()

    s3.put_object(
        Bucket=paths.bucket_name,
        Key=paths.merged_file(theme_id),
        Body=merged_content,
        ContentType="video/mp4"
    )

    return paths.merged_file(theme_id)


@asset
def metadata(theme: dict, merged_video: str, phrase: str) -> dict:
    # Gera título e descrição usando OpenAI e salva no S3
    theme_id = str(theme["id"])
    
    prompt = f"""Com base na frase '{phrase}' sobre o tema '{theme["name"]}', 
    crie um título curto e atraente e uma descrição envolvente para um vídeo.
    
    Retorne a resposta em formato JSON seguindo exatamente esta estrutura:
    {{
        "titulo": "título curto e atraente aqui",
        "descricao": "descrição envolvente aqui"
    }}"""
    
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that always responds in valid JSON format."},
            {"role": "user", "content": prompt}
        ],
        response_format={"type": "json_object"},
        max_tokens=200,
        temperature=0.7
    )
    
    metadata_dict = json.loads(response.choices[0].message.content)

    s3.put_object(
        Bucket=paths.bucket_name,
        Key=paths.metadata_file(theme_id),
        Body=json.dumps(metadata_dict, ensure_ascii=False).encode("utf-8")
    )
    return metadata_dict


# --------------------------------------------------------
# Jobs
# --------------------------------------------------------
# Cria um job que materializa todos os assets
materialize_assets_job = define_asset_job(
    name="materialize_assets_job",
    selection=[theme, phrase, audio, video, merged_video, metadata]
)


# --------------------------------------------------------
# Schedules
# --------------------------------------------------------
daily_schedule = ScheduleDefinition(
    name="daily_vmaker_assets",
    cron_schedule="0 10 * * *",  # todos os dias às 10h
    job=materialize_assets_job,
    execution_timezone="America/Sao_Paulo"
)


# --------------------------------------------------------
# Definitions para o Dagster carregar
# --------------------------------------------------------
defs = Definitions(
    assets=[theme, phrase, audio, video, merged_video, metadata],
    jobs=[materialize_assets_job],
    schedules=[daily_schedule]
)