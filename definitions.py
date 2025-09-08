import requests
from dagster import asset, Definitions, define_asset_job, MetadataValue, OpExecutionContext


# --------------------------------------------------------
# API
# --------------------------------------------------------
apipath = "https://vmaker.actioncode.com.br/themes/sample"


# --------------------------------------------------------
# Assets
# --------------------------------------------------------
@asset(description="Busca um novo tema")
def theme(context: OpExecutionContext) -> dict:
    resp = requests.get(apipath)
    resp.raise_for_status()  # levanta se a API retornar erro
    data = resp.json()
    context.add_output_metadata({"theme_id": MetadataValue.text(str(data.get("id")))})
    return data


@asset(description="Gera um roteiro")
def phrase(context: OpExecutionContext, theme: dict) -> str:
    resultado1 = f"Esta é uma bela frase sobre o assunto escolhido: {theme['name']}"
    context.add_output_metadata(
        {
            "preview": MetadataValue.text(resultado1),
            "length": MetadataValue.int(len(resultado1)),
        }
    )
    return resultado1


@asset(description="Gera o áudio")
def audio(context: OpExecutionContext, phrase: str) -> str:
    resultado2 = f"Este é o áudio do roteiro gerado: {phrase}"
    context.add_output_metadata(
        {
            "preview": MetadataValue.text(resultado2),
        }
    )
    return resultado2


@asset(description="Gera o vídeo")
def video(context: OpExecutionContext, theme: dict, phrase: str) -> str:
    resultado3 = f"Este é o vídeo do roteiro gerado: {phrase}"
    context.add_output_metadata(
        {
            "preview": MetadataValue.text(resultado3),
        }
    )
    return resultado3


@asset(description="Une áudio e vídeo")
def video_metadata(context: OpExecutionContext, audio: str, video: str) -> str:
    resultado4 = "Estes são o título e a descrição do vídeo"
    context.add_output_metadata(
        {
            "preview": MetadataValue.text(resultado4),
        }
    )
    return resultado4


# --------------------------------------------------------
# O job
# --------------------------------------------------------
workshop_job = define_asset_job(
    name="workshop_job",
    selection=["theme", "phrase", "audio", "video", "video_metadata"],
)

# --------------------------------------------------------
# Definitions
# --------------------------------------------------------
defs = Definitions(
    assets=[theme, phrase, audio, video, video_metadata],
    jobs=[workshop_job],
)