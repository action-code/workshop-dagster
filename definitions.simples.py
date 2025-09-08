# definitions.py Completo!
from dagster import asset, Definitions, define_asset_job, ScheduleDefinition


# --------------------------------------------------------
# Assets
# --------------------------------------------------------
@asset
def theme() -> str:
    return "Meu tema"

@asset
def phrase(theme: str) -> str:
    return "Minha frase curta sobre o tema"

@asset
def audio(theme: str, phrase: str) -> str:
    return "Meu áudio gerado"

@asset
def video(theme: str, phrase: str) -> str:
    return "Meu vídeo gerado"

@asset
def merged_video(theme: str, audio: str, video: str) -> str:
    return "Meu vídeo mesclado gerado"

@asset
def metadata(theme: str, merged_video: str, phrase: str) -> dict:
    return {"theme": theme, "merged_video": merged_video, "phrase": phrase}


# --------------------------------------------------------
# O job
# --------------------------------------------------------
materialize_assets_job = define_asset_job(
    name="materialize_assets_job",
    selection=[theme, phrase, audio, video, merged_video, metadata]
)


# --------------------------------------------------------
# Agendamento diário
# --------------------------------------------------------
daily_schedule = ScheduleDefinition(
    name="daily_vmaker_assets",
    cron_schedule="0 10 * * *",  # todos os dias às 10h
    job=materialize_assets_job,
    execution_timezone="America/Sao_Paulo"
)


# --------------------------------------------------------
# Definições
# --------------------------------------------------------
defs = Definitions(
    assets=[theme, phrase, audio, video, merged_video, metadata],
    jobs=[materialize_assets_job],
    schedules=[daily_schedule]
)