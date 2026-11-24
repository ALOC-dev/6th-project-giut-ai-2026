import os
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")


class Settings:
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-6-luna")

    TEXT_CACHE_KEY = os.getenv("TEXT_CACHE_KEY")
    IMAGE_CACHE_KEY = os.getenv("IMAGE_CACHE_KEY")
    UOS_TEXT_CACHE_KEY = os.getenv("UOS_TEXT_CACHE_KEY")
    UOS_IMAGE_CACHE_KEY = os.getenv("UOS_IMAGE_CACHE_KEY")

    AWS_REGION = os.getenv("AWS_REGION")

    # 배치 결과 처리에 필요한 중간 컨텍스트 저장 버킷
    BATCH_CONTEXT_S3_BUCKET = os.getenv("BATCH_CONTEXT_S3_BUCKET")
    BATCH_CONTEXT_S3_PREFIX = os.getenv("BATCH_CONTEXT_S3_PREFIX", "batch_contexts")
    # BATCH_CONTEXT_S3_BUCKET 안의 정기 수집 상태 객체 키
    COLLECTOR_STATE_S3_KEY = os.getenv("COLLECTOR_STATE_S3_KEY", "collector_state.json")

    # 로그 저장 버킷 (별도 버킷 분리 전까지는 컨텍스트 버킷과 동일)
    LOG_S3_BUCKET = os.getenv("LOG_S3_BUCKET", BATCH_CONTEXT_S3_BUCKET)
    LOG_S3_PREFIX = os.getenv("LOG_S3_PREFIX", "logs")

    X_AI_API_KEY = os.getenv("X_AI_API_KEY")
    COMPETITIONS_API_URL = os.getenv("COMPETITIONS_API_URL")

    TIMEZONE = ZoneInfo("Asia/Seoul")


settings = Settings()
