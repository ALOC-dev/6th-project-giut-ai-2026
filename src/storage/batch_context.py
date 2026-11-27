"""배치 제출 단계와 결과 처리 단계 사이의 중간 컨텍스트를 S3에 저장/로드"""

from datetime import date, datetime
from enum import Enum
import json

from src.config import settings
from src.storage.s3 import s3_client as _s3


def _json_default(value: object) -> object:
    """Enum은 value, 날짜는 ISO 문자열로 직렬화 (LLM 응답과 같은 형태)"""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"JSON으로 직렬화할 수 없는 타입: {type(value).__name__}")


def batch_context_key(window_end: datetime) -> str:
    """수집 구간 끝(KST) 기준 키: 같은 구간 재실행 시 같은 키를 덮어씀"""
    window_end_kst = window_end.astimezone(settings.TIMEZONE).strftime("%Y%m%dT%H%M")
    return f"{settings.BATCH_CONTEXT_S3_PREFIX}/{window_end_kst}.json"


def save_batch_context(key: str, document: dict) -> None:
    _s3.put_object(
        Bucket=settings.BATCH_CONTEXT_S3_BUCKET,
        Key=key,
        Body=json.dumps(document, ensure_ascii=False, default=_json_default).encode(
            "utf-8"
        ),
        ContentType="application/json; charset=utf-8",
    )


def load_batch_context(key: str) -> dict:
    """저장된 컨텍스트 로드 (Enum·날짜는 문자열 상태 그대로 반환)"""
    response = _s3.get_object(Bucket=settings.BATCH_CONTEXT_S3_BUCKET, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))
