"""정기 수집 상태(콘테스트코리아 목록별 마지막 str_no)를 S3에 저장/로드"""

from datetime import datetime
import json
import logging

from botocore.exceptions import ClientError

from src.config import settings
from src.storage.s3 import s3_client

logger = logging.getLogger(__name__)


def load_contest_korea_state(
    list_names: list[str], window_start: datetime
) -> dict[str, int]:
    """목록별 마지막으로 수집한 콘테스트코리아 str_no 로드

    저장된 값이 없으면 수집 구간 시작일 0번으로 간주(해당 날짜 게시물부터 수집)
    """
    default = int(window_start.strftime("%Y%m%d") + "0000")
    try:
        response = s3_client.get_object(
            Bucket=settings.BATCH_CONTEXT_S3_BUCKET, Key=settings.COLLECTOR_STATE_S3_KEY
        )
        state = json.loads(response["Body"].read().decode("utf-8"))
        saved = state.get("contest_korea_last_str_no", {})
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "NoSuchKey":
            logger.exception(
                "수집 상태를 읽을 수 없음: %s", settings.COLLECTOR_STATE_S3_KEY
            )
        saved = {}
    except (ValueError, AttributeError):
        logger.exception("수집 상태 형식 오류: %s", settings.COLLECTOR_STATE_S3_KEY)
        saved = {}

    return {
        name: int(saved[name]) if str(saved.get(name, "")).isdigit() else default
        for name in list_names
    }


def save_contest_korea_state(last_str_nos: dict[str, int]) -> None:
    """배치 결과 처리 및 백엔드 전송이 성공한 뒤에만 호출할 것 (실패 시 다음 수집에서 재시도)"""
    s3_client.put_object(
        Bucket=settings.BATCH_CONTEXT_S3_BUCKET,
        Key=settings.COLLECTOR_STATE_S3_KEY,
        Body=json.dumps({"contest_korea_last_str_no": last_str_nos}, indent=2).encode(
            "utf-8"
        ),
        ContentType="application/json; charset=utf-8",
    )
