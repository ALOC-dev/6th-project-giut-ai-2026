"""GPT 사용량, 백엔드 요청, API 요청 기록을 S3에 저장

S3 객체는 이어 쓰기가 불가능하므로 다음과 같이 저장한다.
- gpt_usage: 메모리에 모아 두었다가 flush_logs() 호출 시 JSONL 객체 하나로 업로드
  (Lambda 핸들러 끝에서 finally로 flush_logs()를 호출해야 함)
- backend_requests: 실행당 호출 횟수가 적고 유실되면 안 되므로 호출할 때마다 바로 업로드

키 형식: {LOG_S3_PREFIX}/{종류}/{YYYY-MM-DD}/{종류}_{YYYYMMDDTHHMMSS}_{uuid}.{jsonl|json}
(날짜는 서울 기준. 오래된 로그 삭제는 버킷 수명 주기 규칙으로 관리)
"""

from datetime import datetime
import json
import logging
from threading import Lock
from typing import Any, Literal
from uuid import uuid4

from openai.types import CompletionUsage
from pydantic import BaseModel
import requests

from src.config import settings
from src.storage.s3 import s3_client

logger = logging.getLogger(__name__)

GPT_USAGE_LOG = "gpt_usage"
BACKEND_REQUEST_LOG = "backend_requests"

_buffer_lock = Lock()
_buffers: dict[str, list[str]] = {GPT_USAGE_LOG: []}


def _log_key(kind: str, at: datetime, extension: str) -> str:
    at = at.astimezone(settings.TIMEZONE)
    return (
        f"{settings.LOG_S3_PREFIX}/{kind}/{at.date().isoformat()}/"
        f"{kind}_{at:%Y%m%dT%H%M%S}_{uuid4().hex[:8]}.{extension}"
    )


def _put_log(key: str, body: str, content_type: str) -> None:
    s3_client.put_object(
        Bucket=settings.LOG_S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType=f"{content_type}; charset=utf-8",
    )


def _buffer_line(kind: str, line: str) -> None:
    with _buffer_lock:
        _buffers[kind].append(line)


def flush_logs() -> None:
    """메모리에 모아 둔 로그를 종류별 JSONL 객체로 업로드

    업로드에 실패한 종류는 버퍼에 다시 넣어 다음 flush에서 재시도
    (Lambda warm start 시 같은 실행 환경에서 재사용됨)
    """
    with _buffer_lock:
        pending = {kind: lines for kind, lines in _buffers.items() if lines}
        for kind in pending:
            _buffers[kind] = []

    now = datetime.now(settings.TIMEZONE)
    for kind, lines in pending.items():
        try:
            _put_log(
                _log_key(kind, now, "jsonl"),
                "\n".join(lines) + "\n",
                "application/x-ndjson",
            )
        except Exception:
            logger.exception("%s 로그 업로드 실패 (%d건)", kind, len(lines))
            with _buffer_lock:
                _buffers[kind][:0] = lines


def write_gpt_usage_log(
    usage: CompletionUsage | None,
    *,
    source_url: str,
    request_type: Literal["text", "image"],
    # run_id: str,
    model: str = settings.OPENAI_MODEL,
    started_at: datetime,
    ended_at: datetime,
    # response_id: str | None = None,
    status: Literal["success", "error"] = "success",
    error_type: str | None = None,
) -> None:
    """시작·종료 시각으로 elapsed_ms를 계산하고 GPT 호출 한 건을 버퍼에 기록

    잘못된 인자는 ValueError로 알리지만, 기록 실패는 GPT 호출 결과에 영향을 주지 않도록
    로그만 남기고 삼킴. 실제 업로드는 flush_logs()에서 수행
    """
    if started_at.utcoffset() is None or ended_at.utcoffset() is None:
        raise ValueError("started_at, ended_at에 시간대가 필요합니다.")
    if request_type not in {"text", "image"}:
        raise ValueError("request_type은 text 또는 image여야 합니다.")

    started_at = started_at.astimezone(settings.TIMEZONE)
    ended_at = ended_at.astimezone(settings.TIMEZONE)
    elapsed_ms = (ended_at - started_at).total_seconds() * 1000
    if elapsed_ms < 0:
        # datetime.now()는 벽시계라 NTP 보정 등으로 뒤로 갈 수 있으므로 0으로 기록
        logger.warning(
            "GPT 호출 종료 시각이 시작 시각보다 빠름: %s < %s", ended_at, started_at
        )
        elapsed_ms = 0.0

    prompt_details = usage.prompt_tokens_details if usage else None
    completion_details = usage.completion_tokens_details if usage else None
    record = {
        # "run_id": run_id,
        "source_url": source_url,
        "type": request_type,
        "model": model,
        # "response_id": response_id,
        "status": status,
        "error_type": error_type,
        "started_at": started_at.isoformat(timespec="milliseconds"),
        "ended_at": ended_at.isoformat(timespec="milliseconds"),
        "elapsed_ms": round(elapsed_ms, 3),
        "input_tokens": usage.prompt_tokens if usage else None,
        "cached_input_tokens": prompt_details.cached_tokens if prompt_details else None,
        "output_tokens": usage.completion_tokens if usage else None,
        "reasoning_tokens": (
            completion_details.reasoning_tokens if completion_details else None
        ),
    }
    try:
        _buffer_line(
            GPT_USAGE_LOG, json.dumps(record, ensure_ascii=False, allow_nan=False)
        )
    except Exception:
        # 기록 실패가 GPT 추출 결과에 영향을 주지 않도록 로그만 남김
        logger.exception("GPT 사용량 기록 실패")


def _response_body_for_log(response: requests.Response | None) -> Any:
    """201 응답은 JSON 객체로, 그 외에는 원문 문자열로 반환"""
    if response is None:
        return None
    if response.status_code == 201:
        try:
            return response.json()
        except ValueError:
            pass
    return response.text


def write_backend_request_log(
    payload: list[dict],
    *,
    url: str,
    sent_at: datetime,
    response: requests.Response | None,
    error: Exception | None,
) -> None:
    """백엔드로 보낸 요청 본문과 응답 결과를 요청 한 건당 JSON 객체 하나로 바로 업로드"""
    try:
        sent_at = sent_at.astimezone(settings.TIMEZONE)
        record = {
            "sent_at": sent_at.isoformat(timespec="seconds"),
            "url": url,
            "count": len(payload),
            "status_code": response.status_code if response is not None else None,
            "response_body": _response_body_for_log(response),
            "error": repr(error) if error is not None else None,
            "payload": payload,
        }
        _put_log(
            _log_key(BACKEND_REQUEST_LOG, sent_at, "json"),
            json.dumps(record, ensure_ascii=False, indent=2),
            "application/json",
        )
    except Exception:
        # 기록 실패가 전송 결과에 영향을 주지 않도록 로그만 남김
        logger.exception("백엔드 요청 기록 실패")
