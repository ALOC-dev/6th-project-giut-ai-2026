import asyncio
import logging
from datetime import datetime

from src.llm.dto import ActivityKindEnum, CategoryEnum, TargetParticipantsEnum
from src.config import settings
from src.constants import UrlType
from src.llm.field_extractor import extract_fields_from_text, extract_fields_from_images
from src.llm.prompts import FIELDS_DESCRIPTION
from src.parsers.uos_rss_parser import parse_uos_rss
from src.parsers.contest_korea_parser import parse_contest_korea
from src.services.batch_submitter import (
    CONTEST_KOREA_LISTS,
    UOS_RSS_GENERAL,
    UOS_RSS_HAKSA,
    _contest_korea_detail_urls,
    _published_in_range,
    _uos_link_context_blocks,
    contest_korea_str_no,
    filter_official_url,
)

PAGE_EXTRACTION_CONCURRENCY = 5

logger = logging.getLogger(__name__)


class InvalidContestError(Exception):
    """필드 추출 결과가 조건에 맞지 않을 시 발생하는 예외"""


def check_valid_activity_after_text(extracted_fields: dict):
    """참여할 수 없는 활동이거나 팀 참가가 불가능한 경우 필터링 함수"""
    application_end_at = extracted_fields.get("application_end_at", None)
    target_participants = extracted_fields.get("target_participants", None)
    activity_kind = extracted_fields.get("activity_kind", None)
    is_team = extracted_fields.get("is_team", None)

    error_message = ""

    if (
        application_end_at is not None
        and application_end_at < datetime.now(settings.TIMEZONE).date()
    ):
        error_message = "마감일이 지난 활동입니다."
    elif target_participants == TargetParticipantsEnum.NONE:
        error_message = "대학(원)생이 참가할 수 없는 활동입니다."
    elif activity_kind == ActivityKindEnum.INVALID_ACTIVITY:
        error_message = "공모전 및 대외활동에 해당하지 않습니다."
    elif is_team == False:
        error_message = "팀으로 참가할 수 없는 활동입니다."

    if error_message:
        logger.info(f"[필터링됨] {extracted_fields}")
        raise InvalidContestError(error_message)


def check_valid_activity_after_image(extracted_fields: dict):
    check_valid_activity_after_text(extracted_fields)

    application_end_at = extracted_fields.get("application_end_at", None)
    is_team = extracted_fields.get("is_team", None)

    error_message = ""

    if not application_end_at:
        error_message = "신청 마감일 정보를 찾을 수 없습니다."
    elif not is_team:
        error_message = "팀으로 참가할 수 없는 활동입니다."

    if error_message:
        logger.info(f"[필터링됨] {extracted_fields}")
        raise InvalidContestError(error_message)


async def extract_uos_page(page: dict) -> dict:
    page_text = page["title"] + "\n" + page["body"]
    # 제목을 RSS피드에서 이미 제공하므로 추출할 키에서 제외
    keys = [*FIELDS_DESCRIPTION][:]
    keys.remove("title")

    extracted_fields = await extract_fields_from_text(
        page_text, page["source_url"], keys
    )
    extracted_fields["title"] = page["title"]
    extracted_fields["images"] = page["images"]
    extracted_fields["urls"] = [
        {"type": UrlType.SOURCE_ORIGINAL, "url": page["source_url"]},
        {
            "type": UrlType.OFFICIAL,
            "url": filter_official_url(
                _uos_link_context_blocks(page_text), page["source_url"]
            ),
        },
    ]

    check_valid_activity_after_text(extracted_fields)

    missing_keys = [key for key, value in extracted_fields.items() if value is None]
    if missing_keys:
        image_extracted_fields = await extract_fields_from_images(
            page["images"], page["source_url"], missing_keys
        )
        for key, value in image_extracted_fields.items():
            if value is not None:
                extracted_fields[key] = value

    check_valid_activity_after_image(extracted_fields)

    if not extracted_fields["category"]:
        # 카테고리 분류에 실패했을 경우 '기타'로 분류
        extracted_fields["category"] = [CategoryEnum.ETC]

    return extracted_fields


async def collect_uos(start: datetime, end: datetime) -> list[dict]:
    """[start, end) 사이에 게시된 공지만 수집"""
    general, haksa = await asyncio.gather(
        asyncio.to_thread(parse_uos_rss, UOS_RSS_GENERAL),
        asyncio.to_thread(parse_uos_rss, UOS_RSS_HAKSA),
    )
    semaphore = asyncio.Semaphore(PAGE_EXTRACTION_CONCURRENCY)

    async def extract_limited(page: dict) -> dict | None:
        async with semaphore:
            try:
                return await extract_uos_page(page)
            except InvalidContestError as exc:
                # 등록 기준에 맞지 않는 활동은 정상 필터링이므로 traceback 없이 기록
                logger.info(
                    "시립대 공지 필터링됨: %s (%s)", page.get("source_url"), str(exc)
                )
                return None
            except Exception:
                # 한 페이지 실패가 전체 수집을 중단시키지 않도록 로그만 남기고 제외
                logger.exception("시립대 공지 추출 실패: %s", page.get("source_url"))
                return None

    results = await asyncio.gather(
        *(
            extract_limited(page)
            for page in general + haksa
            if _published_in_range(page, start, end)
        )
    )
    return [result for result in results if result is not None]


async def extract_contest_korea_page(page: dict) -> dict:
    extracted_fields = {key: page.get(key, None) for key in FIELDS_DESCRIPTION.keys()}
    extracted_fields["urls"] = page["urls"]

    missing_keys = [
        key for key in FIELDS_DESCRIPTION.keys() if page.get(key, None) is None
    ]
    extracted_missing_fields = await extract_fields_from_text(
        page["full_text"], page["source_url"], missing_keys
    )
    for key, value in extracted_missing_fields.items():
        extracted_fields[key] = value

    check_valid_activity_after_text(extracted_fields)

    missing_keys = [key for key, value in extracted_fields.items() if value is None]
    if missing_keys:
        image_extracted_fields = await extract_fields_from_images(
            [page["main_image_link"], *page["detail_image_links"]],
            page["source_url"],
            missing_keys,
        )
        for key, value in image_extracted_fields.items():
            if value is not None:
                extracted_fields[key] = value

    check_valid_activity_after_image(extracted_fields)

    if not extracted_fields["category"]:
        # 카테고리 분류에 실패했을 경우 '기타'로 분류
        extracted_fields["category"] = [CategoryEnum.ETC]

    return extracted_fields


async def collect_contest_korea(
    last_str_nos: dict[str, int],
) -> tuple[list[dict], dict[str, int]]:
    """목록별로 마지막으로 수집한 str_no보다 큰 게시물만 수집

    반환값: (추출 결과, 목록별 갱신된 마지막 str_no)
    """
    names = list(CONTEST_KOREA_LISTS)
    url_lists = await asyncio.gather(
        *(
            _contest_korea_detail_urls(CONTEST_KOREA_LISTS[name], last_str_nos[name])
            for name in names
        )
    )
    new_last_str_nos = {
        name: max([last_str_nos[name], *(contest_korea_str_no(url) for url in urls)])
        for name, urls in zip(names, url_lists)
    }
    detail_urls = list(dict.fromkeys(url for urls in url_lists for url in urls))

    results = []
    for url in detail_urls:
        try:
            page = await asyncio.to_thread(parse_contest_korea, url)
        except Exception:
            logger.exception("콘테스트코리아 상세 페이지 파싱 실패: %s", url)
            continue
        page["source_url"] = url
        results.append(page)

    semaphore = asyncio.Semaphore(PAGE_EXTRACTION_CONCURRENCY)

    async def extract_limited(page: dict) -> dict:
        async with semaphore:
            try:
                return await extract_contest_korea_page(page)
            except InvalidContestError as exc:
                # 등록 기준에 맞지 않는 활동은 정상 필터링이므로 traceback 없이 기록
                logger.info(
                    "콘테스트코리아 필터링됨: %s (%s)", page.get("source_url"), str(exc)
                )
                return None
            except Exception:
                # 한 페이지 실패가 전체 수집을 중단시키지 않도록 로그만 남기고 제외
                logger.exception("콘테스트코리아 추출 실패: %s", page.get("source_url"))
                return None

    extracted = await asyncio.gather(*(extract_limited(page) for page in results))
    return [result for result in extracted if result is not None], new_last_str_nos
