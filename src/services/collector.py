import asyncio
import logging
from datetime import datetime
from email.utils import parsedate_to_datetime
import re
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from src.llm.dto import ActivityKindEnum, CategoryEnum, TargetParticipantsEnum
from src.config import settings
from src.constants import UrlType
from src.llm.field_extractor import extract_fields_from_text, extract_fields_from_images
from src.llm.prompts import FIELDS_DESCRIPTION
from src.parsers.uos_rss_parser import parse_uos_rss
from src.parsers.contest_korea_parser import parse_contest_korea

from constants import (
    CAMPUSPICK_HOSTNAME,
    UOS_HOSTNAME,
    UrlType,
    SNS_DOMAINS,
    APPLY_PATH_KEYWORDS,
    IRRELEVANT_KEYWORDS,
    OFFICIAL_PAGE_KEYWORDS,
    DACON_HOSTNAME,
    DAKER_HOSTNAME,
)

UOS_RSS_GENERAL = "https://www.uos.ac.kr/rss/gBoard.do"
UOS_RSS_HAKSA = "https://www.uos.ac.kr/rss/hBoard.do"

CONTEST_KOREA_CONTEST_LIST = (
    "https://www.contestkorea.com/sub/list.php?int_gbn=1"  # 공모전/대회
)
CONTEST_KOREA_ACTIVITY_LIST = (
    "https://www.contestkorea.com/sub/list.php?int_gbn=2"  # 대외활동
)
CONTEST_KOREA_SCAN_LIMIT = 30  # 각 목록 앞 30개에서 새 게시물만 선택
CONTEST_KOREA_FILTERS = {  # 공모전 검색 조건
    "Txt_sortkey": "a.int_sort",
    "Txt_sortword": "desc",
    "Txt_code1[0]": "26",  # 누구나
    "Txt_code1[1]": "30",  # 대학생
    "Txt_code1[2]": "76",  # 대학원생
    "Txt_area[0]": "75",  # 전국
    "Txt_area[1]": "97",  # 온라인
    "Txt_area[2]": "31",  # 서울
}

CONTEST_KOREA_LISTS = {
    "contest": CONTEST_KOREA_CONTEST_LIST,
    "activity": CONTEST_KOREA_ACTIVITY_LIST,
}

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


def _normalize_hostname(url: str) -> str:
    """도메인 비교를 위해 호스트를 소문자로 바꾸고 선행 www. 제거"""
    return (urlparse(url).hostname or "").lower().removeprefix("www.").strip()


def filter_official_url(blocks: list[dict], source_url: str) -> str | None:
    """링크 목록 중 주최사 공식 홈페이지를 찾아 반환 (연관 공모전, 신청 폼, 주최사 SNS 등 제외)"""
    candidate_urls = []
    source_domain = _normalize_hostname(source_url)

    for block in blocks:
        url = block["url"]
        domain = _normalize_hostname(url)
        if re.search(domain, source_domain) or re.search(source_domain, domain):
            # 같은 도메인(연관공모전, 공모전 사이트 자체 게시판 페이지 등) 제외
            if domain == UOS_HOSTNAME or source_domain == UOS_HOSTNAME:
                if domain == source_domain:
                    continue  # UOS 공지는 도메인 완전 일치할 경우에만 제외
            elif domain == CAMPUSPICK_HOSTNAME:
                if url.find("linkclick") == -1:
                    continue  # 캠퍼스픽 redirect 경로는 포함
            else:
                continue
        if (source_domain == DACON_HOSTNAME and domain == DAKER_HOSTNAME) or (
            source_domain == DAKER_HOSTNAME and domain == DACON_HOSTNAME
        ):
            continue  # 데이콘-데이커 (연관 플랫폼) 필터링

        if any(re.search(sns, _normalize_hostname(url)) for sns in SNS_DOMAINS):
            continue  # SNS 주소 제외
        if any(re.search(keyword, url) for keyword in APPLY_PATH_KEYWORDS):
            continue  # 신청폼 주소 제외

        has_official_keyword = any(
            re.search(keyword, block["text"]) for keyword in OFFICIAL_PAGE_KEYWORDS
        )
        has_official_inline_keyword = any(
            re.search(keyword, block["line"]) for keyword in OFFICIAL_PAGE_KEYWORDS
        )
        has_official_context_keyword = any(
            re.search(keyword, block["context"]) for keyword in OFFICIAL_PAGE_KEYWORDS
        )

        has_irrelevant_keyword = any(
            re.search(keyword, block["text"]) for keyword in IRRELEVANT_KEYWORDS
        )
        has_irrelevant_inline_keyword = any(
            re.search(keyword, block["line"]) for keyword in IRRELEVANT_KEYWORDS
        )
        has_irrelevant_context_keyword = any(
            re.search(keyword, block["context"]) for keyword in IRRELEVANT_KEYWORDS
        )

        if (
            has_irrelevant_keyword
            or (not has_official_keyword and has_irrelevant_inline_keyword)
            or (
                not has_official_keyword
                and not has_irrelevant_inline_keyword
                and has_irrelevant_context_keyword
            )
        ):
            # irrelevant 키워드가 있으면 추출하지 않음(a 태그 자체 텍스트 -> 해당 라인의 텍스트 -> 이전 문맥 순으로 비교)
            continue

        candidate_level = (
            1
            if has_official_keyword
            else (
                2
                if has_official_inline_keyword
                else (3 if has_official_context_keyword else 4)
            )
        )

        candidate_urls.append({"url": url, "candidate_level": candidate_level})

    for level in range(1, 5):
        result = list(
            {url["url"] for url in candidate_urls if url["candidate_level"] == level}
        )
        if result:
            break

    if len(result) == 0:
        return None
    elif len(result) > 1:
        result = sorted(result, key=len)
    return result[0]


def _published_in_range(page: dict, start: datetime, end: datetime) -> bool:
    """게시 시각이 [start, end) 범위에 속하는지 확인"""
    published_date = page.get("published_date", "")
    try:
        try:
            published_at = parsedate_to_datetime(published_date)
        except ValueError:
            published_at = datetime.fromisoformat(published_date)
    except (TypeError, ValueError):
        logger.warning("시립대 RSS 게시일을 읽을 수 없음: %r", published_date)
        return False

    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=settings.TIMEZONE)
    return start <= published_at < end


def _uos_link_context_blocks(full_text: str) -> list[dict]:
    blocks = []
    for line in full_text.split("\n"):
        urls_iter = {
            *re.finditer(
                r"(https?:\/\/)(?:[\w-]+\.)+[\w-]+(?::\d+)?(?:[\/?#][^\s]*)?", line
            ),
        }
        next_idx = 0
        for i in sorted(urls_iter, key=lambda i: i.start()):
            start, end = i.span()
            blocks.append(
                {
                    "url": line[start:end].strip(),
                    "text": line[next_idx:start].strip(),
                    "line": line,
                    "context": "",
                }
            )
            next_idx = end
    return blocks


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


def contest_korea_str_no(url: str) -> int | None:
    """상세 URL의 str_no(YYYYMMDD + 일련번호)를 정수로 반환"""
    str_no = parse_qs(urlparse(url).query).get("str_no", [""])[0]
    return int(str_no) if str_no.isdigit() else None


async def _contest_korea_detail_urls(list_url: str, last_str_no: int) -> list[str]:
    """목록에서 str_no가 last_str_no보다 큰 게시물의 상세 URL 반환"""
    page_url = list_url.split("?", maxsplit=1)[0]
    int_gbn = parse_qs(urlparse(list_url).query)["int_gbn"][0]
    first_page = True
    detail_urls = []
    seen_urls = set()
    scanned_entries = 0

    while page_url and scanned_entries < CONTEST_KOREA_SCAN_LIMIT:
        response = await asyncio.to_thread(
            requests.get,
            page_url,
            params=(
                {"int_gbn": int_gbn, **CONTEST_KOREA_FILTERS} if first_page else None
            ),
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        response.raise_for_status()
        first_page = False
        soup = BeautifulSoup(response.content, "html.parser")
        entries = soup.select(".list_style_2 .title a[href*='str_no=']")
        if not entries:
            break

        # 사이트의 목록 순서는 게시일 순서가 아니므로 오래된 글이 나와도 CONTEST_KOREA_SCAN_LIMIT 범위 내에서 계속 확인
        candidates = entries[: CONTEST_KOREA_SCAN_LIMIT - scanned_entries]
        scanned_entries += len(candidates)
        for entry in candidates:
            url = urljoin(response.url, entry["href"])
            str_no = contest_korea_str_no(url)
            if str_no is None:
                logger.warning("콘테스트코리아 게시물 번호를 읽을 수 없음: %s", url)
                continue

            if str_no > last_str_no and url not in seen_urls:
                seen_urls.add(url)
                detail_urls.append(url)

        if scanned_entries >= CONTEST_KOREA_SCAN_LIMIT:
            break

        current_page = int(parse_qs(urlparse(response.url).query).get("page", ["1"])[0])
        next_page = None
        for link in soup.select(".pagination a[href]"):
            candidate = urljoin(response.url, link["href"])
            if parse_qs(urlparse(candidate).query).get("page") == [
                str(current_page + 1)
            ]:
                next_page = candidate
                break
        page_url = next_page

    return detail_urls


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
