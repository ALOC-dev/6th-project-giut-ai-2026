"""공모전 페이지 파싱 후 텍스트 필드 추출 요청을 OpenAI Batch API로 제출

collector.py의 extract_fields_from_text 호출 직전 단계까지를 담당한다.
배치 결과를 받아 검증/이미지 추출/전송하는 단계는 별도 함수에서 처리하며,
그때 필요한 페이지 정보(custom_id 기준)는 S3에 JSON으로 저장하고
그 키를 배치 metadata의 context_s3_key에 기록한다.
"""

import asyncio
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import json
import logging
import re
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from openai.lib._parsing._completions import type_to_response_format_param
import requests

from src.config import settings
from src.constants import (
    APPLY_PATH_KEYWORDS,
    CAMPUSPICK_HOSTNAME,
    DACON_HOSTNAME,
    DAKER_HOSTNAME,
    IRRELEVANT_KEYWORDS,
    OFFICIAL_PAGE_KEYWORDS,
    SNS_DOMAINS,
    UOS_HOSTNAME,
    UrlType,
)
from src.llm.dto import ExtractedFields
from src.llm.openai_client import client
from src.llm.prompts import FIELDS_DESCRIPTION, CONTEST_TEXT_PROMPT, UOS_TEXT_PROMPT
from src.parsers.contest_korea_parser import parse_contest_korea
from src.parsers.uos_rss_parser import parse_uos_rss
from src.storage.batch_context import batch_context_key, save_batch_context
from src.storage.collector_state import load_contest_korea_state

logger = logging.getLogger(__name__)

BATCH_ENDPOINT = "/v1/chat/completions"
BATCH_COMPLETION_WINDOW = "24h"

COLLECT_INTERVAL = timedelta(hours=12)  # EventBridge가 서울 기준 00시, 12시에 호출

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


def _text_request_line(
    custom_id: str, page_text: str, keys: list[str], is_uos: bool
) -> dict:
    """extract_fields_from_text와 동일한 요청 본문을 Batch API 입력 한 줄 형식으로 생성"""
    if is_uos:
        prompt = UOS_TEXT_PROMPT
        prompt_cache_key = settings.UOS_TEXT_CACHE_KEY
    else:
        prompt = CONTEST_TEXT_PROMPT
        prompt_cache_key = settings.TEXT_CACHE_KEY
    prompt += "\n".join(FIELDS_DESCRIPTION[key] for key in keys)

    body = {
        "model": settings.OPENAI_MODEL,
        "messages": [
            {"role": "developer", "content": [{"type": "text", "text": prompt}]},
            {"role": "user", "content": [{"type": "text", "text": page_text}]},
        ],
        # chat.completions.parse()가 pydantic 모델을 변환하는 방식과 동일
        "response_format": type_to_response_format_param(
            ExtractedFields.select_fields(keys)
        ),
        "store": False,
        "reasoning_effort": "low",
    }
    if prompt_cache_key:
        body["prompt_cache_key"] = prompt_cache_key

    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": BATCH_ENDPOINT,
        "body": body,
    }


async def _parse_uos_pages(start: datetime, end: datetime) -> list[dict]:
    """[start, end) 사이에 게시된 시립대 공지 파싱 (일반/학사 공지 중복 제거)"""
    feeds = await asyncio.gather(
        asyncio.to_thread(parse_uos_rss, UOS_RSS_GENERAL),
        asyncio.to_thread(parse_uos_rss, UOS_RSS_HAKSA),
        return_exceptions=True,
    )
    pages = {}
    for feed_url, feed in zip((UOS_RSS_GENERAL, UOS_RSS_HAKSA), feeds):
        if isinstance(feed, Exception):
            logger.error("시립대 RSS 파싱 실패: %s (%r)", feed_url, feed)
            continue
        for page in feed:
            if _published_in_range(page, start, end):
                pages.setdefault(page["source_url"], page)
    return list(pages.values())


async def _parse_contest_korea_pages(
    last_str_nos: dict[str, int],
) -> tuple[list[dict], dict[str, int]]:
    """목록별 마지막 str_no 이후 게시물의 상세 페이지 파싱

    반환값: (파싱 결과, 목록별 갱신된 마지막 str_no)
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

    pages = []
    for url in detail_urls:
        try:
            page = await asyncio.to_thread(parse_contest_korea, url)
        except Exception:
            logger.exception("콘테스트코리아 상세 페이지 파싱 실패: %s", url)
            continue
        page["source_url"] = url
        pages.append(page)
    return pages, new_last_str_nos


def _uos_request(index: int, page: dict) -> tuple[str, dict, dict | None]:
    """시립대 공지 1건의 (custom_id, context, 배치 요청 줄) 생성"""
    custom_id = f"uos-{index}"
    page_text = page["title"] + "\n" + page["body"]
    # 제목은 RSS피드에서 이미 제공하므로 추출할 키에서 제외
    keys = [key for key in FIELDS_DESCRIPTION if key != "title"]

    context = {
        "source": "uos",
        "source_url": page["source_url"],
        "title": page["title"],
        "images": page["images"],
        "urls": [
            {"type": UrlType.SOURCE_ORIGINAL, "url": page["source_url"]},
            {
                "type": UrlType.OFFICIAL,
                "url": filter_official_url(
                    _uos_link_context_blocks(page_text), page["source_url"]
                ),
            },
        ],
        "keys": keys,
    }
    line = _text_request_line(custom_id, page_text, keys, is_uos=True)
    return custom_id, context, line


def _contest_korea_request(index: int, page: dict) -> tuple[str, dict, dict | None]:
    """콘테스트코리아 게시물 1건의 (custom_id, context, 배치 요청 줄) 생성

    파서가 이미 채운 필드는 prefilled로 보관하고, 비어 있는 필드만 LLM에 요청
    본문 텍스트가 없거나 비어 있는 필드가 없으면 배치 요청 줄은 None
    """
    custom_id = f"contest_korea-{contest_korea_str_no(page['source_url']) or index}"
    keys = [key for key in FIELDS_DESCRIPTION if page.get(key) is None]

    context = {
        "source": "contest_korea",
        "source_url": page["source_url"],
        "prefilled": {
            key: page.get(key) for key in FIELDS_DESCRIPTION if key not in keys
        },
        "images": [page["main_image_link"], *page["detail_image_links"]],
        "urls": page["urls"],
        "keys": keys,
    }
    line = None
    if keys and page.get("full_text"):
        line = _text_request_line(
            custom_id, page["full_text"], keys, is_uos=False
        )
    return custom_id, context, line


async def submit_extraction_batch(run_at: datetime) -> dict:
    """시립대 공지 및 콘테스트코리아를 파싱해 텍스트 필드 추출 요청을 배치로 제출

    run_at: 이번 수집 예정 시각 (EventBridge Scheduler의 scheduled-time, 시간대 포함)
    - 시립대: [run_at - COLLECT_INTERVAL, run_at) 사이에 게시된 공지
    - 콘테스트코리아: 목록별로 S3에 저장된 마지막 str_no보다 큰 게시물

    S3 컨텍스트 문서({prefix}/{end(KST) YYYYMMDDTHHMM}.json) 구성:
        window_start, window_end: 수집 구간
        last_str_nos: 목록별 갱신된 마지막 str_no (배치 결과 처리 성공 후 저장할 것)
        batch_id, input_file_id: 제출한 배치 정보 (요청이 없으면 None)
        contexts: custom_id별 결과 처리에 필요한 페이지 정보
            (has_text_request가 False면 배치 결과 없이 이미지 추출 단계로 넘겨야 함)
            Enum은 value, 날짜는 ISO 문자열로 저장됨

    반환값: batch_id, input_file_id, context_s3_key, last_str_nos
    """
    if run_at.utcoffset() is None:
        raise ValueError("run_at에 시간대가 필요합니다.")
    end = run_at
    start = end - COLLECT_INTERVAL
    last_str_nos = await asyncio.to_thread(
        load_contest_korea_state, list(CONTEST_KOREA_LISTS), start
    )

    uos_pages, (contest_korea_pages, new_last_str_nos) = await asyncio.gather(
        _parse_uos_pages(start, end), _parse_contest_korea_pages(last_str_nos)
    )

    contexts = {}
    lines = []
    builders = [(_uos_request, page) for page in uos_pages] + [
        (_contest_korea_request, page) for page in contest_korea_pages
    ]
    for index, (build, page) in enumerate(builders):
        try:
            custom_id, context, line = build(index, page)
        except Exception:
            # 한 페이지 실패가 전체 배치를 중단시키지 않도록 로그만 남기고 제외
            logger.exception("배치 요청 생성 실패: %s", page.get("source_url"))
            continue
        if custom_id in contexts:
            logger.warning("중복 custom_id 제외: %s", custom_id)
            continue
        context["has_text_request"] = line is not None
        contexts[custom_id] = context
        if line is not None:
            lines.append(line)

    context_s3_key = batch_context_key(end)
    document = {
        "window_start": start,
        "window_end": end,
        "last_str_nos": new_last_str_nos,
        "batch_id": None,
        "input_file_id": None,
        "contexts": contexts,
    }

    if not lines:
        logger.info("배치로 보낼 텍스트 추출 요청이 없음 (페이지 %d건)", len(contexts))
    else:
        jsonl = "\n".join(json.dumps(line, ensure_ascii=False) for line in lines)
        input_file = await client.files.create(
            file=("extraction_batch.jsonl", jsonl.encode("utf-8")),
            purpose="batch",
        )
        document["input_file_id"] = input_file.id
        # 배치 완료 전에 결과 처리 단계가 컨텍스트를 읽을 수 있도록 배치 생성 전에 먼저 저장
        await asyncio.to_thread(save_batch_context, context_s3_key, document)

        batch = await client.batches.create(
            input_file_id=input_file.id,
            endpoint=BATCH_ENDPOINT,
            completion_window=BATCH_COMPLETION_WINDOW,
            metadata={
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "context_s3_key": context_s3_key,
            },
        )
        document["batch_id"] = batch.id
        logger.info(
            "배치 제출 완료: %s (요청 %d건, 페이지 %d건)",
            batch.id,
            len(lines),
            len(contexts),
        )

    await asyncio.to_thread(save_batch_context, context_s3_key, document)
    logger.info(
        "배치 컨텍스트 저장 완료: s3://%s/%s",
        settings.BATCH_CONTEXT_S3_BUCKET,
        context_s3_key,
    )

    return {
        "batch_id": document["batch_id"],
        "input_file_id": document["input_file_id"],
        "context_s3_key": context_s3_key,
        "last_str_nos": new_last_str_nos,
    }
