from __future__ import annotations
import datetime as dt
import logging
from typing import List, Dict, Iterator
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from etl.config import settings
from etl.clients.minio_client import upload_bytes
from etl.utils.jsonl import dumps_bytes

logger = logging.getLogger(__name__)

def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=max(0, settings.request_retries),
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def _request_page_from(base_url: str, limit: int, offset: int, session: requests.Session, tolerate_400: bool = False) -> List[Dict]:
    params = {"limit": limit, "offset": offset}
    resp = session.get(base_url, params=params, timeout=settings.request_timeout)
    # Some public APIs (e.g. MangaDex) return 400 when offset exceeds total. Treat that as "no more data".
    if tolerate_400 and resp.status_code == 400:
        return []
    # Safety: also catch raise_for_status 400 and treat it as end-of-data if tolerate_400 is True
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        if tolerate_400 and getattr(e, "response", None) is not None and e.response.status_code == 400:
            return []
        raise
    body = resp.json()
    if isinstance(body, dict) and "data" in body:
        return body["data"]
    if isinstance(body, list):
        return body
    return [body]

def _request_page(limit: int, offset: int) -> List[Dict]:
    """
    Try primary API first; if it fails (DNS/connect/HTTP), fall back to public API
    specified by settings.manga_api_fallback to keep the pipeline runnable.
    """
    session = _make_session()
    last_err: Exception | None = None
    if settings.manga_api_base:
        try:
            return _request_page_from(settings.manga_api_base, limit, offset, session, tolerate_400=False)
        except Exception as e:
            logger.warning("Primary API failed: %r, will try fallback if configured", e)
            last_err = e
    if settings.manga_api_fallback:
        try:
            return _request_page_from(settings.manga_api_fallback, limit, offset, session, tolerate_400=True)
        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 400:
                # Treat 400 as "no more data" for public API pagination edge
                return []
            logger.error("Fallback API failed: %r", e)
            last_err = e
        except Exception as e:
            logger.error("Fallback API failed: %r", e)
            last_err = e
    if last_err:
        raise last_err
    raise RuntimeError("No API endpoint configured. Set MANGA_API_BASE or MANGA_API_FALLBACK.")

def fetch_and_store_jsonl(ds: str, page_size: int = 100, batch_size: int = 1000) -> None:
    """
    Extracts manga list from API and stores as JSONL into MinIO:
      raw/manga/load_date=YYYY-MM-DD/manga_YYYYMMDD_HHMMSS.jsonl
    Processes data in batches to avoid loading all into memory.
    """
    load_date = dt.datetime.strptime(ds, "%Y-%m-%d").date()
    prefix = f"raw/manga/load_date={load_date.isoformat()}/"
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}manga_{timestamp}.jsonl"

    offset = 0
    batch: List[Dict] = []
    file_index = 0

    while True:
        items = _request_page(page_size, offset)
        if not items:
            break
        batch.extend(items)
        offset += page_size

        # If batch is full or last page, upload
        if len(batch) >= batch_size or len(items) < page_size:
            if batch:
                payload = dumps_bytes(batch)
                if file_index == 0:
                    upload_key = key
                else:
                    upload_key = key.replace('.jsonl', f'_{file_index}.jsonl')
                upload_bytes(upload_key, payload, "application/jsonl")
                batch = []
                file_index += 1

        if len(items) < page_size:
            break
