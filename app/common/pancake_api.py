"""
Pancake Inbox & Ads API Utilities
Common functions for pulling Conversations/Ads data from Pancake (developer.pancake.biz)

Auth model note (different from CareSoft):
- CareSoft: one (domain, api_key) pair per brand.
- Pancake:  one page_access_token PER connected page (Facebook/Instagram/...),
            same shape as core_facebook_ad_account -> we store one row per page
            in `core_pancake_page` and loop over pages like get_active_facebook_accounts().

Server (Page API v1): https://pages.fm/api/public_api/v1
"""

import time
import requests
from typing import Dict, List, Optional, Tuple, Any
from loguru import logger

PAGE_API_V1_BASE = "https://pages.fm/api/public_api/v1"

CORE_PANCAKE_PAGE_DDL = """
CREATE TABLE IF NOT EXISTS core_pancake_page (
    id SERIAL PRIMARY KEY,
    name TEXT,
    page_id TEXT NOT NULL UNIQUE,
    platform TEXT,                 -- facebook / instagram / whatsapp / zalo ...
    page_access_token TEXT NOT NULL,
    current_brand_id INTEGER,      -- maps to core_brand.erp_id, for cross-source joins
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    priority_order INTEGER NOT NULL DEFAULT 100,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
)
"""


def ensure_pancake_page_table_exists(db) -> None:
    """Idempotent CREATE TABLE for the page-credential config table."""
    db.execute(CORE_PANCAKE_PAGE_DDL)


def get_active_pancake_pages(db) -> List[Dict]:
    """
    Load active Pancake pages (one row = one connected Facebook/Instagram/... page)
    from the config table.

    Returns list of dicts: id, name, page_id, platform, page_access_token, current_brand_id
    """
    ensure_pancake_page_table_exists(db)

    query = """
        SELECT id, name, page_id, platform, page_access_token, current_brand_id
        FROM core_pancake_page
        WHERE is_active = TRUE
        ORDER BY priority_order ASC
    """
    rows = db.fetchall(query)

    pages = []
    for row in rows:
        pages.append({
            'id': row[0],
            'name': row[1],
            'page_id': row[2],
            'platform': row[3],
            'page_access_token': row[4],
            'current_brand_id': row[5],
        })

    logger.info(f"Loaded {len(pages)} active Pancake pages")
    return pages


def call_pancake_page_api(
    page: Dict,
    path: str,
    params: Optional[Dict] = None,
    base_url: str = PAGE_API_V1_BASE,
    timeout: int = 30,
    max_retries: int = 3,
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Generic GET wrapper for the Pancake Page API.

    Args:
        page: dict from get_active_pancake_pages() — needs 'page_id', 'page_access_token'
        path: endpoint under /pages/{page_id}/, e.g. 'conversations', 'statistics/pages_campaigns'
        params: extra query params (page_access_token is injected automatically)

    Returns:
        (data, error) — data is the parsed JSON body, error is a string message or None.
    Retries on HTTP 429 with exponential backoff (Pancake does not publish a rate-limit spec).
    """
    url = f"{base_url}/pages/{page['page_id']}/{path}"
    query = dict(params or {})
    query['page_access_token'] = page['page_access_token']

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=query, timeout=timeout)
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries:
                time.sleep(2 ** attempt)
                continue
            return None, last_error

        if resp.status_code == 200:
            return resp.json(), None

        if resp.status_code == 429:
            wait = 2 ** attempt
            logger.warning(f"Rate limited on page '{page.get('name')}' ({path}); retrying in {wait}s")
            time.sleep(wait)
            last_error = "HTTP 429: rate limited"
            continue

        return None, f"HTTP {resp.status_code}: {resp.text[:500]}"

    return None, last_error or "Max retries exceeded"


def fetch_conversations(
    page: Dict,
    since: int,
    until: int,
    conv_types: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Pull all conversations for a page within [since, until] (unix seconds).

    Pancake paginates by cursor, not by page number: each call returns the 60
    most-recently-updated conversations; pass `last_conversation_id` (the id of
    the last item returned) to walk further back in time. We stop when a page
    comes back short (<60, i.e. end of data) — `since`/`until` bound what the
    API itself returns, so no separate date check is needed client-side.
    """
    all_conversations: List[Dict] = []
    last_conversation_id = None
    PAGE_SIZE = 60

    while True:
        params = {'since': since, 'until': until, 'order_by': 'updated_at'}
        if last_conversation_id:
            params['last_conversation_id'] = last_conversation_id
        if conv_types:
            params['type'] = conv_types

        data, error = call_pancake_page_api(page, 'conversations', params=params)
        if error:
            logger.error(f"Error fetching conversations for page '{page.get('name')}': {error}")
            break

        batch = (data or {}).get('conversations', [])
        if not batch:
            break

        all_conversations.extend(batch)
        logger.info(f"Page '{page.get('name')}': +{len(batch)} conversations (total {len(all_conversations)})")

        last_conversation_id = batch[-1].get('id')
        if len(batch) < PAGE_SIZE:
            break

        time.sleep(0.5)  # polite pacing; Pancake publishes no documented rate limit

    return all_conversations
