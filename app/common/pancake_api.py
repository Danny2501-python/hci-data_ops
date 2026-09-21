"""
Pancake Inbox & Ads API Utilities
Common functions for pulling Conversations/Ads data from Pancake (developer.pancake.biz)

Auth model note (different from CareSoft):
- CareSoft: one (domain, api_key) pair per brand.
- Pancake:  one page_access_token PER connected page (Facebook/Instagram/...),
            same shape as core_facebook_ad_account -> we store one row per page
            in `core_pancake_page` and loop over pages like get_active_facebook_accounts().

Server (Page API v2): https://pages.fm/api/public_api/v2
(v1 requires `page_number` pagination and is not used here; v2 paginates by cursor.)
"""

import time
import requests
from typing import Dict, List, Optional, Tuple, Any
from loguru import logger

PAGE_API_V2_BASE = "https://pages.fm/api/public_api/v2"

# Account-level API (needs the user `access_token`, valid ~90 days): list pages / generate page token
ACCOUNT_API_BASE = "https://pages.fm/api/v1"


def redact(text: str, *secrets: Optional[str]) -> str:
    """Mask secrets in a string before it is logged or returned as an error message."""
    for s in secrets:
        if s:
            text = text.replace(s, '***')
    return text

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

# Columns filled by app/pancake/page_sync.py (account-level "list pages" response).
# Added with ALTER so tables created by the first version of the DDL are upgraded in place.
CORE_PANCAKE_PAGE_EXTRA_COLUMNS_DDL = """
ALTER TABLE core_pancake_page ADD COLUMN IF NOT EXISTS is_activated BOOLEAN;
ALTER TABLE core_pancake_page ADD COLUMN IF NOT EXISTS role_in_page TEXT;
ALTER TABLE core_pancake_page ADD COLUMN IF NOT EXISTS account_group TEXT;
ALTER TABLE core_pancake_page ADD COLUMN IF NOT EXISTS raw_info JSONB;
ALTER TABLE core_pancake_page ADD COLUMN IF NOT EXISTS token_generated_at TIMESTAMP;
ALTER TABLE core_pancake_page ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMP;
ALTER TABLE core_pancake_page ADD COLUMN IF NOT EXISTS account_id INTEGER;   -- core_pancake_account.id that owns the page
"""


def ensure_pancake_page_table_exists(db) -> None:
    """Idempotent CREATE TABLE (+ extra columns) for the page-credential config table."""
    db.execute(CORE_PANCAKE_PAGE_DDL)
    db.execute(CORE_PANCAKE_PAGE_EXTRA_COLUMNS_DDL)


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


def get_active_pancake_accounts(db) -> List[Dict]:
    """
    Load active Pancake accounts from `core_pancake_account` (one row = one Pancake login with
    its account-level "API Access Token", valid ~90 days, and the brand it belongs to).
    The table is created by hand (it holds secrets); this function only reads it.

    Returns list of dicts: id, name, access_token, token_expires_at, brand_id — ordered by id
    (oldest account first, which is also who wins when the same page is in two accounts).
    """
    rows = db.fetchall("""
        SELECT id, name, access_token, token_expires_at, brand_id
        FROM core_pancake_account
        WHERE is_active = TRUE
        ORDER BY id ASC
    """)
    accounts = [{
        'id': r[0], 'name': r[1], 'access_token': r[2],
        'token_expires_at': r[3], 'brand_id': r[4],
    } for r in rows]
    logger.info(f"Loaded {len(accounts)} active Pancake accounts")
    return accounts


def _find_pages(node: Any, group: Optional[str] = None) -> List[Tuple[Optional[str], Dict]]:
    """
    Walk the "list pages" JSON and return (group, page) for every dict that looks like a page
    (has 'id' and 'platform'). The walk stops at a page, so nested objects inside it
    (settings, staff, ...) are never mistaken for pages. `group` is the nearest parent key,
    e.g. 'activated' / 'inactivated' / 'nopermission'.
    """
    found: List[Tuple[Optional[str], Dict]] = []
    if isinstance(node, dict):
        if 'id' in node and 'platform' in node:
            found.append((group, node))
        else:
            for key, value in node.items():
                found.extend(_find_pages(value, key))
    elif isinstance(node, list):
        for item in node:
            found.extend(_find_pages(item, group))
    return found


def _find_token(node: Any) -> Optional[str]:
    """First non-empty string value whose key contains 'token' anywhere in a JSON body."""
    if isinstance(node, dict):
        for key, value in node.items():
            if 'token' in key.lower() and isinstance(value, str) and value:
                return value
        for value in node.values():
            token = _find_token(value)
            if token:
                return token
    elif isinstance(node, list):
        for value in node:
            token = _find_token(value)
            if token:
                return token
    return None


def list_account_pages(
    access_token: str, timeout: int = 60
) -> Tuple[Optional[List[Tuple[Optional[str], Dict]]], Optional[str]]:
    """
    Account-level: list every page of the account (GET {ACCOUNT_API_BASE}/pages).

    Returns ([(group, page_dict), ...], error). Pages are de-duplicated by id (first one wins).
    Each page_dict is the untouched object from the API (id, name, platform, is_activated, ...).
    """
    try:
        resp = requests.get(
            f"{ACCOUNT_API_BASE}/pages", params={'access_token': access_token}, timeout=timeout
        )
    except Exception as e:
        return None, redact(str(e), access_token)

    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}: {redact(resp.text[:300], access_token)}"

    body = resp.json()
    if not body.get('success', True):
        return None, f"success=false: {redact(str(body)[:300], access_token)}"

    seen = set()
    pages = []
    for group, page in _find_pages(body):
        page_id = str(page['id'])
        if page_id in seen:
            continue
        seen.add(page_id)
        pages.append((group, page))
    return pages, None


def generate_page_access_token(
    access_token: str, page_id: str, timeout: int = 60
) -> Tuple[Optional[str], Optional[str]]:
    """
    Account-level: ask Pancake for a page_access_token of one page
    (POST {ACCOUNT_API_BASE}/pages/{page_id}/generate_page_access_token).

    Returns (token, error). Error messages never contain the account or page token.
    NOTE: whether calling this for a page that already has a token replaces (invalidates)
    the old one is not documented — callers should only call it for pages without a token.
    """
    try:
        resp = requests.post(
            f"{ACCOUNT_API_BASE}/pages/{page_id}/generate_page_access_token",
            params={'access_token': access_token}, timeout=timeout,
        )
    except Exception as e:
        return None, redact(str(e), access_token)

    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}: {redact(resp.text[:200], access_token)}"

    body = resp.json()
    token = _find_token(body)
    if not token:
        keys = list(body.keys()) if isinstance(body, dict) else type(body).__name__
        return None, f"no token in response (keys: {keys})"
    return token, None


def call_pancake_page_api(
    page: Dict,
    path: str,
    params: Optional[Dict] = None,
    base_url: str = PAGE_API_V2_BASE,
    timeout: int = 30,
    max_retries: int = 3,
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Generic GET wrapper for the Pancake Page API (v2).

    Args:
        page: dict from get_active_pancake_pages() — needs 'page_id', 'page_access_token'
        path: endpoint under /pages/{page_id}/, e.g. 'conversations', 'statistics/pages_campaigns'
        params: extra query params (page_access_token is injected automatically)

    Returns:
        (data, error) — data is the parsed JSON body, error is a string message or None.
    Retries on HTTP 429 with exponential backoff. Pancake limits each page_id to
    5 requests/second (counted per page); exceeding it returns HTTP 429.
    """
    url = f"{base_url}/pages/{page['page_id']}/{path}"
    query = dict(params or {})
    query['page_access_token'] = page['page_access_token']

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=query, timeout=timeout)
        except Exception as e:
            # requests puts the full URL (incl. ?page_access_token=...) in its messages
            last_error = redact(str(e), page['page_access_token'])
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

        time.sleep(0.5)  # ~2 req/s per page, under Pancake's 5 req/s/page limit

    return all_conversations
