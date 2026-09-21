"""
Pancake Page Sync
Keep Postgres table `core_pancake_page` (page_id -> page_access_token) in step with the pages
of every Pancake account, so new pages are ingested without anyone copying tokens by hand.

Accounts come from table `core_pancake_account` (created by hand, holds the account-level
"API Access Token" valid ~90 days + the brand the account belongs to). For each active account,
oldest id first:
  1. Account-level API: list every page of the account.
  2. Page already in core_pancake_page and owned by this account (or by nobody, e.g. inserted
     by hand) -> refresh descriptive info only (name, platform, is_activated, role_in_page,
     raw_info, last_synced_at) and adopt it (account_id). current_brand_id is filled from the
     account's brand ONLY when it is still NULL — a brand set by hand is never overwritten.
     Token, is_active and priority_order are never touched, no new token is requested.
  3. Page already owned by ANOTHER account -> skipped with a warning (oldest account wins).
  4. Page not in the table yet -> generate a page_access_token and INSERT it with
     is_active = TRUE and current_brand_id = the account's brand. If the token cannot be
     generated (e.g. group 'nopermission') the page is skipped and retried next run.

The task FAILS at the end (after all accounts were processed, so Airflow's email_on_failure
fires) when an account could not be synced (e.g. HTTP 401 = expired token) or its token expires
within TOKEN_EXPIRY_WARN_DAYS. token_expires_at is filled by hand and is only advisory: it never
stops an account from being synced.

Env:
  POSTGRES_URI   DWH Postgres holding core_pancake_account and core_pancake_page

Usage:
  python page_sync.py             # do the sync
  python page_sync.py --dry-run   # list what would be inserted/updated, change nothing, generate no token

Secrets: tokens are never logged. Writes use a raw cursor on purpose — PostgreSQLHelper.execute()
logs the parameters of a failed statement, which would include the token.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from loguru import logger

# Add parent directory (app/) to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from common.database import PostgreSQLHelper
from common.pancake_api import (
    ensure_pancake_page_table_exists,
    get_active_pancake_accounts,
    list_account_pages,
    generate_page_access_token,
)

POSTGRES_URI = os.getenv('POSTGRES_URI')

TOKEN_EXPIRY_WARN_DAYS = 14

UPDATE_SQL = """
    UPDATE core_pancake_page
    SET name = %s, platform = %s, is_activated = %s, role_in_page = %s,
        account_group = %s, raw_info = %s::jsonb,
        account_id = %s, current_brand_id = COALESCE(current_brand_id, %s),
        last_synced_at = now(), updated_at = now()
    WHERE page_id = %s
"""

INSERT_SQL = """
    INSERT INTO core_pancake_page
        (name, page_id, platform, page_access_token, is_active,
         is_activated, role_in_page, account_group, raw_info,
         account_id, current_brand_id, token_generated_at, last_synced_at)
    VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s, %s::jsonb, %s, %s, now(), now())
    ON CONFLICT (page_id) DO NOTHING
"""


def _write(db, sql: str, params: tuple, page_id: str) -> None:
    """Run one write on its own transaction. Never re-raises the driver error as-is: psycopg2
    messages can echo the failing row (token included), so only the SQLSTATE is kept."""
    cursor = db.conn.cursor()
    try:
        cursor.execute(sql, params)
        db.conn.commit()
    except Exception as e:
        db.conn.rollback()
        raise RuntimeError(f"DB write failed for page {page_id} "
                           f"({type(e).__name__}, pgcode={getattr(e, 'pgcode', None)})") from None
    finally:
        cursor.close()


def sync_account(db, account: dict, owners: dict, dry_run: bool, stats: dict) -> None:
    """Sync the pages of one account. `owners` maps page_id -> account_id (None = unowned) for
    every row already in core_pancake_page and is updated in place as pages are claimed."""
    access_token = account['access_token']
    account_id = account['id']
    brand_id = account['brand_id']
    label = f"account '{account['name']}' (id={account_id})"

    pages, error = list_account_pages(access_token)
    if error:
        raise RuntimeError(f"Cannot list pages of {label}: {error}")
    if not pages:
        # A 200 with no page at all almost certainly means the response shape changed
        raise RuntimeError(f"Pancake returned no pages for {label} — check the token / response format")
    logger.info(f"{label}: {len(pages)} pages")

    for group, page in pages:
        page_id = str(page['id'])
        name = page.get('name')
        platform = page.get('platform')
        raw_info = json.dumps(page, ensure_ascii=False)
        info = (name, platform, page.get('is_activated'), page.get('role_in_page'), group, raw_info)
        stats['total'] += 1

        if page_id in owners:
            owner = owners[page_id]
            if owner is not None and owner != account_id:
                logger.warning(f"Page '{name}' ({page_id}) is owned by account id={owner}; "
                               f"also listed in {label} — skipped")
                stats['duplicate'] += 1
                continue
            if dry_run:
                logger.info(f"[dry-run] would refresh existing page '{name}' ({page_id}) via {label}")
                stats['updated'] += 1
                continue
            try:
                _write(db, UPDATE_SQL, info + (account_id, brand_id, page_id), page_id)
                owners[page_id] = account_id
                stats['updated'] += 1
            except RuntimeError as e:
                logger.error(str(e))
                stats['db_failed'] += 1
            continue

        # New page
        if dry_run:
            logger.info(f"[dry-run] would generate token + insert NEW page '{name}' "
                        f"({page_id}, {platform}, group={group}, brand={brand_id}) via {label}")
            owners[page_id] = account_id
            stats['inserted'] += 1
            continue

        token, err = generate_page_access_token(access_token, page_id)
        if err:
            logger.warning(f"Skip new page '{name}' ({page_id}, group={group}): {err}")
            stats['token_failed'] += 1
            continue

        try:
            _write(db, INSERT_SQL, (name, page_id, platform, token) + info[2:] + (account_id, brand_id), page_id)
            owners[page_id] = account_id
            logger.success(f"Added new page '{name}' ({page_id}, {platform}) brand={brand_id} via {label}")
            stats['inserted'] += 1
        except RuntimeError as e:
            logger.error(str(e))
            stats['db_failed'] += 1

        time.sleep(0.3)  # gentle on the account-level API


def sync_pages(db, dry_run: bool = False) -> dict:
    if not dry_run:
        ensure_pancake_page_table_exists(db)

    accounts = get_active_pancake_accounts(db)
    if not accounts:
        raise RuntimeError("No active Pancake accounts in core_pancake_account — nothing to sync")

    # dry-run must not create core_pancake_page, so tolerate it not existing yet
    table_exists = db.fetchone("SELECT to_regclass('core_pancake_page')")[0] is not None
    owners = {}
    if table_exists:
        has_account_id = db.fetchone(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'core_pancake_page' AND column_name = 'account_id'") is not None
        query = ("SELECT page_id, account_id FROM core_pancake_page" if has_account_id
                 else "SELECT page_id, NULL FROM core_pancake_page")
        owners = {str(r[0]): r[1] for r in db.fetchall(query)}

    stats = {'total': 0, 'updated': 0, 'inserted': 0, 'duplicate': 0, 'token_failed': 0, 'db_failed': 0}
    problems = []
    now = datetime.now()

    for account in accounts:
        label = f"account '{account['name']}' (id={account['id']})"

        expires = account['token_expires_at']
        if expires is not None and expires - now <= timedelta(days=TOKEN_EXPIRY_WARN_DAYS):
            days_left = (expires - now).days
            msg = (f"{label}: access token expires {expires:%Y-%m-%d} "
                   f"({'EXPIRED' if days_left < 0 else f'{days_left} days left'}) — renew it")
            logger.warning(msg)
            problems.append(msg)

        try:
            sync_account(db, account, owners, dry_run, stats)
        except RuntimeError as e:
            logger.error(str(e))
            problems.append(str(e))

    logger.info(f"Page sync done{' (dry-run)' if dry_run else ''}: {stats}")
    if stats['db_failed']:
        problems.append(f"{stats['db_failed']} page(s) failed to write to the DB")

    if problems and not dry_run:
        raise RuntimeError("Page sync finished with problems:\n - " + "\n - ".join(problems))
    stats['problems'] = problems
    return stats


def main():
    dry_run = '--dry-run' in sys.argv

    if not POSTGRES_URI:
        raise ValueError("POSTGRES_URI environment variable not set")

    with PostgreSQLHelper(POSTGRES_URI) as db:
        sync_pages(db, dry_run=dry_run)


if __name__ == "__main__":
    main()
