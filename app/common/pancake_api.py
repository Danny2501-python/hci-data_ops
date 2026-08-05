"""Shared clients for Pancake POS and Pancake Pages APIs."""

import os
import time
from typing import Any, Dict, List, Optional

import requests
from loguru import logger


class PancakeAPIError(RuntimeError):
    """Raised when Pancake returns an HTTP error or an unexpected payload."""


class PancakeAPIClient:
    """Client for Pancake POS and Pancake Pages APIs."""

    POS_BASE_URL = "https://pos.pancake.vn/api/v1"
    PAGES_BASE_URL = "https://pages.fm/api/v1"
    PUBLIC_PAGES_BASE_URL = "https://pages.fm/api/public_api/v1"
    PUBLIC_PAGES_V2_BASE_URL = "https://pages.fm/api/public_api/v2"

    def __init__(self, user_access_token: Optional[str] = None):
        self.user_access_token = user_access_token or os.getenv(
            "PANCAKE_USER_ACCESS_TOKEN"
        )
        if not self.user_access_token:
            raise ValueError(
                "PANCAKE_USER_ACCESS_TOKEN is required; configure it as an Airflow "
                "Variable or environment secret"
            )

        self.page_tokens_cache: Dict[str, str] = {}
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "DataEngine/1.0 (Pancake Integration)",
                "Accept": "application/json",
            }
        )

    def _request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        max_retries: int = 5,
    ) -> Dict[str, Any]:
        """Run an HTTP request and fail the task when Pancake cannot be read."""
        retry_delay = 2.0
        last_error: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params or {},
                    json=json_data,
                    timeout=30,
                )
            except requests.RequestException as exc:
                last_error = exc
                logger.warning(
                    f"Pancake request error on {url}; retrying in {retry_delay}s "
                    f"(attempt {attempt}/{max_retries}): {exc}"
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                continue

            if 200 <= response.status_code < 300:
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise PancakeAPIError(
                        f"Pancake returned non-JSON content for {url}"
                    ) from exc
                if not isinstance(payload, dict):
                    raise PancakeAPIError(
                        f"Pancake returned an unexpected payload type for {url}"
                    )
                return payload

            if response.status_code == 429 or response.status_code in {
                500,
                502,
                503,
                504,
            }:
                last_error = PancakeAPIError(
                    f"Pancake HTTP {response.status_code} for {url}"
                )
                logger.warning(
                    f"{last_error}; retrying in {retry_delay}s "
                    f"(attempt {attempt}/{max_retries})"
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 2.0 if response.status_code == 429 else 1.5
                continue

            raise PancakeAPIError(
                f"Pancake request failed with HTTP {response.status_code} for {url}: "
                f"{response.text[:200]}"
            )

        raise PancakeAPIError(
            f"Pancake request failed after {max_retries} attempts for {url}: "
            f"{last_error}"
        )

    @staticmethod
    def _collection(
        payload: Dict[str, Any], endpoint: str, *candidate_keys: str
    ) -> List[Dict[str, Any]]:
        """Extract a list while validating Pancake's success/response contract."""
        if payload.get("success") is False:
            raise PancakeAPIError(
                f"Pancake {endpoint} returned success=false: "
                f"{payload.get('message') or payload.get('error') or 'unknown error'}"
            )

        for key in candidate_keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        raise PancakeAPIError(
            f"Pancake {endpoint} response does not contain any expected list key: "
            f"{', '.join(candidate_keys)}"
        )

    # POS API endpoints

    def get_shops(self) -> List[Dict[str, Any]]:
        payload = self._request(
            "GET",
            f"{self.POS_BASE_URL}/shops",
            params={"access_token": self.user_access_token},
        )
        return self._collection(payload, "shops", "shops", "data")

    def get_orders(
        self,
        shop_id: str,
        page: int = 1,
        page_size: int = 100,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "access_token": self.user_access_token,
            "page_number": page,
            "page_size": page_size,
        }
        if since:
            params["since_date"] = since
        if until:
            params["until_date"] = until

        payload = self._request(
            "GET",
            f"{self.POS_BASE_URL}/shops/{shop_id}/orders",
            params=params,
        )
        return self._collection(payload, "orders", "orders", "data")

    def get_customers(
        self, shop_id: str, page: int = 1, page_size: int = 100
    ) -> List[Dict[str, Any]]:
        payload = self._request(
            "GET",
            f"{self.POS_BASE_URL}/shops/{shop_id}/customers",
            params={
                "access_token": self.user_access_token,
                "page_number": page,
                "page_size": page_size,
            },
        )
        return self._collection(payload, "customers", "customers", "data")

    def get_products(
        self, shop_id: str, page: int = 1, page_size: int = 100
    ) -> List[Dict[str, Any]]:
        payload = self._request(
            "GET",
            f"{self.POS_BASE_URL}/shops/{shop_id}/products",
            params={
                "access_token": self.user_access_token,
                "page_number": page,
                "page_size": page_size,
            },
        )
        return self._collection(payload, "products", "products", "data")

    # Pages / Social API endpoints

    def get_pages(self) -> List[Dict[str, Any]]:
        """Return managed pages from both documented and current response shapes."""
        payload = self._request(
            "GET",
            f"{self.PAGES_BASE_URL}/pages",
            params={"access_token": self.user_access_token},
        )
        if isinstance(payload.get("pages"), list):
            return self._collection(payload, "pages", "pages")

        categorized = payload.get("categorized")
        if not isinstance(categorized, dict):
            raise PancakeAPIError(
                "Pancake pages response contains neither pages nor categorized"
            )

        pages: List[Dict[str, Any]] = []
        seen_page_ids = set()
        for category in ("activated", "inactivated", "hidden", "nopermission"):
            for page in categorized.get(category) or []:
                if not isinstance(page, dict):
                    continue
                page_id = str(page.get("id") or "")
                if page_id and page_id not in seen_page_ids:
                    page["pancake_category"] = category
                    pages.append(page)
                    seen_page_ids.add(page_id)
        return pages

    def generate_page_access_token(self, page_id: str) -> str:
        """Generate a page token explicitly; regeneration invalidates the old token."""
        if page_id in self.page_tokens_cache:
            return self.page_tokens_cache[page_id]

        payload = self._request(
            "POST",
            f"{self.PAGES_BASE_URL}/pages/{page_id}/generate_page_access_token",
            params={"access_token": self.user_access_token},
        )
        token = payload.get("page_access_token")
        if not token:
            raise PancakeAPIError(
                f"Pancake did not return page_access_token for page {page_id}"
            )
        self.page_tokens_cache[page_id] = str(token)
        return str(token)

    def get_page_customers(
        self,
        page_id: str,
        page_access_token: str,
        since: int,
        until: int,
        page: int = 1,
        page_size: int = 100,
        order_by: str = "inserted_at",
    ) -> List[Dict[str, Any]]:
        """Read Pancake page customers, including phone_numbers and emails."""
        payload = self._request(
            "GET",
            f"{self.PUBLIC_PAGES_BASE_URL}/pages/{page_id}/page_customers",
            params={
                "page_access_token": page_access_token,
                "since": since,
                "until": until,
                "page_number": page,
                "page_size": min(page_size, 100),
                "order_by": order_by,
            },
        )
        return self._collection(payload, "page_customers", "customers", "data")

    def get_conversations(
        self,
        page_id: str,
        page_access_token: str,
        fields: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"page_access_token": page_access_token}
        if fields:
            params["fields"] = fields
        payload = self._request(
            "GET",
            f"{self.PUBLIC_PAGES_V2_BASE_URL}/pages/{page_id}/conversations",
            params=params,
        )
        return self._collection(payload, "conversations", "conversations", "data")
