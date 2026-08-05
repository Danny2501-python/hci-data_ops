"""
Pancake API Utilities
Shared helper class for interacting with Pancake POS API and Pancake Social/Pages API.
"""

import os
import sys
import time
import requests
from typing import Dict, List, Optional, Any
from loguru import logger


class PancakeAPIClient:
    """
    Pancake API Client supporting both Pancake POS API and Pancake Pages/Social API.
    
    Handles:
    - User Access Token authentication
    - Dynamic Page Access Token generation & caching
    - Rate limit handling (5 req/sec default limit) with exponential backoff on HTTP 429
    - Pagination for Shops, Orders, Customers, Products, and Conversations
    """
    
    POS_BASE_URL = "https://pos.pancake.vn/api/v1"
    PAGES_BASE_URL = "https://pages.fm/api/v1"
    PUBLIC_PAGES_BASE_URL = "https://pages.fm/api/public_api/v1"
    PUBLIC_PAGES_V2_BASE_URL = "https://pages.fm/api/public_api/v2"

    def __init__(self, user_access_token: Optional[str] = None):
        """
        Initialize Pancake API Client.
        If user_access_token is not provided, load from environment variable PANCAKE_USER_ACCESS_TOKEN
        or fallback to reading from local credentials file.
        """
        self.user_access_token = user_access_token or os.getenv("PANCAKE_USER_ACCESS_TOKEN")
        
        if not self.user_access_token:
            # Fallback for local testing if file exists
            cre_file = r"D:\Dự án\Pancake\Cre.txt"
            if os.path.exists(cre_file):
                try:
                    with open(cre_file, "r", encoding="utf-8") as f:
                        for line in f:
                            if "User access token" in line:
                                self.user_access_token = line.split(":", 1)[1].strip()
                                logger.info("Loaded Pancake User Access Token from Cre.txt fallback")
                                break
                except Exception as e:
                    logger.warning(f"Could not read Cre.txt fallback: {e}")
                    
        if not self.user_access_token:
            logger.error("Pancake User Access Token is missing! Please set PANCAKE_USER_ACCESS_TOKEN environment variable.")
            
        self.page_tokens_cache: Dict[str, str] = {}
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "DataEngine/1.0 (Pancake Integration)",
            "Accept": "application/json"
        })

    def _request(self, method: str, url: str, params: Optional[Dict] = None, json_data: Optional[Dict] = None, max_retries: int = 5) -> Optional[Dict[str, Any]]:
        """
        Internal HTTP request runner with rate-limiting backoff.
        """
        params = params or {}
        retry_delay = 2.0

        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    timeout=30
                )

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logger.warning(f"Rate limited (429) on {url}. Retrying in {retry_delay}s (Attempt {attempt}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2.0
                elif response.status_code in (500, 502, 503, 504):
                    logger.warning(f"Server error ({response.status_code}) on {url}. Retrying in {retry_delay}s")
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                else:
                    logger.error(f"API Request failed [{response.status_code}] {url}: {response.text[:200]}")
                    return None
            except Exception as e:
                logger.error(f"Request exception on {url}: {e}")
                time.sleep(retry_delay)
                retry_delay *= 1.5

        logger.error(f"Exceeded max retries for {url}")
        return None

    # =========================================================================
    # POS API ENDPOINTS
    # =========================================================================

    def get_shops(self) -> List[Dict[str, Any]]:
        """Fetch all POS Shops accessible by the user."""
        url = f"{self.POS_BASE_URL}/shops"
        params = {"access_token": self.user_access_token}
        data = self._request("GET", url, params=params)
        if data and data.get("success"):
            return data.get("shops", [])
        return []

    def get_orders(self, shop_id: str, page: int = 1, page_size: int = 100, since: Optional[str] = None, until: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch Orders from a specific POS Shop."""
        url = f"{self.POS_BASE_URL}/shops/{shop_id}/orders"
        params = {
            "access_token": self.user_access_token,
            "page_number": page,
            "page_size": page_size
        }
        if since:
            params["since_date"] = since
        if until:
            params["until_date"] = until

        data = self._request("GET", url, params=params)
        if data and data.get("success"):
            return data.get("orders", [])
        elif data and isinstance(data.get("data"), list):
            return data.get("data")
        return []

    def get_customers(self, shop_id: str, page: int = 1, page_size: int = 100) -> List[Dict[str, Any]]:
        """Fetch Customers from a specific POS Shop."""
        url = f"{self.POS_BASE_URL}/shops/{shop_id}/customers"
        params = {
            "access_token": self.user_access_token,
            "page_number": page,
            "page_size": page_size
        }
        data = self._request("GET", url, params=params)
        if data and data.get("success"):
            return data.get("customers", [])
        return []

    def get_products(self, shop_id: str, page: int = 1, page_size: int = 100) -> List[Dict[str, Any]]:
        """Fetch Products from a specific POS Shop."""
        url = f"{self.POS_BASE_URL}/shops/{shop_id}/products"
        params = {
            "access_token": self.user_access_token,
            "page_number": page,
            "page_size": page_size
        }
        data = self._request("GET", url, params=params)
        if data and data.get("success"):
            return data.get("products", [])
        return []

    # =========================================================================
    # PAGES / SOCIAL API ENDPOINTS
    # =========================================================================

    def get_pages(self) -> List[Dict[str, Any]]:
        """Fetch list of managed Facebook Pages / Zalo OAs from Pancake Social."""
        url = f"{self.PAGES_BASE_URL}/pages"
        params = {"access_token": self.user_access_token}
        data = self._request("GET", url, params=params)
        if data and isinstance(data.get("pages"), list):
            return data.get("pages")
        return []

    def generate_page_access_token(self, page_id: str) -> Optional[str]:
        """Dynamically generate or refresh a page_access_token for a given page_id."""
        if page_id in self.page_tokens_cache:
            return self.page_tokens_cache[page_id]

        url = f"{self.PAGES_BASE_URL}/pages/{page_id}/generate_page_access_token"
        params = {"access_token": self.user_access_token}
        data = self._request("POST", url, params=params)

        if data and data.get("page_access_token"):
            token = data.get("page_access_token")
            self.page_tokens_cache[page_id] = token
            return token
        return None

    def get_conversations(self, page_id: str, page_access_token: Optional[str] = None, fields: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch Conversations for a specific Page."""
        token = page_access_token or self.generate_page_access_token(page_id)
        if not token:
            logger.error(f"Cannot get page_access_token for page {page_id}")
            return []

        url = f"{self.PUBLIC_PAGES_V2_BASE_URL}/pages/{page_id}/conversations"
        params = {"page_access_token": token}
        if fields:
            params["fields"] = fields

        data = self._request("GET", url, params=params)
        if data and isinstance(data.get("conversations"), list):
            return data.get("conversations")
        return []
