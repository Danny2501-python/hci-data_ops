"""
CareSoft API Utilities
Common functions for interacting with CareSoft API across all modules
"""

import time

import requests
from loguru import logger
from typing import Tuple, Optional, Any

# Thử lại khi CareSoft trả 429 (nginx rate limit) / 5xx / lỗi mạng: chờ tăng dần (giây).
POST_RETRY_BACKOFF = (10, 30, 60, 120)
POST_RETRY_STATUS = (429, 500, 502, 503, 504)
MAX_RETRY_WAIT = 300


def _parse_retry_after(value) -> Optional[int]:
    """Header Retry-After dạng số giây; bỏ qua dạng HTTP-date vì CareSoft không dùng."""
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def get_api_url(brand: dict, path: str) -> str:
    """
    Construct CareSoft API URL
    
    Args:
        brand: Brand configuration dict with 'caresoft_domain'
        path: API endpoint path (e.g., 'tickets', 'contacts/123')
    
    Returns:
        Full API URL
    
    Example:
        >>> get_api_url(brand, 'tickets')
        'https://api.caresoft.vn/kangnam2/api/v1/tickets'
    """
    subdomain = brand["caresoft_domain"]
    # API URL pattern: https://api.caresoft.vn/{subdomain}/api/v1/{path}
    return f"https://api.caresoft.vn/{subdomain}/api/v1/{path}"


def get_api_headers(brand: dict) -> dict:
    """
    Get CareSoft API headers
    
    Args:
        brand: Brand configuration dict with 'caresoft_key'
    
    Returns:
        Dictionary of HTTP headers
    """
    api_key = brand["caresoft_key"]
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }


def call_caresoft_api(
    brand: dict,
    endpoint: str,
    resource_id: Optional[int] = None,
    timeout: int = 10,
    response_key: Optional[str] = None
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Generic function to call CareSoft API
    
    Args:
        brand: Brand configuration dict with 'caresoft_domain' and 'caresoft_key'
        endpoint: API endpoint (e.g., 'tickets', 'contacts', 'deal')
        resource_id: Optional resource ID to append to endpoint
        timeout: Request timeout in seconds (default: 10)
        response_key: Optional key to extract from JSON response (e.g., 'ticket', 'contact')
    
    Returns:
        Tuple of (data, error):
            - data: Parsed JSON data if successful, None if error
            - error: Error message if failed, None if successful
    
    Examples:
        >>> # Get ticket by ID
        >>> data, error = call_caresoft_api(brand, 'tickets', 12345, response_key='ticket')
        
        >>> # Get contact by ID
        >>> data, error = call_caresoft_api(brand, 'contacts', 67890, response_key='contact')
        
        >>> # List tickets (no ID)
        >>> data, error = call_caresoft_api(brand, 'tickets')
    """
    # Build endpoint path
    if resource_id is not None:
        path = f"{endpoint}/{resource_id}"
    else:
        path = endpoint
    
    # Construct URL and headers using brand dict
    url = get_api_url(brand, path)
    headers = get_api_headers(brand)
    
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract specific key if provided
            if response_key:
                data = data.get(response_key)
            
            return data, None
        else:
            error_msg = f"HTTP {response.status_code}: {response.text[:500]}"
            return None, error_msg
            
    except Exception as e:
        return None, str(e)

def call_caresoft_api_post(
    brand: dict,
    endpoint: str,
    data: dict,
    timeout: int = 30
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Function to call CareSoft API using POST method with JSON data

    Tự thử lại tối đa 4 lần khi gặp 429 / 5xx / lỗi mạng, chờ tăng dần 10s → 30s → 60s → 120s;
    nếu response có header Retry-After thì chờ theo nó (tối đa 300s). Các lỗi 4xx khác (401 sai
    token, 404 sai endpoint…) trả về ngay vì thử lại cũng không đổi kết quả.

    Args:
        brand: Brand configuration dict
        endpoint: API endpoint (e.g., 'marketing/insights')
        data: JSON data to send in the request body
        timeout: Request timeout in seconds (default: 30)
    
    Returns:
        Tuple of (data, error)
    """
    url = get_api_url(brand, endpoint)
    headers = get_api_headers(brand)
    domain = brand.get("caresoft_domain")

    error_msg = None
    retry_after = None

    for attempt in range(len(POST_RETRY_BACKOFF) + 1):
        if attempt:
            wait = POST_RETRY_BACKOFF[attempt - 1]
            if retry_after:
                wait = min(max(wait, retry_after), MAX_RETRY_WAIT)
            logger.warning(
                f"[{domain}/{endpoint}] {error_msg} — chờ {wait}s rồi thử lại "
                f"(lần {attempt}/{len(POST_RETRY_BACKOFF)})"
            )
            time.sleep(wait)

        retry_after = None
        try:
            response = requests.post(url, headers=headers, json=data, timeout=timeout)

            if response.status_code == 200:
                if attempt:
                    logger.info(f"[{domain}/{endpoint}] thành công sau {attempt} lần thử lại")
                return response.json(), None

            error_msg = f"HTTP {response.status_code}: {response.text[:500]}"
            if response.status_code not in POST_RETRY_STATUS:
                return None, error_msg
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))

        except Exception as e:
            error_msg = str(e)

    return None, f"{error_msg} (đã thử lại {len(POST_RETRY_BACKOFF)} lần)"
