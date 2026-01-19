"""
CareSoft API Utilities
Common functions for interacting with CareSoft API across all modules
"""

import requests
from typing import Tuple, Optional, Any


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

