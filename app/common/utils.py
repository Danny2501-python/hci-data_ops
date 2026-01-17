"""
Common utility functions for the data pipeline
"""

from datetime import datetime, timezone, timedelta


# Define UTC+7 timezone (Asia/Ho_Chi_Minh)
UTC_PLUS_7 = timezone(timedelta(hours=7))


def get_current_timestamp():
    """
    Get current timestamp in UTC+7 (Asia/Ho_Chi_Minh timezone)
    
    Returns:
        datetime: Current datetime with UTC+7 timezone
    """
    return datetime.now(UTC_PLUS_7)


def get_current_timestamp_naive():
    """
    Get current timestamp in UTC+7 as naive datetime (no timezone info)
    Use this for compatibility with systems that don't support timezone-aware datetimes
    
    Returns:
        datetime: Current datetime in UTC+7 without timezone info
    """
    return datetime.now(UTC_PLUS_7).replace(tzinfo=None)
