import os
import logging
import uuid
from datetime import UTC, datetime

# Configure logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)

def generate_random_id() -> str:
    """
    Generate a random UUID.

    """
    return str(uuid.uuid4())

def get_current_timestamp() -> datetime:
    """
    Get current UTC timestamp.

    """
    return datetime.now(UTC)

def get_current_date_short_str() -> str:
    """
    Get current date as YYYY-MM-DD string.

    """
    return get_current_timestamp().strftime("%Y-%m-%d")

def get_current_date_full_str() -> str:
    """
    Get current date and time as YYYY-MM-DD HH:MM string.

    Returns:
        str: Current date and time in YYYY-MM-DD HH:MM format
    """
    return get_current_timestamp().strftime("%Y-%m-%d %H:%M")
