"""
Common utility functions
"""
import re
import logging
from typing import Any

logger = logging.getLogger(__name__)


def clean_text(text: Any) -> str:
    """
    Clean text by removing non-alphanumeric characters except common punctuation
    
    Args:
        text: Input text to clean
        
    Returns:
        Cleaned text string
    """
    if text is None:
        return ""
    
    # Convert to string and clean
    cleaned = re.sub(r'[^a-zA-Z0-9,."\']', '', str(text))
    return cleaned.strip()


def is_numeric_string(value: Any) -> bool:
    """
    Check if a value can be converted to a number
    
    Args:
        value: Value to check
        
    Returns:
        True if value is numeric, False otherwise
    """
    if value is None:
        return False
    
    try:
        float(str(value))
        return True
    except (ValueError, TypeError):
        return False


def safe_cast_to_float(value: Any) -> float:
    """
    Safely cast value to float, returning 0.0 if conversion fails
    
    Args:
        value: Value to cast
        
    Returns:
        Float value or 0.0 if conversion fails
    """
    if value is None:
        return 0.0
    
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert {value} to float, returning 0.0")
        return 0.0


def validate_required_columns(df: "DataFrame", required_columns: list[str]) -> bool:
    """
    Validate that DataFrame contains all required columns
    
    Args:
        df: Spark DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if all columns exist, False otherwise
    """
    existing_columns = set(df.columns)
    missing_columns = set(required_columns) - existing_columns
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    return True
