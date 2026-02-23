"""
Utility Functions
=================

Common utilities used across the Fabric Agent package.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4


def generate_id(prefix: str = "") -> str:
    """
    Generate a unique ID with optional prefix.
    
    Args:
        prefix: Optional prefix for the ID.
    
    Returns:
        Unique ID string.
    """
    uid = str(uuid4())
    if prefix:
        return f"{prefix}-{uid}"
    return uid


def utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def compute_hash(data: Any) -> str:
    """
    Compute SHA-256 hash of data.
    
    Args:
        data: Data to hash (will be JSON serialized).
    
    Returns:
        Hex digest of hash.
    """
    data_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(data_str.encode()).hexdigest()


def safe_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Safely get nested dictionary value.
    
    Args:
        data: Dictionary to search.
        *keys: Keys to traverse.
        default: Default value if not found.
    
    Returns:
        Value at path or default.
    """
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current


def truncate_string(s: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate string to max length.
    
    Args:
        s: String to truncate.
        max_length: Maximum length.
        suffix: Suffix to add if truncated.
    
    Returns:
        Truncated string.
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


def extract_table_name(path: str) -> Optional[str]:
    """
    Extract table name from TMDL file path.
    
    Args:
        path: File path like "tables/Sales.tmdl"
    
    Returns:
        Table name or None.
    """
    match = re.search(r"tables/([^/]+)\.tmdl$", path)
    if match:
        return match.group(1)
    return None


def find_measure_references(text: str) -> List[str]:
    """
    Find measure references in TMDL text.
    
    Args:
        text: TMDL content.
    
    Returns:
        List of measure names.
    """
    measures = []
    for match in re.finditer(r"(?im)^\s*measure\s+(.+?)\s*=", text):
        name = match.group(1).strip().strip("'\"")
        measures.append(name)
    return measures


def sanitize_name(name: str) -> str:
    """
    Sanitize a name for use in file paths.
    
    Args:
        name: Name to sanitize.
    
    Returns:
        Sanitized name.
    """
    # Remove or replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    sanitized = sanitized.strip('. ')
    return sanitized or "unnamed"
