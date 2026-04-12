"""Regex-based pattern detection and format normalisation tools."""

from __future__ import annotations

import re
from typing import Any

from langchain_core.tools import tool

COMMON_PATTERNS: dict[str, str] = {
    "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    "phone_us": r"^\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$",
    "zip_us": r"^\d{5}(-\d{4})?$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
    "date_us": r"^\d{1,2}/\d{1,2}/\d{2,4}$",
    "date_eu": r"^\d{1,2}\.\d{1,2}\.\d{2,4}$",
    "integer": r"^-?\d+$",
    "decimal": r"^-?\d+\.\d+$",
    "boolean": r"^(true|false|yes|no|1|0|t|f|y|n)$",
    "url": r"^https?://[^\s]+$",
    "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
}


@tool
def detect_format_pattern(values: list[str]) -> dict[str, Any]:
    """Detect the dominant format pattern in a list of values."""
    if not values:
        return {"pattern": None, "match_rate": 0.0}

    non_empty = [v for v in values if v and v.strip()]
    if not non_empty:
        return {"pattern": None, "match_rate": 0.0}

    best_pattern = None
    best_rate = 0.0

    for name, regex in COMMON_PATTERNS.items():
        compiled = re.compile(regex, re.IGNORECASE)
        matches = sum(1 for v in non_empty if compiled.match(v.strip()))
        rate = matches / len(non_empty)
        if rate > best_rate:
            best_rate = rate
            best_pattern = name

    return {
        "pattern": best_pattern,
        "match_rate": round(best_rate, 4),
        "total_checked": len(non_empty),
    }


@tool
def find_pattern_violations(
    records: list[dict], column: str, pattern_name: str
) -> list[dict[str, Any]]:
    """Find values that do NOT match the expected pattern."""
    if pattern_name not in COMMON_PATTERNS:
        return [{"error": f"Unknown pattern '{pattern_name}'. Known: {list(COMMON_PATTERNS)}"}]

    compiled = re.compile(COMMON_PATTERNS[pattern_name], re.IGNORECASE)
    violations: list[dict[str, Any]] = []

    for row_idx, row in enumerate(records):
        val = row.get(column, "")
        if val is None or (isinstance(val, str) and val.strip() == ""):
            continue
        if not compiled.match(str(val).strip()):
            violations.append({"row": row_idx, "column": column, "value": val})

    return violations[:2000]


@tool
def normalize_format(value: str, target_format: str) -> str:
    """Normalize a single value to a target format (e.g. phone, email, date_iso)."""
    value = str(value).strip()

    if target_format == "email":
        return value.lower().strip()

    if target_format == "phone_us":
        digits = re.sub(r"\D", "", value)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        return value

    if target_format == "boolean":
        low = value.lower().strip()
        if low in {"true", "yes", "1", "t", "y"}:
            return "true"
        if low in {"false", "no", "0", "f", "n"}:
            return "false"
        return value

    if target_format == "date_iso":
        for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d.%m.%Y", "%Y/%m/%d", "%m/%d/%y"):
            try:
                from datetime import datetime

                dt = datetime.strptime(value, fmt)
                if dt.year < 100:
                    dt = dt.replace(year=dt.year + 2000)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return value

    return value
