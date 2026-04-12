"""Schema Analyzer Agent — infer schema, detect type mismatches and format inconsistencies."""

from __future__ import annotations

import logging
import random
import re
from typing import Any

import pandas as pd
from langsmith import traceable

from src.config import settings
from src.graph.state import DataCleaningState
from src.tools.regex_tools import COMMON_PATTERNS

logger = logging.getLogger(__name__)


def _infer_column_type(values: list[str]) -> dict[str, Any]:
    """Heuristic type inference for a column based on sampled values."""
    non_empty = [v for v in values if v is not None and str(v).strip() != ""]
    total = len(non_empty)
    if total == 0:
        return {"dtype": "empty", "nullable": True, "format_pattern": None}

    # Try numeric
    numeric_count = sum(1 for v in non_empty if _is_numeric(str(v)))
    if numeric_count / total > 0.8:
        has_decimal = any("." in str(v) for v in non_empty if _is_numeric(str(v)))
        return {
            "dtype": "float" if has_decimal else "integer",
            "nullable": total < len(values),
            "format_pattern": "decimal" if has_decimal else "integer",
            "numeric_rate": round(numeric_count / total, 4),
        }

    # Try boolean
    bool_vals = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}
    bool_count = sum(1 for v in non_empty if str(v).strip().lower() in bool_vals)
    if bool_count / total > 0.8:
        return {
            "dtype": "boolean",
            "nullable": total < len(values),
            "format_pattern": "boolean",
        }

    # Try date patterns
    for pattern_name in ("date_iso", "date_us", "date_eu"):
        compiled = re.compile(COMMON_PATTERNS[pattern_name])
        match_count = sum(1 for v in non_empty if compiled.match(str(v).strip()))
        if match_count / total > 0.6:
            return {
                "dtype": "date",
                "nullable": total < len(values),
                "format_pattern": pattern_name,
                "match_rate": round(match_count / total, 4),
            }

    # Try email / phone / url
    for pattern_name in ("email", "phone_us", "url", "uuid"):
        compiled = re.compile(COMMON_PATTERNS[pattern_name], re.IGNORECASE)
        match_count = sum(1 for v in non_empty if compiled.match(str(v).strip()))
        if match_count / total > 0.6:
            return {
                "dtype": "string",
                "nullable": total < len(values),
                "format_pattern": pattern_name,
                "match_rate": round(match_count / total, 4),
            }

    # Default: string
    unique_rate = len(set(non_empty)) / total if total > 0 else 0
    return {
        "dtype": "string",
        "nullable": total < len(values),
        "format_pattern": None,
        "unique_rate": round(unique_rate, 4),
    }


def _is_numeric(value: str) -> bool:
    cleaned = re.sub(r"[,$%\s]", "", value.strip())
    try:
        float(cleaned)
        return True
    except (ValueError, TypeError):
        return False


def _detect_mixed_types(values: list[str]) -> str | None:
    """Detect if a column has mixed types (e.g., numbers and text)."""
    non_empty = [str(v).strip() for v in values if v is not None and str(v).strip() != ""]
    if len(non_empty) < 2:
        return None

    numeric_count = sum(1 for v in non_empty if _is_numeric(v))
    rate = numeric_count / len(non_empty)
    if 0.1 < rate < 0.9:
        return f"mixed_types: {round(rate * 100, 1)}% numeric, {round((1 - rate) * 100, 1)}% non-numeric"
    return None


@traceable(name="schema_analyzer", metadata={"agent": "schema_analyzer"})
def schema_analyzer_node(state: DataCleaningState) -> dict[str, Any]:
    """LangGraph node: analyse schema of raw records."""
    records = state.get("raw_records", [])
    if not records:
        return {
            "inferred_schema": {},
            "schema_issues": [{"issue": "No records provided"}],
        }

    columns = list(records[0].keys())
    sample_size = min(settings.schema_sample_size, len(records))

    # Sample: first N/2 + random N/2
    half = sample_size // 2
    first_half = records[:half]
    rest = records[half:]
    random_half = random.sample(rest, min(half, len(rest))) if rest else []
    sample = first_half + random_half

    inferred_schema: dict[str, dict[str, Any]] = {}
    schema_issues: list[dict[str, Any]] = []

    for col in columns:
        col_values = [row.get(col) for row in sample]
        type_info = _infer_column_type(col_values)
        inferred_schema[col] = type_info

        mixed = _detect_mixed_types(col_values)
        if mixed:
            schema_issues.append({
                "column": col,
                "issue_type": "mixed_types",
                "details": mixed,
                "severity": "warning",
            })

        if type_info["dtype"] == "empty":
            schema_issues.append({
                "column": col,
                "issue_type": "all_empty",
                "details": "Column contains no non-empty values",
                "severity": "critical",
            })

    logger.info(
        "Schema analysis complete: %d columns, %d issues found",
        len(columns),
        len(schema_issues),
    )

    return {
        "inferred_schema": inferred_schema,
        "schema_issues": schema_issues,
    }
