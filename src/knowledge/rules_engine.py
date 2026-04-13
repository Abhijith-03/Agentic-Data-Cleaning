"""Deterministic rules engine — the fast path for data cleaning.

Each rule returns a CleaningAction or None if it cannot handle the issue.
Rules are tried in priority order; first match wins.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
from thefuzz import fuzz


@dataclass
class CleaningAction:
    new_value: Any
    rule_name: str
    confidence: float
    reasoning: str
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Missing-value rules
# ---------------------------------------------------------------------------

def impute_numeric_median(
    column_values: list[Any],
    current_value: Any,
    column_name: str,
) -> CleaningAction | None:
    """Impute missing numeric value with column median."""
    if current_value is not None and str(current_value).strip() != "":
        return None

    numeric = pd.to_numeric(pd.Series(column_values), errors="coerce").dropna()
    if numeric.empty:
        return None

    median_val = round(float(numeric.median()), 6)
    return CleaningAction(
        new_value=str(median_val),
        rule_name="rule:median_impute",
        confidence=0.90,
        reasoning=f"Missing numeric value in '{column_name}' replaced with column median ({median_val})",
    )


def impute_categorical_mode(
    column_values: list[Any],
    current_value: Any,
    column_name: str,
) -> CleaningAction | None:
    """Impute missing categorical value with mode."""
    if current_value is not None and str(current_value).strip() != "":
        return None

    series = pd.Series([v for v in column_values if v is not None and str(v).strip() != ""])
    if series.empty:
        return None

    mode_val = series.mode()
    if mode_val.empty:
        return None

    return CleaningAction(
        new_value=str(mode_val.iloc[0]),
        rule_name="rule:mode_impute",
        confidence=0.85,
        reasoning=f"Missing categorical value in '{column_name}' replaced with mode ('{mode_val.iloc[0]}')",
    )


def impute_unknown(
    current_value: Any,
    column_name: str,
) -> CleaningAction | None:
    """Last-resort: fill missing with 'Unknown'."""
    if current_value is not None and str(current_value).strip() != "":
        return None
    return CleaningAction(
        new_value="Unknown",
        rule_name="rule:unknown_fill",
        confidence=0.70,
        reasoning=f"Missing value in '{column_name}' filled with 'Unknown' (no better imputation available)",
    )


# ---------------------------------------------------------------------------
# Format normalisation rules
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    ("%m/%d/%Y", "date_us"),
    ("%d/%m/%Y", "date_eu_slash"),
    ("%m-%d-%Y", "date_us_dash"),
    ("%d.%m.%Y", "date_eu_dot"),
    ("%Y/%m/%d", "date_iso_slash"),
    ("%m/%d/%y", "date_us_short"),
    ("%d-%m-%Y", "date_eu_dash"),
]


def normalize_date_to_iso(value: str, column_name: str) -> CleaningAction | None:
    """Convert various date formats to ISO 8601 (YYYY-MM-DD)."""
    if not value or not value.strip():
        return None
    value = value.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return None  # already ISO

    for fmt, fmt_name in _DATE_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            iso = dt.strftime("%Y-%m-%d")
            return CleaningAction(
                new_value=iso,
                rule_name=f"rule:date_normalize_{fmt_name}",
                confidence=0.95,
                reasoning=f"Converted '{value}' to ISO date '{iso}' in '{column_name}'",
            )
        except ValueError:
            continue
    return None


def normalize_phone_us(value: str, column_name: str) -> CleaningAction | None:
    """Normalise US phone numbers to (XXX) XXX-XXXX format."""
    if not value or not value.strip():
        return None
    digits = re.sub(r"\D", "", value.strip())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if formatted == value.strip():
        return None
    return CleaningAction(
        new_value=formatted,
        rule_name="rule:phone_normalize",
        confidence=0.95,
        reasoning=f"Normalized phone '{value}' → '{formatted}' in '{column_name}'",
    )


def normalize_email(value: str, column_name: str) -> CleaningAction | None:
    """Lowercase and strip whitespace from emails."""
    if not value or not value.strip():
        return None
    cleaned = value.strip().lower()
    if cleaned == value:
        return None
    if not re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", cleaned):
        return None
    return CleaningAction(
        new_value=cleaned,
        rule_name="rule:email_normalize",
        confidence=0.98,
        reasoning=f"Normalized email '{value}' → '{cleaned}' in '{column_name}'",
    )


def coerce_boolean(value: str, column_name: str) -> CleaningAction | None:
    """Normalise boolean-like strings to 'true' / 'false'."""
    if not value or not value.strip():
        return None
    low = value.strip().lower()
    mapping = {"yes": "true", "y": "true", "1": "true", "t": "true",
               "no": "false", "n": "false", "0": "false", "f": "false"}
    if low in mapping and low not in {"true", "false"}:
        new_val = mapping[low]
        return CleaningAction(
            new_value=new_val,
            rule_name="rule:boolean_coerce",
            confidence=0.95,
            reasoning=f"Coerced '{value}' → '{new_val}' in '{column_name}'",
        )
    return None


def coerce_numeric_string(value: str, column_name: str) -> CleaningAction | None:
    """Strip non-numeric chars from strings that are clearly numbers (e.g. '$1,234')."""
    if not value or not value.strip():
        return None
    stripped = value.strip()
    if re.match(r"^-?\d+\.?\d*$", stripped):
        return None  # already clean numeric
    cleaned = re.sub(r"[^\d.\-]", "", stripped)
    if not cleaned or cleaned in {".", "-", "-."}:
        return None
    try:
        float(cleaned)
    except ValueError:
        return None
    if cleaned == stripped:
        return None
    return CleaningAction(
        new_value=cleaned,
        rule_name="rule:numeric_coerce",
        confidence=0.90,
        reasoning=f"Stripped non-numeric characters: '{value}' → '{cleaned}' in '{column_name}'",
    )


# ---------------------------------------------------------------------------
# Whitespace / trim rules
# ---------------------------------------------------------------------------

def trim_whitespace(value: str, column_name: str) -> CleaningAction | None:
    """Remove leading/trailing whitespace and collapse internal runs."""
    if not isinstance(value, str):
        return None
    trimmed = " ".join(value.split())
    if trimmed == value:
        return None
    return CleaningAction(
        new_value=trimmed,
        rule_name="rule:trim_whitespace",
        confidence=0.99,
        reasoning=f"Trimmed whitespace in '{column_name}'",
    )


# ---------------------------------------------------------------------------
# Duplicate detection (fuzzy)
# ---------------------------------------------------------------------------

def find_fuzzy_duplicate(
    value: str,
    canonical_values: list[str],
    threshold: float = 0.9,
) -> str | None:
    """Return the best fuzzy match from canonical values, or None."""
    if not value or not canonical_values:
        return None
    best_score = 0
    best_match = None
    for candidate in canonical_values:
        score = fuzz.ratio(value.lower(), candidate.lower()) / 100.0
        if score > best_score:
            best_score = score
            best_match = candidate
    if best_match and best_score >= threshold and best_match != value:
        return best_match
    return None


# ---------------------------------------------------------------------------
# Aggregated rule runner
# ---------------------------------------------------------------------------

FORMAT_RULES = [
    trim_whitespace,
    normalize_date_to_iso,
    normalize_phone_us,
    normalize_email,
    coerce_boolean,
    coerce_numeric_string,
]


def apply_format_rules(value: str, column_name: str) -> CleaningAction | None:
    """Try every format rule in order; return first match."""
    for rule_fn in FORMAT_RULES:
        result = rule_fn(value, column_name)
        if result is not None:
            return result
    return None
