"""Anomaly Detection Agent — outliers, pattern breaks, cross-column inconsistencies."""

from __future__ import annotations

import logging
import re
from typing import Any

import numpy as np
import pandas as pd
from langsmith import traceable
from scipy import stats as sp_stats

from src.config import settings
from src.graph.state import DataCleaningState
from src.tools.regex_tools import COMMON_PATTERNS

logger = logging.getLogger(__name__)


def _detect_numeric_outliers(
    series: pd.Series, col_name: str, df: pd.DataFrame
) -> list[dict[str, Any]]:
    """Z-score + IQR intersection for precision."""
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if len(valid) < 10:
        return []

    # Z-score outliers
    zscores = np.abs(sp_stats.zscore(valid, nan_policy="omit"))
    z_outliers = set(valid.index[zscores > settings.zscore_threshold].tolist())

    # IQR outliers
    q1, q3 = valid.quantile(0.25), valid.quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - settings.iqr_multiplier * iqr, q3 + settings.iqr_multiplier * iqr
    iqr_outliers = set(valid.index[(valid < lower) | (valid > upper)].tolist())

    # Intersection for precision
    combined = z_outliers & iqr_outliers
    results = []
    for idx in sorted(combined):
        results.append({
            "row": int(idx),
            "column": col_name,
            "value": str(series.iloc[idx]),
            "anomaly_type": "numeric_outlier",
            "severity": "warning",
            "details": f"Value outside Z-score ({settings.zscore_threshold}) AND IQR bounds [{round(float(lower), 2)}, {round(float(upper), 2)}]",
        })
    return results[:500]


def _detect_categorical_anomalies(
    series: pd.Series, col_name: str
) -> list[dict[str, Any]]:
    """Values that are extremely rare (appear once in a predominantly categorical column)."""
    non_empty = series[(series.notna()) & (series != "") & (series != "None")]
    if len(non_empty) < 20:
        return []

    value_counts = non_empty.value_counts()
    unique_ratio = len(value_counts) / len(non_empty)
    if unique_ratio > 0.5:
        return []  # high-cardinality column, not categorical

    # Rare values that appear only once and account for < 1% of data
    threshold = max(1, int(len(non_empty) * 0.01))
    rare = value_counts[value_counts <= threshold]
    if len(rare) == 0 or len(rare) > 50:
        return []

    results = []
    for val in rare.index:
        row_indices = series[series == val].index.tolist()
        for idx in row_indices[:5]:
            results.append({
                "row": int(idx),
                "column": col_name,
                "value": str(val),
                "anomaly_type": "rare_category",
                "severity": "info",
                "details": f"Value '{val}' appears only {rare[val]} time(s)",
            })
    return results[:200]


def _detect_format_violations(
    series: pd.Series, col_name: str, expected_pattern: str | None
) -> list[dict[str, Any]]:
    """Find values that violate the detected format pattern."""
    if not expected_pattern or expected_pattern not in COMMON_PATTERNS:
        return []

    compiled = re.compile(COMMON_PATTERNS[expected_pattern], re.IGNORECASE)
    results = []

    for idx, val in series.items():
        if val is None or (isinstance(val, str) and val.strip() == ""):
            continue
        if not compiled.match(str(val).strip()):
            results.append({
                "row": int(idx),
                "column": col_name,
                "value": str(val),
                "anomaly_type": "format_violation",
                "severity": "warning",
                "details": f"Does not match expected pattern '{expected_pattern}'",
            })

    return results[:500]


def _detect_cross_column_issues(
    df: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Detect logical constraint violations across columns."""
    results: list[dict[str, Any]] = []

    # Date ordering: columns with 'start' before 'end'
    date_cols = [c for c in df.columns if "date" in c.lower() or "time" in c.lower()]
    start_cols = [c for c in date_cols if "start" in c.lower() or "begin" in c.lower()]
    end_cols = [c for c in date_cols if "end" in c.lower() or "finish" in c.lower()]

    for start_col in start_cols:
        for end_col in end_cols:
            try:
                starts = pd.to_datetime(df[start_col], errors="coerce")
                ends = pd.to_datetime(df[end_col], errors="coerce")
                violations = df.index[(starts.notna()) & (ends.notna()) & (starts > ends)]
                for idx in violations[:100]:
                    results.append({
                        "row": int(idx),
                        "column": f"{start_col} > {end_col}",
                        "value": f"{df[start_col].iloc[idx]} > {df[end_col].iloc[idx]}",
                        "anomaly_type": "cross_column_constraint",
                        "severity": "critical",
                        "details": f"Start date ({start_col}) is after end date ({end_col})",
                    })
            except Exception:
                continue

    return results


@traceable(name="anomaly_detector", metadata={"agent": "anomaly_detector"})
def anomaly_detector_node(state: DataCleaningState) -> dict[str, Any]:
    """LangGraph node: detect all anomalies in the dataset."""
    records = state.get("raw_records", [])
    inferred_schema = state.get("inferred_schema", {})

    if not records:
        return {"anomalies": []}

    df = pd.DataFrame(records)
    all_anomalies: list[dict[str, Any]] = []

    for col in df.columns:
        schema_info = inferred_schema.get(col, {})
        dtype = schema_info.get("dtype", "string")
        format_pattern = schema_info.get("format_pattern")

        if dtype in ("integer", "float"):
            all_anomalies.extend(_detect_numeric_outliers(df[col], col, df))

        all_anomalies.extend(_detect_categorical_anomalies(df[col], col))
        all_anomalies.extend(_detect_format_violations(df[col], col, format_pattern))

    all_anomalies.extend(_detect_cross_column_issues(df))

    logger.info("Anomaly detection complete: %d anomalies found", len(all_anomalies))

    return {"anomalies": all_anomalies}
