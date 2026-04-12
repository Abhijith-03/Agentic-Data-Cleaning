"""Validation Agent — verify cleaned data meets quality constraints.

Fully deterministic; no LLM calls.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from langsmith import traceable
from scipy import stats as sp_stats

from src.graph.state import DataCleaningState

logger = logging.getLogger(__name__)


def _check_schema_compliance(
    df: pd.DataFrame, inferred_schema: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Check that cleaned data matches inferred schema types."""
    errors: list[dict[str, Any]] = []

    for col, schema_info in inferred_schema.items():
        if col not in df.columns:
            continue

        dtype = schema_info.get("dtype", "string")
        if dtype in ("integer", "float"):
            numeric = pd.to_numeric(df[col], errors="coerce")
            non_numeric_mask = numeric.isna() & df[col].notna() & (df[col] != "") & (df[col] != "None")
            bad_indices = df.index[non_numeric_mask].tolist()
            for idx in bad_indices[:50]:
                errors.append({
                    "row": int(idx),
                    "column": col,
                    "value": str(df[col].iloc[idx]),
                    "anomaly_type": "type_mismatch",
                    "severity": "warning",
                    "details": f"Expected {dtype} but got non-numeric value",
                })

    return errors


def _check_remaining_nulls(
    df: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Check for remaining null or empty values after cleaning."""
    errors: list[dict[str, Any]] = []
    for col in df.columns:
        null_mask = df[col].isna() | (df[col] == "") | (df[col] == "None")
        null_count = int(null_mask.sum())
        if null_count > 0 and null_count / len(df) > 0.5:
            errors.append({
                "row": -1,
                "column": col,
                "value": f"{null_count} nulls remaining",
                "anomaly_type": "high_null_rate",
                "severity": "warning",
                "details": f"Column '{col}' still has {null_count}/{len(df)} ({null_count / len(df) * 100:.1f}%) null values after cleaning",
            })
    return errors


def _check_distribution_drift(
    original: pd.DataFrame,
    cleaned: pd.DataFrame,
) -> list[dict[str, Any]]:
    """KS-test to ensure cleaning didn't distort distributions."""
    errors: list[dict[str, Any]] = []

    for col in original.columns:
        if col not in cleaned.columns:
            continue

        orig_num = pd.to_numeric(original[col], errors="coerce").dropna()
        clean_num = pd.to_numeric(cleaned[col], errors="coerce").dropna()

        if len(orig_num) < 10 or len(clean_num) < 10:
            continue

        stat, p_value = sp_stats.ks_2samp(orig_num, clean_num)
        if p_value < 0.01:  # very significant drift
            errors.append({
                "row": -1,
                "column": col,
                "value": f"KS stat={stat:.4f}, p={p_value:.6f}",
                "anomaly_type": "distribution_drift",
                "severity": "critical",
                "details": f"Cleaning caused significant distribution shift in '{col}' (p={p_value:.6f})",
            })

    return errors


@traceable(name="validator", metadata={"agent": "validator"})
def validator_node(state: DataCleaningState) -> dict[str, Any]:
    """LangGraph node: validate cleaned data."""
    cleaned_records = state.get("cleaned_records", [])
    raw_records = state.get("raw_records", [])
    inferred_schema = state.get("inferred_schema", {})

    if not cleaned_records:
        return {
            "validation_passed": False,
            "validation_errors": [{"issue": "No cleaned records to validate"}],
        }

    cleaned_df = pd.DataFrame(cleaned_records)
    original_df = pd.DataFrame(raw_records) if raw_records else cleaned_df

    all_errors: list[dict[str, Any]] = []
    all_errors.extend(_check_schema_compliance(cleaned_df, inferred_schema))
    all_errors.extend(_check_remaining_nulls(cleaned_df))
    all_errors.extend(_check_distribution_drift(original_df, cleaned_df))

    passed = len([e for e in all_errors if e.get("severity") == "critical"]) == 0

    logger.info(
        "Validation %s: %d errors (%d critical)",
        "PASSED" if passed else "FAILED",
        len(all_errors),
        len([e for e in all_errors if e.get("severity") == "critical"]),
    )

    return {
        "validation_passed": passed,
        "validation_errors": all_errors,
    }
