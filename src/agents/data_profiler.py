"""Data Profiling Agent — statistical profiling of every column."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from langsmith import traceable

from src.graph.state import DataCleaningState

logger = logging.getLogger(__name__)


def _profile_column(series: pd.Series, col_name: str) -> dict[str, Any]:
    """Generate a profile report for a single column."""
    total = len(series)
    null_count = int(series.isna().sum() + (series == "").sum() + (series == "None").sum())
    non_null = series[(series.notna()) & (series != "") & (series != "None")]
    unique_count = int(non_null.nunique())

    profile: dict[str, Any] = {
        "column": col_name,
        "total": total,
        "null_count": null_count,
        "null_pct": round(null_count / total * 100, 2) if total > 0 else 0,
        "unique_count": unique_count,
        "unique_pct": round(unique_count / total * 100, 2) if total > 0 else 0,
    }

    numeric = pd.to_numeric(non_null, errors="coerce").dropna()
    if len(numeric) > 0 and len(numeric) / max(len(non_null), 1) > 0.5:
        profile.update({
            "is_numeric": True,
            "mean": round(float(numeric.mean()), 6),
            "median": round(float(numeric.median()), 6),
            "std": round(float(numeric.std()), 6) if len(numeric) > 1 else 0.0,
            "min": float(numeric.min()),
            "max": float(numeric.max()),
            "skew": round(float(numeric.skew()), 4) if len(numeric) > 2 else 0.0,
            "q1": round(float(numeric.quantile(0.25)), 6),
            "q3": round(float(numeric.quantile(0.75)), 6),
        })
    else:
        profile["is_numeric"] = False
        if len(non_null) > 0:
            value_counts = non_null.value_counts()
            profile["top_values"] = value_counts.head(10).to_dict()
            profile["min_length"] = int(non_null.str.len().min())
            profile["max_length"] = int(non_null.str.len().max())
            profile["mean_length"] = round(float(non_null.str.len().mean()), 2)

    return profile


def _compute_quality_score(profiles: dict[str, dict[str, Any]]) -> float:
    """Compute an overall data quality score (0.0 - 1.0)."""
    if not profiles:
        return 0.0

    scores: list[float] = []
    for col_profile in profiles.values():
        null_pct = col_profile.get("null_pct", 0)
        completeness = max(0.0, (100 - null_pct) / 100)

        unique_pct = col_profile.get("unique_pct", 0)
        # penalise columns with very low cardinality (single-value) or 100% nulls
        variety = 1.0 if unique_pct > 1 else 0.5

        scores.append(completeness * 0.7 + variety * 0.3)

    return round(float(np.mean(scores)), 4)


@traceable(name="data_profiler", metadata={"agent": "data_profiler"})
def data_profiler_node(state: DataCleaningState) -> dict[str, Any]:
    """LangGraph node: profile every column of the dataset."""
    records = state.get("raw_records", [])
    if not records:
        return {"profile_report": {}, "data_quality_score": 0.0}

    df = pd.DataFrame(records)
    profile_report: dict[str, dict[str, Any]] = {}

    for col in df.columns:
        profile_report[col] = _profile_column(df[col], col)

    quality_score = _compute_quality_score(profile_report)

    logger.info(
        "Profiling complete: %d columns, quality score=%.4f",
        len(df.columns),
        quality_score,
    )

    return {
        "profile_report": profile_report,
        "data_quality_score": quality_score,
    }
