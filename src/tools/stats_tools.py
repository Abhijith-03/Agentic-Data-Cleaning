"""Statistical analysis tools for anomaly detection and profiling."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from langchain_core.tools import tool
from scipy import stats as sp_stats


@tool
def compute_column_statistics(records: list[dict], column: str) -> dict[str, Any]:
    """Compute descriptive statistics for a numeric column."""
    df = pd.DataFrame(records)
    if column not in df.columns:
        return {"error": f"Column '{column}' not found"}

    series = pd.to_numeric(df[column], errors="coerce")
    valid = series.dropna()

    if valid.empty:
        return {"column": column, "numeric_count": 0, "error": "No numeric values"}

    return {
        "column": column,
        "count": len(df[column]),
        "numeric_count": len(valid),
        "non_numeric_count": int(series.isna().sum()),
        "mean": float(valid.mean()),
        "median": float(valid.median()),
        "std": float(valid.std()) if len(valid) > 1 else 0.0,
        "min": float(valid.min()),
        "max": float(valid.max()),
        "skew": float(valid.skew()) if len(valid) > 2 else 0.0,
        "kurtosis": float(valid.kurtosis()) if len(valid) > 3 else 0.0,
        "q1": float(valid.quantile(0.25)),
        "q3": float(valid.quantile(0.75)),
    }


@tool
def detect_zscore_outliers(
    records: list[dict], column: str, threshold: float = 3.0
) -> list[dict[str, Any]]:
    """Detect outliers using Z-score method. Returns row indices and values."""
    df = pd.DataFrame(records)
    series = pd.to_numeric(df[column], errors="coerce")
    valid_mask = series.notna()
    valid = series[valid_mask]

    if len(valid) < 3:
        return []

    zscores = np.abs(sp_stats.zscore(valid, nan_policy="omit"))
    outlier_mask = zscores > threshold
    outlier_indices = valid.index[outlier_mask].tolist()

    return [
        {
            "row": int(idx),
            "column": column,
            "value": str(df[column].iloc[idx]),
            "zscore": round(float(zscores[valid.index.get_loc(idx)]), 3),
        }
        for idx in outlier_indices[:500]
    ]


@tool
def detect_iqr_outliers(
    records: list[dict], column: str, multiplier: float = 1.5
) -> list[dict[str, Any]]:
    """Detect outliers using IQR method."""
    df = pd.DataFrame(records)
    series = pd.to_numeric(df[column], errors="coerce").dropna()

    if len(series) < 4:
        return []

    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - multiplier * iqr, q3 + multiplier * iqr

    mask = (series < lower) | (series > upper)
    outlier_indices = series.index[mask].tolist()

    return [
        {
            "row": int(idx),
            "column": column,
            "value": str(df[column].iloc[idx]),
            "bounds": {"lower": round(float(lower), 3), "upper": round(float(upper), 3)},
        }
        for idx in outlier_indices[:500]
    ]


@tool
def ks_test_distributions(
    records_a: list[dict], records_b: list[dict], column: str
) -> dict[str, Any]:
    """Two-sample KS test to check whether distributions diverged after cleaning."""
    a = pd.to_numeric(pd.DataFrame(records_a)[column], errors="coerce").dropna()
    b = pd.to_numeric(pd.DataFrame(records_b)[column], errors="coerce").dropna()

    if len(a) < 2 or len(b) < 2:
        return {"error": "Not enough numeric values for comparison"}

    stat, p_value = sp_stats.ks_2samp(a, b)
    return {
        "column": column,
        "ks_statistic": round(float(stat), 6),
        "p_value": round(float(p_value), 6),
        "significant_drift": p_value < 0.05,
    }
