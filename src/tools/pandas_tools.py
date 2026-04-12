"""LangChain tools wrapping common pandas operations for agent use."""

from __future__ import annotations

from typing import Any

import pandas as pd
from langchain_core.tools import tool


@tool
def get_column_info(records: list[dict], column: str) -> dict[str, Any]:
    """Return basic info for a single column: dtype, null count, unique count, sample values."""
    df = pd.DataFrame(records)
    if column not in df.columns:
        return {"error": f"Column '{column}' not found"}
    series = df[column]
    return {
        "column": column,
        "dtype": str(series.dtype),
        "total": len(series),
        "null_count": int(series.isna().sum() + (series == "").sum()),
        "unique_count": int(series.nunique()),
        "sample_values": series.dropna().head(10).tolist(),
    }


@tool
def detect_duplicates(records: list[dict], subset: list[str] | None = None) -> dict[str, Any]:
    """Find duplicate rows (exact match). Returns indices and count."""
    df = pd.DataFrame(records)
    mask = df.duplicated(subset=subset, keep="first")
    dup_indices = df.index[mask].tolist()
    return {
        "duplicate_count": len(dup_indices),
        "duplicate_indices": dup_indices[:200],
    }


@tool
def find_null_cells(records: list[dict]) -> list[dict[str, Any]]:
    """Return list of {row, column} pairs where the value is null or empty string."""
    results: list[dict[str, Any]] = []
    for row_idx, row in enumerate(records):
        for col, val in row.items():
            if val is None or (isinstance(val, str) and val.strip() == ""):
                results.append({"row": row_idx, "column": col, "value": val})
    return results[:5000]


@tool
def get_value_counts(records: list[dict], column: str, top_n: int = 20) -> dict[str, int]:
    """Return value frequency counts for a column."""
    df = pd.DataFrame(records)
    if column not in df.columns:
        return {"error": f"Column '{column}' not found"}
    return df[column].value_counts().head(top_n).to_dict()
