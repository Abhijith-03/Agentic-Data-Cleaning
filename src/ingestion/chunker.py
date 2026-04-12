from __future__ import annotations

import math
from typing import Any

import pandas as pd

from src.config import settings


def chunk_dataframe(
    df: pd.DataFrame,
    chunk_size: int | None = None,
) -> list[pd.DataFrame]:
    """Split a DataFrame into row-based chunks for parallel processing."""
    chunk_size = chunk_size or settings.chunk_size
    if len(df) <= chunk_size:
        return [df]

    n_chunks = math.ceil(len(df) / chunk_size)
    return [df.iloc[i * chunk_size : (i + 1) * chunk_size].copy() for i in range(n_chunks)]


def chunk_records(
    records: list[dict[str, Any]],
    chunk_size: int | None = None,
) -> list[list[dict[str, Any]]]:
    """Split a list of record-dicts into chunks."""
    chunk_size = chunk_size or settings.chunk_size
    if len(records) <= chunk_size:
        return [records]

    return [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]
