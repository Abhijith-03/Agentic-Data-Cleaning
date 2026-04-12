from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls", ".parquet"}


def load_file(path: str | Path, **kwargs: Any) -> pd.DataFrame:
    """Load CSV, Excel, TSV, or Parquet into a DataFrame."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    ext = path.suffix.lower()
    if ext not in _SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file extension '{ext}'. Supported: {_SUPPORTED_EXTENSIONS}")

    logger.info("Loading %s (%s)", path.name, ext)

    if ext in {".csv", ".tsv"}:
        sep = kwargs.pop("sep", "\t" if ext == ".tsv" else ",")
        return pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, **kwargs)
    elif ext in {".xlsx", ".xls"}:
        sheet = kwargs.pop("sheet_name", 0)
        return pd.read_excel(path, sheet_name=sheet, dtype=str, keep_default_na=False, **kwargs)
    elif ext == ".parquet":
        return pd.read_parquet(path, **kwargs).astype(str)
    else:
        raise ValueError(f"Unhandled extension: {ext}")


def load_sql(query: str, connection_string: str, **kwargs: Any) -> pd.DataFrame:
    """Execute a SQL query and return the result as a DataFrame of strings."""
    engine = create_engine(connection_string)
    logger.info("Executing SQL query against %s", connection_string.split("@")[-1])
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, **kwargs)
    return df.astype(str)


def load(
    source: str,
    *,
    sql_query: str | None = None,
    connection_string: str | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Unified loader: auto-detects file vs SQL source."""
    if sql_query and connection_string:
        return load_sql(sql_query, connection_string, **kwargs)

    return load_file(source, **kwargs)


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to list-of-dicts for JSON-serialisable state."""
    return df.where(df.notna(), None).to_dict(orient="records")


def records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(records)
