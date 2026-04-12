"""Database read/write tools for persisting results and loading data."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def save_results_json(records: list[dict[str, Any]], path: str | Path) -> str:
    """Write cleaned records to a JSON file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)
    logger.info("Saved %d records to %s", len(records), path)
    return str(path)


def save_results_csv(records: list[dict[str, Any]], path: str | Path) -> str:
    """Write cleaned records to a CSV file."""
    import pandas as pd

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(path, index=False)
    logger.info("Saved %d records to %s", len(records), path)
    return str(path)
