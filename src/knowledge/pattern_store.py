"""Auto-learning pattern store — captures successful LLM fixes as reusable rules.

Uses SQLite for dev persistence; can be swapped to PostgreSQL in production.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_DEFAULT_DB = Path("data/pattern_store.db")


class LearnedPattern(BaseModel):
    id: int | None = None
    column_pattern: str = Field(description="Regex matching column names")
    value_pattern: str = Field(description="Regex matching the dirty value")
    fix_template: str = Field(description="Python expression template for the fix")
    domain: str = "generic"
    success_count: int = 0
    fail_count: int = 0

    @property
    def confidence(self) -> float:
        total = self.success_count + self.fail_count
        if total == 0:
            return 0.85
        return self.success_count / total


class PatternStore:
    """SQLite-backed store for learned cleaning patterns."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        self._db_path = Path(db_path) if db_path else _DEFAULT_DB
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._create_table()

    def _create_table(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                column_pattern TEXT NOT NULL,
                value_pattern TEXT NOT NULL,
                fix_template TEXT NOT NULL,
                domain TEXT NOT NULL DEFAULT 'generic',
                success_count INTEGER NOT NULL DEFAULT 0,
                fail_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        self._conn.commit()

    def add_pattern(self, pattern: LearnedPattern) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO patterns (column_pattern, value_pattern, fix_template, domain, success_count, fail_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                pattern.column_pattern,
                pattern.value_pattern,
                pattern.fix_template,
                pattern.domain,
                pattern.success_count,
                pattern.fail_count,
            ),
        )
        self._conn.commit()
        pattern_id = cursor.lastrowid or 0
        logger.info("Added pattern #%d: col=%s val=%s", pattern_id, pattern.column_pattern, pattern.value_pattern)
        return pattern_id

    def find_match(
        self, column_name: str, value: str, domain: str = "generic"
    ) -> LearnedPattern | None:
        """Find the best matching pattern for a column name + value."""
        rows = self._conn.execute(
            "SELECT * FROM patterns WHERE domain IN (?, 'generic') ORDER BY success_count DESC",
            (domain,),
        ).fetchall()

        for row in rows:
            try:
                col_match = re.match(row["column_pattern"], column_name, re.IGNORECASE)
                val_match = re.match(row["value_pattern"], value, re.IGNORECASE)
            except re.error:
                continue

            if col_match and val_match:
                return LearnedPattern(
                    id=row["id"],
                    column_pattern=row["column_pattern"],
                    value_pattern=row["value_pattern"],
                    fix_template=row["fix_template"],
                    domain=row["domain"],
                    success_count=row["success_count"],
                    fail_count=row["fail_count"],
                )
        return None

    def apply_template(self, pattern: LearnedPattern, value: str) -> str:
        """Apply a fix template to produce the corrected value.

        Templates use `{value}` as the placeholder.  Only safe string
        operations are supported — no ``eval``.
        """
        template = pattern.fix_template
        result = template.replace("{value}", value)

        safe_ops = {
            ".strip()": lambda v: v.strip(),
            ".lower()": lambda v: v.lower(),
            ".upper()": lambda v: v.upper(),
            ".title()": lambda v: v.title(),
        }
        for op, fn in safe_ops.items():
            if op in template:
                result = fn(result.replace(op, ""))

        return result

    def record_success(self, pattern_id: int) -> None:
        self._conn.execute(
            "UPDATE patterns SET success_count = success_count + 1 WHERE id = ?",
            (pattern_id,),
        )
        self._conn.commit()

    def record_failure(self, pattern_id: int) -> None:
        self._conn.execute(
            "UPDATE patterns SET fail_count = fail_count + 1 WHERE id = ?",
            (pattern_id,),
        )
        self._conn.commit()

    def list_patterns(self, domain: str | None = None) -> list[LearnedPattern]:
        if domain:
            rows = self._conn.execute(
                "SELECT * FROM patterns WHERE domain = ? ORDER BY success_count DESC", (domain,)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM patterns ORDER BY success_count DESC"
            ).fetchall()

        return [
            LearnedPattern(
                id=r["id"],
                column_pattern=r["column_pattern"],
                value_pattern=r["value_pattern"],
                fix_template=r["fix_template"],
                domain=r["domain"],
                success_count=r["success_count"],
                fail_count=r["fail_count"],
            )
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()
