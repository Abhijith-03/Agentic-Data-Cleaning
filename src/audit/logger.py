from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.audit.models import AuditEntry, QualityReport

logger = logging.getLogger(__name__)


class AuditLogger:
    """Append-only audit log backed by an in-memory list and optional file sink."""

    def __init__(self, sink_path: str | Path | None = None) -> None:
        self._entries: list[AuditEntry] = []
        self._sink_path = Path(sink_path) if sink_path else None
        if self._sink_path:
            self._sink_path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, entry: AuditEntry) -> None:
        self._entries.append(entry)
        if self._sink_path:
            with open(self._sink_path, "a", encoding="utf-8") as f:
                f.write(entry.model_dump_json() + "\n")

    def log_action(
        self,
        *,
        row_index: int,
        column_name: str,
        original_value: Any,
        new_value: Any,
        issue_type: str,
        fix_method: str,
        confidence: float,
        reasoning: str,
        agent_name: str,
        trace_id: str = "",
    ) -> AuditEntry:
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc),
            row_index=row_index,
            column_name=column_name,
            original_value=original_value,
            new_value=new_value,
            issue_type=issue_type,
            fix_method=fix_method,
            confidence=confidence,
            reasoning=reasoning,
            agent_name=agent_name,
            trace_id=trace_id,
        )
        self.log(entry)
        return entry

    @property
    def entries(self) -> list[AuditEntry]:
        return list(self._entries)

    def to_dicts(self) -> list[dict[str, Any]]:
        return [e.model_dump(mode="json") for e in self._entries]

    def summary(self) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        for e in self._entries:
            by_type[e.issue_type] = by_type.get(e.issue_type, 0) + 1
        return {
            "total_fixes": len(self._entries),
            "by_issue_type": by_type,
            "avg_confidence": (
                sum(e.confidence for e in self._entries) / len(self._entries)
                if self._entries
                else 0.0
            ),
        }

    def export_json(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dicts(), f, indent=2, default=str)
        logger.info("Audit log exported to %s (%d entries)", path, len(self._entries))
