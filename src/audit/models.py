from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class AuditEntry(BaseModel):
    """Single cleaning action with full provenance."""

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    row_index: int
    column_name: str
    original_value: Any
    new_value: Any
    issue_type: str  # missing | outlier | format | duplicate | type_mismatch | schema
    fix_method: str  # rule:<name> | pattern:<name> | llm:<model>
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    agent_name: str
    trace_id: str = ""


class QualityReport(BaseModel):
    """Dataset-level quality summary produced at the end of the pipeline."""

    dataset_id: str
    total_rows: int
    total_columns: int
    issues_detected: int
    issues_fixed: int
    issues_skipped: int
    overall_confidence: float
    data_quality_score_before: float
    data_quality_score_after: float
    fix_breakdown: dict[str, int] = Field(
        default_factory=dict,
        description="Count of fixes by issue_type",
    )
    duration_seconds: float = 0.0
