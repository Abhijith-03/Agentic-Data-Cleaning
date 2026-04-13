from __future__ import annotations

from typing import Any, TypedDict


class DataCleaningState(TypedDict, total=False):
    """Shared state threaded through every node of the LangGraph pipeline."""

    # --- Input ---
    raw_data_path: str
    dataset_id: str
    raw_records: list[dict[str, Any]]

    # --- Structure reconstruction ---
    reconstruction_spec: dict[str, Any]
    reconstruction_report: dict[str, Any]
    reconstruction_row_confidences: list[float]

    # --- Pipeline execution telemetry ---
    pipeline_stages: dict[str, dict[str, Any]]
    stage_previews: dict[str, dict[str, Any]]

    # --- Schema analysis ---
    inferred_schema: dict[str, dict[str, Any]]
    schema_issues: list[dict[str, Any]]

    # --- Profiling ---
    profile_report: dict[str, dict[str, Any]]
    data_quality_score: float

    # --- Anomaly detection ---
    anomalies: list[dict[str, Any]]

    # --- Cleaning ---
    cleaning_actions: list[dict[str, Any]]
    cleaned_records: list[dict[str, Any]]

    # --- Validation ---
    validation_passed: bool
    validation_errors: list[dict[str, Any]]

    # --- Confidence & routing ---
    low_confidence_fixes: list[dict[str, Any]]
    llm_logs: list[dict[str, Any]]
    review_queue: list[dict[str, Any]]
    iteration_count: int

    # --- Audit ---
    audit_log: list[dict[str, Any]]
    final_report: dict[str, Any]

    # --- Metadata ---
    chunk_index: int
    total_chunks: int
    errors: list[str]
