"""LangGraph workflow — wires all agents into a stateful, cyclic graph."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

from langgraph.graph import END, StateGraph

from src.agents.anomaly_detector import anomaly_detector_node
from src.agents.cleaner import cleaner_node
from src.agents.confidence_scorer import confidence_scorer_node
from src.agents.data_profiler import data_profiler_node
from src.agents.reconstruction_schema_planner import reconstruction_schema_planner_node
from src.agents.schema_analyzer import schema_analyzer_node
from src.agents.structure_reconstruction import structure_reconstruction_node
from src.agents.validator import validator_node
from src.config import settings
from src.graph.state import DataCleaningState
from src.ingestion.loader import dataframe_to_records, load

logger = logging.getLogger(__name__)

PipelineNode = Callable[[DataCleaningState], dict[str, Any]]


def _merge_state(
    state: DataCleaningState,
    updates: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(state)
    merged.update(updates)
    return merged


def _average_confidence(items: list[dict[str, Any]], field: str = "confidence") -> float | None:
    values = [float(item.get(field, 0.0)) for item in items if item.get(field) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _records_for_stage_preview(stage_name: str, state: dict[str, Any]) -> list[dict[str, Any]]:
    if stage_name in {"cleaning", "validation", "confidence_scoring", "human_review", "output"}:
        cleaned = state.get("cleaned_records") or []
        if cleaned:
            return cleaned
    return state.get("raw_records") or []


def _build_stage_preview(stage_name: str, state: dict[str, Any]) -> dict[str, Any]:
    records = _records_for_stage_preview(stage_name, state)
    preview_rows = records[:50]
    column_names = list(preview_rows[0].keys()) if preview_rows else []
    return {
        "row_count": len(records),
        "column_names": column_names,
        "rows": preview_rows,
        "truncated": len(records) > len(preview_rows),
    }


def _build_stage_summary(stage_name: str, state: dict[str, Any]) -> dict[str, Any]:
    if stage_name == "ingest":
        records = state.get("raw_records") or []
        return {
            "rows": len(records),
            "columns": len(records[0]) if records else 0,
        }
    if stage_name == "reconstruction_schema_planner":
        spec = state.get("reconstruction_spec") or {}
        return {
            "schema_found": bool(spec),
            "target_columns": spec.get("target_columns", []),
            "delimiter": spec.get("delimiter"),
            "expected_field_count": spec.get("expected_field_count"),
        }
    if stage_name == "structure_reconstruction":
        return state.get("reconstruction_report", {})
    if stage_name == "schema_analysis":
        return {
            "columns_inferred": len(state.get("inferred_schema", {})),
            "issue_count": len(state.get("schema_issues", [])),
        }
    if stage_name == "data_profiling":
        return {
            "profile_columns": len(state.get("profile_report", {})),
            "data_quality_score": state.get("data_quality_score", 0.0),
        }
    if stage_name == "anomaly_detection":
        return {"anomaly_count": len(state.get("anomalies", []))}
    if stage_name == "cleaning":
        return {
            "iteration_count": state.get("iteration_count", 0),
            "fix_count": len(state.get("cleaning_actions", [])),
        }
    if stage_name == "validation":
        return {
            "validation_passed": state.get("validation_passed", False),
            "error_count": len(state.get("validation_errors", [])),
        }
    if stage_name == "confidence_scoring":
        report = state.get("final_report", {})
        return {
            "overall_confidence": report.get("overall_confidence", 0.0),
            "issues_fixed": report.get("issues_fixed", 0),
            "issues_skipped": report.get("issues_skipped", 0),
            "low_confidence_count": len(state.get("low_confidence_fixes", [])),
        }
    if stage_name == "human_review":
        return {
            "remaining_low_confidence": len(state.get("low_confidence_fixes", [])),
        }
    if stage_name == "output":
        report = state.get("final_report", {})
        return {
            "overall_confidence": report.get("overall_confidence", 0.0),
            "validation_passed": report.get("validation_passed", False),
        }
    return {}


def _stage_confidence(stage_name: str, state: dict[str, Any]) -> float | None:
    if stage_name == "structure_reconstruction":
        confs = state.get("reconstruction_row_confidences", [])
        return round(sum(confs) / len(confs), 4) if confs else None
    if stage_name == "cleaning":
        return _average_confidence(state.get("cleaning_actions", []))
    if stage_name in {"validation", "confidence_scoring", "output"}:
        report = state.get("final_report", {})
        if "overall_confidence" in report:
            return report.get("overall_confidence")
    return None


def _instrument_node(stage_name: str, node: PipelineNode) -> PipelineNode:
    def wrapped(state: DataCleaningState) -> dict[str, Any]:
        started_at = time.time()
        started_perf = time.perf_counter()
        updates = node(state)
        completed_at = time.time()
        duration_ms = round((time.perf_counter() - started_perf) * 1000, 2)

        merged = _merge_state(state, updates)

        stages = dict(state.get("pipeline_stages", {}))
        stages[stage_name] = {
            "name": stage_name,
            "status": "success",
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": duration_ms,
            "confidence_score": _stage_confidence(stage_name, merged),
            "summary": _build_stage_summary(stage_name, merged),
        }

        previews = dict(state.get("stage_previews", {}))
        previews[stage_name] = _build_stage_preview(stage_name, merged)

        updates["pipeline_stages"] = stages
        updates["stage_previews"] = previews
        return updates

    return wrapped


# ---------------------------------------------------------------------------
# Ingest node (entry point)
# ---------------------------------------------------------------------------

def ingest_node(state: DataCleaningState) -> dict[str, Any]:
    """Load raw data and convert to records for the pipeline."""
    path = state.get("raw_data_path", "")
    records = state.get("raw_records")

    if records:
        return {"dataset_id": state.get("dataset_id", str(uuid.uuid4())[:8])}

    if not path:
        return {"errors": ["No raw_data_path or raw_records provided"]}

    df = load(path)
    records = dataframe_to_records(df)

    logger.info("Ingested %d records from %s", len(records), path)

    return {
        "raw_records": records,
        "dataset_id": state.get("dataset_id", str(uuid.uuid4())[:8]),
        "iteration_count": 0,
    }


# ---------------------------------------------------------------------------
# Conditional routing
# ---------------------------------------------------------------------------

def should_reclean(state: DataCleaningState) -> str:
    """After validation: reclean if failed and iterations remain, else score."""
    passed = state.get("validation_passed", False)
    iteration = state.get("iteration_count", 0)

    if not passed and iteration < settings.max_cleaning_iterations:
        logger.info("Validation failed, re-cleaning (iteration %d/%d)", iteration, settings.max_cleaning_iterations)
        return "reclean"

    return "score"


def should_human_review(state: DataCleaningState) -> str:
    """After confidence scoring: route to human review or output."""
    low_conf = state.get("low_confidence_fixes", [])

    if low_conf and settings.human_in_loop_enabled:
        return "human_review"

    return "output"


# ---------------------------------------------------------------------------
# Human review placeholder
# ---------------------------------------------------------------------------

def human_review_node(state: DataCleaningState) -> dict[str, Any]:
    """Placeholder for human-in-the-loop review.

    In production, this would pause the graph and expose a review UI.
    For now it auto-approves all fixes and logs a warning.
    """
    low_conf = state.get("low_confidence_fixes", [])
    logger.warning(
        "Human review requested for %d low-confidence fixes (auto-approving in dev mode)",
        len(low_conf),
    )
    return {"low_confidence_fixes": []}


# ---------------------------------------------------------------------------
# Output node
# ---------------------------------------------------------------------------

def output_node(state: DataCleaningState) -> dict[str, Any]:
    """Terminal node: finalise results."""
    report = state.get("final_report", {})
    actions = state.get("cleaning_actions", [])
    cleaned = state.get("cleaned_records", [])

    logger.info(
        "Pipeline complete: %d records cleaned, %d fixes applied, confidence=%.3f",
        len(cleaned),
        len(actions),
        report.get("overall_confidence", 0.0),
    )
    return {}


# ---------------------------------------------------------------------------
# Graph assembly
# ---------------------------------------------------------------------------

def build_graph() -> StateGraph:
    """Build and compile the data cleaning LangGraph workflow."""
    graph = StateGraph(DataCleaningState)

    nodes: dict[str, PipelineNode] = {
        "ingest": _instrument_node("ingest", ingest_node),
        "reconstruction_schema_planner": _instrument_node(
            "reconstruction_schema_planner",
            reconstruction_schema_planner_node,
        ),
        "structure_reconstruction": _instrument_node(
            "structure_reconstruction",
            structure_reconstruction_node,
        ),
        "schema_analysis": _instrument_node("schema_analysis", schema_analyzer_node),
        "data_profiling": _instrument_node("data_profiling", data_profiler_node),
        "anomaly_detection": _instrument_node("anomaly_detection", anomaly_detector_node),
        "cleaning": _instrument_node("cleaning", cleaner_node),
        "validation": _instrument_node("validation", validator_node),
        "confidence_scoring": _instrument_node("confidence_scoring", confidence_scorer_node),
        "human_review": _instrument_node("human_review", human_review_node),
        "output": _instrument_node("output", output_node),
    }

    # Add nodes
    for name, node in nodes.items():
        graph.add_node(name, node)

    # Linear edges
    graph.set_entry_point("ingest")
    graph.add_edge("ingest", "reconstruction_schema_planner")
    graph.add_edge("reconstruction_schema_planner", "structure_reconstruction")
    graph.add_edge("structure_reconstruction", "schema_analysis")
    graph.add_edge("schema_analysis", "data_profiling")
    graph.add_edge("data_profiling", "anomaly_detection")
    graph.add_edge("anomaly_detection", "cleaning")
    graph.add_edge("cleaning", "validation")

    # Conditional: validation → reclean or score
    graph.add_conditional_edges(
        "validation",
        should_reclean,
        {"reclean": "cleaning", "score": "confidence_scoring"},
    )

    # Conditional: confidence → human review or output
    graph.add_conditional_edges(
        "confidence_scoring",
        should_human_review,
        {"human_review": "human_review", "output": "output"},
    )

    graph.add_edge("human_review", "output")
    graph.add_edge("output", END)

    return graph


def compile_graph(**kwargs: Any):
    """Compile the graph, ready for invocation."""
    graph = build_graph()
    return graph.compile(**kwargs)
