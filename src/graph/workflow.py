"""LangGraph workflow — wires all agents into a stateful, cyclic graph."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from langgraph.graph import END, StateGraph

from src.agents.anomaly_detector import anomaly_detector_node
from src.agents.cleaner import cleaner_node
from src.agents.confidence_scorer import confidence_scorer_node
from src.agents.data_profiler import data_profiler_node
from src.agents.schema_analyzer import schema_analyzer_node
from src.agents.validator import validator_node
from src.config import settings
from src.graph.state import DataCleaningState
from src.ingestion.loader import dataframe_to_records, load

logger = logging.getLogger(__name__)


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

    # Add nodes
    graph.add_node("ingest", ingest_node)
    graph.add_node("schema_analysis", schema_analyzer_node)
    graph.add_node("data_profiling", data_profiler_node)
    graph.add_node("anomaly_detection", anomaly_detector_node)
    graph.add_node("cleaning", cleaner_node)
    graph.add_node("validation", validator_node)
    graph.add_node("confidence_scoring", confidence_scorer_node)
    graph.add_node("human_review", human_review_node)
    graph.add_node("output", output_node)

    # Linear edges
    graph.set_entry_point("ingest")
    graph.add_edge("ingest", "schema_analysis")
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
