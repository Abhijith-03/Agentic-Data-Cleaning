"""Confidence Scoring Agent — aggregate fix confidence, route low-confidence items."""

from __future__ import annotations

import logging
from typing import Any

from langsmith import traceable

from src.config import settings
from src.graph.state import DataCleaningState

logger = logging.getLogger(__name__)


def _categorize_fix(fix: dict[str, Any]) -> str:
    """Classify a fix into a confidence tier."""
    rule = fix.get("rule", "")
    if rule.startswith("rule:"):
        return "tier1_deterministic"
    elif rule.startswith("pattern:"):
        return "tier2_pattern"
    elif rule.startswith("llm:"):
        return "tier3_llm"
    return "unknown"


@traceable(name="confidence_scorer", metadata={"agent": "confidence_scorer"})
def confidence_scorer_node(state: DataCleaningState) -> dict[str, Any]:
    """LangGraph node: score confidence and route low-confidence fixes."""
    cleaning_actions = state.get("cleaning_actions", [])
    threshold = settings.confidence_threshold

    if not cleaning_actions:
        return {
            "low_confidence_fixes": [],
            "audit_log": [],
            "final_report": _build_report(state, [], [], 0.0),
        }

    low_confidence: list[dict[str, Any]] = []
    audit_entries: list[dict[str, Any]] = []
    confidence_sum = 0.0

    for fix in cleaning_actions:
        confidence = fix.get("confidence", 0.0)
        tier = _categorize_fix(fix)
        confidence_sum += confidence

        audit_entry = {
            "row_index": fix.get("row", -1),
            "column_name": fix.get("column", ""),
            "original_value": fix.get("old_value"),
            "new_value": fix.get("new_value"),
            "issue_type": fix.get("issue_type", "unknown"),
            "fix_method": fix.get("rule", "unknown"),
            "confidence": confidence,
            "reasoning": fix.get("reasoning", ""),
            "agent_name": "cleaner",
            "tier": tier,
        }
        audit_entries.append(audit_entry)

        if confidence < threshold:
            low_confidence.append(fix)

    overall_confidence = confidence_sum / len(cleaning_actions) if cleaning_actions else 0.0

    # Breakdown by tier
    tier_counts = {"tier1_deterministic": 0, "tier2_pattern": 0, "tier3_llm": 0}
    for fix in cleaning_actions:
        tier = _categorize_fix(fix)
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    logger.info(
        "Confidence scoring: %d fixes, avg confidence=%.3f, %d below threshold (%.2f). "
        "Tiers: T1=%d, T2=%d, T3=%d",
        len(cleaning_actions),
        overall_confidence,
        len(low_confidence),
        threshold,
        tier_counts["tier1_deterministic"],
        tier_counts["tier2_pattern"],
        tier_counts["tier3_llm"],
    )

    report = _build_report(state, cleaning_actions, low_confidence, overall_confidence)

    return {
        "low_confidence_fixes": low_confidence,
        "audit_log": audit_entries,
        "final_report": report,
    }


def _build_report(
    state: DataCleaningState,
    actions: list[dict[str, Any]],
    low_confidence: list[dict[str, Any]],
    overall_confidence: float,
) -> dict[str, Any]:
    """Build the final quality report."""
    records = state.get("raw_records", [])
    cleaned = state.get("cleaned_records", [])

    issue_breakdown: dict[str, int] = {}
    for a in actions:
        itype = a.get("issue_type", "unknown")
        issue_breakdown[itype] = issue_breakdown.get(itype, 0) + 1

    return {
        "dataset_id": state.get("dataset_id", "unknown"),
        "total_rows": len(records),
        "total_columns": len(records[0]) if records else 0,
        "issues_detected": len(state.get("anomalies", [])),
        "issues_fixed": len(actions),
        "issues_skipped": len(low_confidence),
        "overall_confidence": round(overall_confidence, 4),
        "data_quality_score_before": state.get("data_quality_score", 0.0),
        "fix_breakdown": issue_breakdown,
        "iterations": state.get("iteration_count", 0),
        "validation_passed": state.get("validation_passed", False),
    }
