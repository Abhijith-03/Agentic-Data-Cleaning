"""Cleaning/Repair Agent — hybrid rules + LLM engine.

Strategy:
1. Try deterministic rules engine (fast, high confidence)
2. Try pattern store (learned from previous LLM fixes)
3. Fall back to LLM inference (slow, variable confidence)
"""

from __future__ import annotations

import copy
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langsmith import traceable
from pydantic import BaseModel, Field

from src.config import settings
from src.graph.state import DataCleaningState
from src.knowledge.pattern_store import PatternStore
from src.knowledge.rules_engine import (
    CleaningAction,
    apply_format_rules,
    impute_categorical_mode,
    impute_numeric_median,
    impute_unknown,
)

logger = logging.getLogger(__name__)


class LLMFixResponse(BaseModel):
    """Structured response from the LLM for a single fix."""
    corrected_value: str
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str


_CLEANER_SYSTEM_PROMPT = """\
You are an expert data cleaning agent. You are given a data quality issue in a tabular dataset.

Your task is to suggest the corrected value for the cell. Be conservative: only change what is clearly wrong.

Respond with:
- corrected_value: the fixed value (string)
- confidence: your confidence the fix is correct (0.0 to 1.0)
- reasoning: brief explanation of why this fix is appropriate

Context about the column will be provided including the column name, detected type, sample values, and the specific issue."""


def _get_llm() -> ChatOpenAI:
    return ChatOpenAI(
        model=settings.primary_model,
        temperature=settings.llm_temperature,
        max_retries=settings.llm_max_retries,
    )


def _build_llm_context(
    issue: dict[str, Any],
    records: list[dict[str, Any]],
    inferred_schema: dict[str, Any],
) -> str:
    col = issue["column"]
    row_idx = issue["row"]
    value = issue.get("value", "")
    schema_info = inferred_schema.get(col, {})

    sample_values = list(set(str(r.get(col, "")) for r in records[:50] if r.get(col)))[:15]

    context = (
        f"Column: {col}\n"
        f"Detected type: {schema_info.get('dtype', 'unknown')}\n"
        f"Format pattern: {schema_info.get('format_pattern', 'none')}\n"
        f"Current value: '{value}'\n"
        f"Issue type: {issue.get('anomaly_type', 'unknown')}\n"
        f"Issue details: {issue.get('details', '')}\n"
        f"Sample values from this column: {sample_values}\n"
        f"Row index: {row_idx}\n"
    )

    if row_idx < len(records):
        row_data = {k: v for k, v in records[row_idx].items() if k != col}
        context += f"Other columns in this row: {row_data}\n"

    return context


def _try_rules_engine(
    issue: dict[str, Any],
    records: list[dict[str, Any]],
    inferred_schema: dict[str, Any],
) -> CleaningAction | None:
    """Attempt deterministic rule-based fix."""
    col = issue["column"]
    row = issue["row"]
    value = str(issue.get("value", ""))
    anomaly_type = issue.get("anomaly_type", "")
    col_values = [str(r.get(col, "")) for r in records]

    # Missing value imputation
    if value.strip() == "" or value == "None" or anomaly_type == "missing":
        schema_info = inferred_schema.get(col, {})
        dtype = schema_info.get("dtype", "string")

        if dtype in ("integer", "float"):
            result = impute_numeric_median(col_values, value, col)
            if result:
                return result
        result = impute_categorical_mode(col_values, value, col)
        if result:
            return result
        return impute_unknown(value, col)

    # Format normalisation
    format_fix = apply_format_rules(value, col)
    if format_fix:
        return format_fix

    return None


def _try_pattern_store(
    issue: dict[str, Any],
    pattern_store: PatternStore,
) -> tuple[CleaningAction | None, int | None]:
    """Try the learned pattern store."""
    col = issue["column"]
    value = str(issue.get("value", ""))

    pattern = pattern_store.find_match(col, value)
    if pattern is None:
        return None, None

    try:
        new_value = pattern_store.apply_template(pattern, value)
        return CleaningAction(
            new_value=new_value,
            rule_name=f"pattern:{pattern.column_pattern}",
            confidence=pattern.confidence,
            reasoning=f"Applied learned pattern #{pattern.id} (success rate: {pattern.confidence:.0%})",
        ), pattern.id
    except Exception as e:
        logger.warning("Pattern application failed for pattern #%s: %s", pattern.id, e)
        return None, None


@traceable(name="llm_repair", metadata={"agent": "cleaner"})
def _llm_repair(
    issue: dict[str, Any],
    records: list[dict[str, Any]],
    inferred_schema: dict[str, Any],
) -> CleaningAction | None:
    """Fall back to LLM for complex repairs."""
    col = issue["column"]
    row_idx = issue["row"]
    context = _build_llm_context(issue, records, inferred_schema)

    try:
        llm = _get_llm()
        structured_llm = llm.with_structured_output(LLMFixResponse)
        response: LLMFixResponse = structured_llm.invoke([
            SystemMessage(content=_CLEANER_SYSTEM_PROMPT),
            HumanMessage(content=context),
        ])

        return CleaningAction(
            new_value=response.corrected_value,
            rule_name=f"llm:{settings.primary_model}",
            confidence=response.confidence,
            reasoning=response.reasoning,
            metadata={
                "llm_log": {
                    "row": row_idx,
                    "column": col,
                    "model": settings.primary_model,
                    "prompt": context,
                    "structured_output": response.model_dump(),
                }
            },
        )
    except Exception as e:
        logger.error("LLM repair failed for row %d, col %s: %s", row_idx, col, e)
        return None


@traceable(name="cleaner", metadata={"agent": "cleaner"})
def cleaner_node(state: DataCleaningState) -> dict[str, Any]:
    """LangGraph node: repair all detected issues."""
    iteration = state.get("iteration_count", 0)
    # On first pass use raw_records; on re-iterations use the previously cleaned data
    if iteration > 0 and state.get("cleaned_records"):
        records = state.get("cleaned_records", [])
    else:
        records = state.get("raw_records", [])
    anomalies = state.get("anomalies", [])
    inferred_schema = state.get("inferred_schema", {})
    existing_actions = state.get("cleaning_actions", [])
    existing_llm_logs = state.get("llm_logs", [])

    # Also fix validation errors from previous iteration
    validation_errors = state.get("validation_errors", [])

    if not records:
        return {
            "cleaning_actions": existing_actions,
            "cleaned_records": records,
            "llm_logs": existing_llm_logs,
            "iteration_count": iteration + 1,
        }

    cleaned = copy.deepcopy(records)
    actions: list[dict[str, Any]] = list(existing_actions)
    llm_logs: list[dict[str, Any]] = list(existing_llm_logs)

    # Build null-cell issues from records
    null_issues: list[dict[str, Any]] = []
    for row_idx, row in enumerate(records):
        for col, val in row.items():
            if val is None or (isinstance(val, str) and val.strip() == ""):
                null_issues.append({
                    "row": row_idx,
                    "column": col,
                    "value": val,
                    "anomaly_type": "missing",
                    "severity": "warning",
                })

    all_issues = anomalies + null_issues + validation_errors

    # Deduplicate by (row, column)
    seen: set[tuple[int, str]] = set()
    unique_issues: list[dict[str, Any]] = []
    for issue in all_issues:
        key = (issue.get("row", -1), issue.get("column", ""))
        if key not in seen:
            seen.add(key)
            unique_issues.append(issue)

    pattern_store = PatternStore()

    for issue in unique_issues:
        row_idx = issue.get("row", -1)
        col = issue.get("column", "")
        if row_idx < 0 or row_idx >= len(cleaned) or not col:
            continue
        if col not in cleaned[row_idx]:
            continue

        # Tier 1: Rules engine
        action = _try_rules_engine(issue, records, inferred_schema)

        # Tier 2: Pattern store
        pattern_id = None
        if action is None:
            action, pattern_id = _try_pattern_store(issue, pattern_store)

        # Tier 3: LLM fallback
        if action is None:
            action = _llm_repair(issue, records, inferred_schema)

        if action is not None:
            old_value = cleaned[row_idx][col]
            cleaned[row_idx][col] = action.new_value

            actions.append({
                "row": row_idx,
                "column": col,
                "old_value": old_value,
                "new_value": action.new_value,
                "rule": action.rule_name,
                "confidence": action.confidence,
                "reasoning": action.reasoning,
                "issue_type": issue.get("anomaly_type", "unknown"),
                "metadata": action.metadata,
            })

            llm_log = action.metadata.get("llm_log")
            if llm_log:
                llm_logs.append({
                    **llm_log,
                    "old_value": old_value,
                    "new_value": action.new_value,
                    "confidence": action.confidence,
                    "reasoning": action.reasoning,
                    "issue_type": issue.get("anomaly_type", "unknown"),
                })

            # Auto-learn: store LLM fixes as patterns for future reuse
            if action.rule_name.startswith("llm:") and action.confidence >= 0.7:
                try:
                    import re as _re
                    from src.knowledge.pattern_store import LearnedPattern

                    pattern_store.add_pattern(LearnedPattern(
                        column_pattern=f".*{_re.escape(col)}.*",
                        value_pattern=_re.escape(str(old_value)) if old_value else "^$",
                        fix_template=str(action.new_value),
                        domain="generic",
                        success_count=1,
                        fail_count=0,
                    ))
                except Exception:
                    pass

    pattern_store.close()

    logger.info(
        "Cleaning iteration %d complete: %d fixes applied",
        iteration + 1,
        len(actions) - len(existing_actions),
    )

    return {
        "cleaning_actions": actions,
        "cleaned_records": cleaned,
        "llm_logs": llm_logs,
        "iteration_count": iteration + 1,
    }


