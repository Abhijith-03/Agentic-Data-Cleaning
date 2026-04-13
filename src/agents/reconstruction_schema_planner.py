"""Optional LLM-assisted planner for structure reconstruction."""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, field_validator

from src.agents.reconstruction_spec import (
    ReconstructionSpec,
    SEMANTIC_TAGS,
    coerce_reconstruction_spec,
)
from src.config import settings
from src.graph.state import DataCleaningState

logger = logging.getLogger(__name__)

_MAX_SAMPLE_ROWS = 20
_MAX_SAMPLE_CHARS = 12_000


class ReconstructionPlannerResponse(BaseModel):
    """Structured response from the LLM schema planner."""

    target_columns: list[str] = Field(
        min_length=2,
        description="Final output columns in left-to-right order of the delimited row.",
    )
    column_semantics: list[str] = Field(
        description=(
            "One semantic tag per target column. Allowed values: "
            "id, name, first_name, last_name, age, gender, date, currency, "
            "numeric, categorical, text."
        ),
    )
    delimiter: str = Field(default="|", description="Most likely delimiter in full records.")
    expected_field_count: int | None = Field(
        default=None,
        description="Expected number of fields in a full record.",
    )
    rationale: str = ""

    @field_validator("column_semantics", mode="before")
    @classmethod
    def _normalize_semantics(cls, value: list[str]) -> list[str]:
        return [str(item).lower().strip() for item in value]

_PLANNER_SYSTEM_PROMPT = """You infer the logical schema for semi-structured tabular data.

The input can include:
- rows squeezed into a single cell with delimiters like '|'
- repeated header rows
- partial rows or fragments
- mixed formatting across the same spreadsheet region

Your job is NOT to clean rows. Your job is only to infer the ordered output
columns and assign one semantic tag to each column.

Use only these semantic tags:
- id
- name
- first_name
- last_name
- age
- gender
- date
- currency
- numeric
- categorical
- text

Return concise, stable column names that fit the domain shown in the sample.
If the sample clearly represents student records, student-oriented names are fine.
"""


def _get_llm() -> ChatOpenAI:
    return ChatOpenAI(
        model=settings.fast_model,
        temperature=0.0,
        max_retries=settings.llm_max_retries,
    )


def _build_sample(records: list[dict[str, Any]]) -> str:
    sample = records[:_MAX_SAMPLE_ROWS]
    try:
        payload = json.dumps(sample, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        payload = str(sample)
    if len(payload) > _MAX_SAMPLE_CHARS:
        payload = payload[:_MAX_SAMPLE_CHARS] + "\n... (truncated)"
    return payload


def _infer_spec_with_llm(records: list[dict[str, Any]]) -> ReconstructionSpec | None:
    if not settings.openai_api_key:
        return None

    llm = _get_llm().with_structured_output(ReconstructionPlannerResponse)
    prompt = (
        "Infer the target schema for structure reconstruction from this sample "
        "of raw records:\n\n"
        f"{_build_sample(records)}"
    )

    try:
        response: ReconstructionPlannerResponse = llm.invoke(
            [
                SystemMessage(content=_PLANNER_SYSTEM_PROMPT),
                HumanMessage(content=prompt),
            ]
        )
        if len(response.target_columns) != len(response.column_semantics):
            raise ValueError("target_columns and column_semantics must have the same length")
        for semantic in response.column_semantics:
            if semantic not in SEMANTIC_TAGS:
                raise ValueError(f"Invalid semantic tag: {semantic!r}")
        return ReconstructionSpec(
            target_columns=tuple(response.target_columns),
            column_semantics=tuple(response.column_semantics),
            delimiter=response.delimiter or "|",
            expected_field_count=response.expected_field_count,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Reconstruction schema planning failed: %s", exc)
        return None


def reconstruction_schema_planner_node(state: DataCleaningState) -> dict[str, Any]:
    """Populate `reconstruction_spec` for downstream deterministic parsing."""

    existing = state.get("reconstruction_spec")
    if existing:
        spec = coerce_reconstruction_spec(existing)
        if spec is not None:
            return {"reconstruction_spec": spec.to_dict()}

    records = state.get("raw_records", [])
    if records and settings.reconstruction_schema_llm_enabled and settings.openai_api_key:
        inferred = _infer_spec_with_llm(records)
        if inferred is not None:
            logger.info(
                "Reconstruction schema planned: delimiter=%r fields=%d columns=%s",
                inferred.delimiter,
                inferred.field_count,
                list(inferred.target_columns),
            )
            return {"reconstruction_spec": inferred.to_dict()}

    logger.info("No reconstruction spec available; planner returned no schema")
    return {}
