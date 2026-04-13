"""Deterministic preprocessing for semi-structured rows."""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd

from src.agents.reconstruction_spec import ReconstructionSpec, coerce_reconstruction_spec

logger = logging.getLogger(__name__)

MIN_DELIMITER_TRIGGER = 4

_RE_ID = re.compile(r"^\d{2,12}$")
_RE_AGE = re.compile(r"^\d{1,3}\*?$")
_RE_GENDER = re.compile(r"^[MFmf]$")
_RE_DATE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}|\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}|NA|N/?A)$",
    re.IGNORECASE,
)
_RE_NUMBER = re.compile(r"^-?\d+(?:\.\d+)?$")
_RE_NAME_TOKEN = re.compile(r"^[A-Za-z][A-Za-z'\-]{0,40}$")


class RowKind(str, Enum):
    FULL_RECORD = "FULL_RECORD"
    PARTIAL_RECORD = "PARTIAL_RECORD"
    NOISE = "NOISE"
    HEADER = "HEADER"


@dataclass
class ReconstructionResult:
    clean_table: pd.DataFrame
    reconstruction_report: dict[str, Any]
    row_confidences: list[float] = field(default_factory=list)


def _split_line(text: str, spec: ReconstructionSpec) -> list[str]:
    return [part.strip() for part in text.split(spec.delimiter)]


def _normalize_text(text: str) -> str:
    return text.strip().lower().replace("_", " ").replace("-", " ")


def _normalize_currency(value: str) -> str:
    cleaned = str(value).strip()
    cleaned = re.sub(r"^[^\d\-]+", "", cleaned)
    cleaned = re.sub(r"[,\s]", "", cleaned)
    return "" if cleaned.upper() in {"NA", "N/A"} else cleaned


def _normalize_numeric(value: str) -> str:
    return str(value).strip().rstrip("*")


def _looks_like_header(parts: list[str], spec: ReconstructionSpec) -> bool:
    if len(parts) < 2:
        return False

    normalized_parts = {_normalize_text(part) for part in parts if part.strip()}
    normalized_columns = {_normalize_text(column) for column in spec.target_columns}
    return len(normalized_parts & normalized_columns) >= max(2, len(spec.target_columns) // 3)


def _extract_primary_line(record: dict[str, Any], spec: ReconstructionSpec) -> str:
    texts: list[str] = []
    for value in record.values():
        if value is None:
            continue
        string_value = str(value).strip()
        if string_value:
            texts.append(string_value)
    if not texts:
        return ""
    return max(texts, key=lambda value: value.count(spec.delimiter))


def _full_record_threshold(spec: ReconstructionSpec) -> int:
    return max(3, min(spec.field_count, spec.field_count - 1 if spec.field_count > 4 else spec.field_count))


def classify_row(text: str, spec: ReconstructionSpec) -> tuple[RowKind, list[str]]:
    """Classify a row into FULL_RECORD, PARTIAL_RECORD, HEADER, or NOISE."""
    stripped = text.strip()
    if not stripped:
        return RowKind.NOISE, []

    delimiter_count = stripped.count(spec.delimiter)
    parts = _split_line(stripped, spec)

    if _looks_like_header(parts, spec):
        return RowKind.HEADER, parts

    full_threshold = _full_record_threshold(spec)
    if delimiter_count >= max(2, full_threshold - 1) and len(parts) >= full_threshold:
        return RowKind.FULL_RECORD, parts
    if delimiter_count >= 2 and parts and parts[0].isdigit() and len(parts) >= max(3, full_threshold - 1):
        return RowKind.FULL_RECORD, parts

    if delimiter_count <= 1 and len(parts) <= 2:
        tokens = parts if delimiter_count else stripped.split()
        tokens = [token.strip() for token in tokens if token.strip()]
        if 1 <= len(tokens) <= 2:
            return RowKind.PARTIAL_RECORD, tokens

    if delimiter_count >= 1 and len(parts) < full_threshold:
        return RowKind.PARTIAL_RECORD, parts

    return RowKind.NOISE, parts


def map_parts_to_record(parts: list[str], spec: ReconstructionSpec) -> dict[str, str]:
    record = {column: "" for column in spec.target_columns}
    for index, column in enumerate(spec.target_columns):
        if index < len(parts):
            record[column] = str(parts[index]).strip()
    return record


def validate_and_score_row(
    record: dict[str, str],
    spec: ReconstructionSpec,
) -> tuple[dict[str, str], float]:
    normalized = dict(record)
    score = 1.0

    for column, semantic in zip(spec.target_columns, spec.column_semantics):
        semantic = semantic.lower()
        value = str(normalized.get(column, "") or "").strip()

        if semantic == "id":
            if value and not _RE_ID.match(value):
                score -= 0.15
            elif not value:
                score -= 0.1
        elif semantic == "age":
            value = _normalize_numeric(value)
            normalized[column] = value
            if value and not _RE_AGE.match(value):
                score -= 0.12
            elif not value:
                score -= 0.05
        elif semantic == "gender":
            if value and _RE_GENDER.match(value):
                normalized[column] = value.upper()
            elif value:
                score -= 0.1
            else:
                score -= 0.05
        elif semantic == "date":
            if value.upper() in {"NA", "N/A", ""}:
                normalized[column] = ""
            elif not _RE_DATE.match(value):
                score -= 0.1
        elif semantic == "currency":
            value = _normalize_currency(value)
            normalized[column] = value
            if value:
                try:
                    float(value)
                except ValueError:
                    score -= 0.12
            else:
                score -= 0.05
        elif semantic == "numeric":
            value = _normalize_numeric(value)
            normalized[column] = value
            if value and not _RE_NUMBER.match(value):
                score -= 0.12
        elif semantic in {"name", "first_name", "last_name"}:
            if value and not all(_RE_NAME_TOKEN.match(token) for token in value.split()):
                score -= 0.08

    primary_name_col = spec.first_column_for_any(("name", "first_name"))
    if primary_name_col and not str(normalized.get(primary_name_col, "")).strip():
        score -= 0.08

    return normalized, max(0.0, min(1.0, score))


def _row_hash(record: dict[str, str], spec: ReconstructionSpec) -> str:
    payload = "|".join(str(record.get(column, "") or "").strip().lower() for column in spec.target_columns)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def needs_structure_reconstruction(
    records: list[dict[str, Any]],
    spec: ReconstructionSpec | None,
) -> bool:
    if not records:
        return False
    if spec is None:
        return False

    keys_lower = {str(key).lower() for key in records[0].keys()}
    has_target_shape = len(keys_lower & spec.target_set_lower()) >= min(6, len(spec.target_columns))

    max_delimiters = 0
    any_blob = False
    for record in records:
        for value in record.values():
            if value is None:
                continue
            count = str(value).count(spec.delimiter)
            max_delimiters = max(max_delimiters, count)
            if count >= MIN_DELIMITER_TRIGGER:
                any_blob = True

    if any_blob:
        return True
    if has_target_shape and max_delimiters == 0:
        return False
    return max_delimiters >= 2


def _seed_partial_record(parts: list[str], spec: ReconstructionSpec) -> dict[str, str]:
    record = {column: "" for column in spec.target_columns}
    id_column = spec.first_column_for("id")
    name_column = spec.first_column_for("name")
    first_name_column = spec.first_column_for("first_name")
    last_name_column = spec.first_column_for("last_name")

    if not parts:
        return record

    first = parts[0]
    if id_column and first.isdigit():
        record[id_column] = first
    elif name_column and first:
        record[name_column] = first
    elif first_name_column and first:
        record[first_name_column] = first

    if len(parts) > 1:
        second = parts[1]
        if last_name_column and second:
            record[last_name_column] = second
        elif name_column and not record.get(name_column):
            record[name_column] = second

    return record


def _merge_partial(tokens: list[str], previous: dict[str, str], spec: ReconstructionSpec) -> dict[str, str] | None:
    merged = dict(previous)
    name_column = spec.first_column_for("name")
    first_name_column = spec.first_column_for("first_name")
    last_name_column = spec.first_column_for("last_name")
    text_column = spec.first_column_for_any(("text", "categorical"))

    if len(tokens) == 1:
        token = tokens[0]
        if name_column and not merged.get(name_column):
            merged[name_column] = token
            return merged
        if first_name_column and not merged.get(first_name_column):
            merged[first_name_column] = token
            return merged
        if last_name_column and not merged.get(last_name_column):
            merged[last_name_column] = token
            return merged
        if text_column and not merged.get(text_column) and len(token) > 2:
            merged[text_column] = token
            return merged
        return None

    if len(tokens) == 2:
        first, second = tokens
        if name_column and not merged.get(name_column):
            merged[name_column] = f"{first} {second}".strip()
            return merged
        if first_name_column and last_name_column:
            if not merged.get(first_name_column):
                merged[first_name_column] = first
            if not merged.get(last_name_column):
                merged[last_name_column] = second
            return merged
        return None

    return None


def reconstruct_structure(
    records: list[dict[str, Any]],
    spec: ReconstructionSpec,
) -> ReconstructionResult:
    rows_parsed = 0
    rows_dropped = 0
    duplicates_removed = 0
    built_records: list[dict[str, str]] = []
    row_confidences: list[float] = []
    pending_record: dict[str, str] | None = None

    for record in records:
        line = _extract_primary_line(record, spec)
        if not line:
            rows_dropped += 1
            continue

        kind, parts = classify_row(line, spec)

        if kind in {RowKind.HEADER, RowKind.NOISE}:
            rows_dropped += 1
            continue

        if kind == RowKind.FULL_RECORD:
            mapped = map_parts_to_record(parts, spec)
            validated, confidence = validate_and_score_row(mapped, spec)
            built_records.append(validated)
            row_confidences.append(confidence)
            rows_parsed += 1
            pending_record = None
            continue

        merged = None
        merge_into_previous = False

        if pending_record is not None:
            merged = _merge_partial(parts, pending_record, spec)

        if merged is None and built_records:
            merged = _merge_partial(parts, built_records[-1], spec)
            merge_into_previous = merged is not None

        if merged is not None:
            validated, confidence = validate_and_score_row(merged, spec)
            if merge_into_previous:
                built_records[-1] = validated
                row_confidences[-1] = round((row_confidences[-1] + confidence) / 2, 4)
            else:
                built_records.append(validated)
                row_confidences.append(confidence)
                rows_parsed += 1
            pending_record = None
            continue

        pending_record = _seed_partial_record(parts, spec)
        rows_dropped += 1

    seen_hashes: set[str] = set()
    deduped_records: list[dict[str, str]] = []
    deduped_confidences: list[float] = []

    for record, confidence in zip(built_records, row_confidences):
        record_hash = _row_hash(record, spec)
        if record_hash in seen_hashes:
            duplicates_removed += 1
            continue
        seen_hashes.add(record_hash)
        deduped_records.append(record)
        deduped_confidences.append(confidence)

    clean_table = pd.DataFrame(deduped_records, columns=list(spec.target_columns))
    report = {
        "rows_parsed": rows_parsed,
        "rows_dropped": rows_dropped,
        "duplicates_removed": duplicates_removed,
        "output_rows": len(clean_table),
    }
    return ReconstructionResult(
        clean_table=clean_table,
        reconstruction_report=report,
        row_confidences=deduped_confidences,
    )


def structure_reconstruction_node(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node for deterministic row reconstruction."""

    records: list[dict[str, Any]] = state.get("raw_records") or []
    spec = coerce_reconstruction_spec(state.get("reconstruction_spec"))

    if not records:
        return {
            "reconstruction_report": {
                "rows_parsed": 0,
                "rows_dropped": 0,
                "duplicates_removed": 0,
                "output_rows": 0,
                "skipped": True,
            },
            "reconstruction_row_confidences": [],
        }

    if spec is None:
        logger.info("Structure reconstruction skipped because no reconstruction spec is available")
        return {
            "reconstruction_report": {
                "rows_parsed": 0,
                "rows_dropped": 0,
                "duplicates_removed": 0,
                "output_rows": len(records),
                "skipped": True,
                "reason": "no_reconstruction_spec",
            },
            "reconstruction_row_confidences": [],
        }

    if not needs_structure_reconstruction(records, spec):
        logger.info("Structure reconstruction skipped because input already appears tabular")
        return {
            "reconstruction_report": {
                "rows_parsed": len(records),
                "rows_dropped": 0,
                "duplicates_removed": 0,
                "output_rows": len(records),
                "skipped": True,
            },
            "reconstruction_row_confidences": [],
        }

    result = reconstruct_structure(records, spec)
    logger.info(
        "Structure reconstruction complete: parsed=%d dropped=%d duplicates=%d output=%d",
        result.reconstruction_report["rows_parsed"],
        result.reconstruction_report["rows_dropped"],
        result.reconstruction_report["duplicates_removed"],
        result.reconstruction_report["output_rows"],
    )
    return {
        "raw_records": result.clean_table.where(result.clean_table.notna(), None).to_dict(orient="records"),
        "reconstruction_report": {**result.reconstruction_report, "skipped": False},
        "reconstruction_row_confidences": result.row_confidences,
    }
