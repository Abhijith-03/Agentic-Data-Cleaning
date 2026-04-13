"""Schema contract for structure reconstruction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SEMANTIC_TAGS = frozenset(
    {
        "id",
        "name",
        "first_name",
        "last_name",
        "age",
        "gender",
        "date",
        "currency",
        "numeric",
        "categorical",
        "text",
    }
)


@dataclass(frozen=True)
class ReconstructionSpec:
    """Ordered schema used by deterministic structure reconstruction."""

    target_columns: tuple[str, ...]
    column_semantics: tuple[str, ...]
    delimiter: str = "|"
    expected_field_count: int | None = None

    def __post_init__(self) -> None:
        if len(self.target_columns) != len(self.column_semantics):
            raise ValueError("target_columns and column_semantics must have the same length")
        if len(set(self.target_columns)) != len(self.target_columns):
            raise ValueError("target_columns must be unique")
        for semantic in self.column_semantics:
            if semantic.lower() not in SEMANTIC_TAGS:
                raise ValueError(f"Unknown semantic tag: {semantic!r}")
        if not self.delimiter:
            raise ValueError("delimiter must be a non-empty string")

    def target_set_lower(self) -> set[str]:
        return {column.lower() for column in self.target_columns}

    @property
    def field_count(self) -> int:
        return self.expected_field_count or len(self.target_columns)

    def first_column_for(self, semantic: str) -> str | None:
        semantic = semantic.lower()
        for column, tag in zip(self.target_columns, self.column_semantics):
            if tag.lower() == semantic:
                return column
        return None

    def first_column_for_any(self, semantics: tuple[str, ...]) -> str | None:
        for semantic in semantics:
            column = self.first_column_for(semantic)
            if column:
                return column
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_columns": list(self.target_columns),
            "column_semantics": list(self.column_semantics),
            "delimiter": self.delimiter,
            "expected_field_count": self.expected_field_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReconstructionSpec":
        return cls(
            target_columns=tuple(str(column) for column in data["target_columns"]),
            column_semantics=tuple(str(tag).lower() for tag in data["column_semantics"]),
            delimiter=str(data.get("delimiter", "|")),
            expected_field_count=data.get("expected_field_count"),
        )


def coerce_reconstruction_spec(value: Any | None) -> ReconstructionSpec | None:
    if isinstance(value, ReconstructionSpec):
        return value
    if isinstance(value, dict) and "target_columns" in value and "column_semantics" in value:
        return ReconstructionSpec.from_dict(value)
    return None
