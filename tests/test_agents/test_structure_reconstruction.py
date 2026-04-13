"""Tests for reconstruction schema planning and deterministic row reconstruction."""

from src.config import settings
from src.agents.reconstruction_schema_planner import reconstruction_schema_planner_node
from src.agents.reconstruction_spec import ReconstructionSpec
from src.agents.structure_reconstruction import (
    RowKind,
    classify_row,
    needs_structure_reconstruction,
    reconstruct_structure,
    structure_reconstruction_node,
)

STUDENT_SPEC = ReconstructionSpec(
    target_columns=(
        "Student_ID",
        "First_Name",
        "Last_Name",
        "Age",
        "Gender",
        "Course",
        "Enrollment_Date",
        "Total_Payments",
    ),
    column_semantics=(
        "id",
        "first_name",
        "last_name",
        "age",
        "gender",
        "text",
        "date",
        "currency",
    ),
    delimiter="|",
    expected_field_count=8,
)


class TestClassifyRow:
    def test_detects_full_record(self):
        line = "101 | John | Smith | 22 | M | Data Science | 2022-05-15 | $1200"
        kind, parts = classify_row(line, STUDENT_SPEC)
        assert kind == RowKind.FULL_RECORD
        assert parts[0] == "101"

    def test_detects_header(self):
        line = "Student ID | First Name | Last Name | Age | Gender | Course | Enrollment Date | Total Payments"
        kind, _ = classify_row(line, STUDENT_SPEC)
        assert kind == RowKind.HEADER

    def test_detects_partial_record(self):
        kind, parts = classify_row("Davis", STUDENT_SPEC)
        assert kind == RowKind.PARTIAL_RECORD
        assert parts == ["Davis"]


class TestReconstructStructure:
    def test_parses_student_example(self):
        records = [
            {"blob": "101 | John | Smith | 22 | M | Data Science | 2022-05-15 | $1200"},
        ]
        result = reconstruct_structure(records, STUDENT_SPEC)
        assert len(result.clean_table) == 1
        row = result.clean_table.iloc[0]
        assert row["Student_ID"] == "101"
        assert row["First_Name"] == "John"
        assert row["Last_Name"] == "Smith"
        assert row["Age"] == "22"
        assert row["Gender"] == "M"
        assert row["Enrollment_Date"] == "2022-05-15"
        assert row["Total_Payments"] == "1200"

    def test_merges_partial_into_previous_full_record(self):
        records = [
            {"blob": "104 | Mary |  | 25 | F | Bio | 2019-06-01 | $200"},
            {"blob": "Jones"},
        ]
        result = reconstruct_structure(records, STUDENT_SPEC)
        assert len(result.clean_table) == 1
        assert result.clean_table.iloc[0]["Last_Name"] == "Jones"

    def test_removes_duplicates(self):
        line = "103 | A | B | 21 | M | X | 2020-01-01 | $100"
        result = reconstruct_structure([{"a": line}, {"a": line}], STUDENT_SPEC)
        assert len(result.clean_table) == 1
        assert result.reconstruction_report["duplicates_removed"] == 1

    def test_supports_dynamic_schema(self):
        spec = ReconstructionSpec(
            target_columns=("Employee_ID", "Employee_Name", "Department", "Join_Date", "Salary"),
            column_semantics=("id", "name", "text", "date", "currency"),
            delimiter="|",
            expected_field_count=5,
        )
        records = [
            {"raw": "501 | Anita Davis | Analytics | 2023-01-05 | $2300"},
            {"raw": "502 | Joseph Murphy | Web Development | 2023-06-05 | $1050"},
        ]
        result = reconstruct_structure(records, spec)
        assert list(result.clean_table.columns) == list(spec.target_columns)
        assert result.clean_table.iloc[0]["Employee_Name"] == "Anita Davis"
        assert result.clean_table.iloc[1]["Salary"] == "1050"


class TestPlannerAndNodeIntegration:
    def test_planner_uses_existing_spec(self):
        spec = ReconstructionSpec(
            target_columns=("Order_ID", "Customer_Name", "Amount"),
            column_semantics=("id", "name", "currency"),
            expected_field_count=3,
        )
        out = reconstruction_schema_planner_node(
            {"reconstruction_spec": spec.to_dict(), "raw_records": [{"x": "1|Jane|$20"}]}
        )
        assert out["reconstruction_spec"]["target_columns"] == ["Order_ID", "Customer_Name", "Amount"]

    def test_planner_returns_nothing_without_llm_or_existing_spec(self, monkeypatch):
        monkeypatch.setattr(settings, "reconstruction_schema_llm_enabled", False)
        monkeypatch.setattr(settings, "openai_api_key", "")
        out = reconstruction_schema_planner_node(
            {"raw_records": [{"blob": "101 | John | Smith | 22 | M | Data Science | 2022-05-15 | $1200"}]}
        )
        assert out == {}

    def test_needs_reconstruction_skips_already_tabular(self):
        records = [
            {
                "Student_ID": "1",
                "First_Name": "Ana",
                "Last_Name": "West",
                "Age": "24",
                "Gender": "F",
                "Course": "Math",
                "Enrollment_Date": "2020-01-01",
                "Total_Payments": "100",
            }
        ]
        assert needs_structure_reconstruction(records, STUDENT_SPEC) is False

    def test_node_skips_when_no_spec_exists(self):
        out = structure_reconstruction_node(
            {"raw_records": [{"blob": "201 | X | Y | 30 | F | CS | 2022-01-01 | $99"}]}
        )
        assert out["reconstruction_report"]["skipped"] is True
        assert out["reconstruction_report"]["reason"] == "no_reconstruction_spec"

    def test_node_replaces_raw_records_for_messy_data(self):
        state = {
            "reconstruction_spec": STUDENT_SPEC.to_dict(),
            "raw_records": [{"blob": "201 | X | Y | 30 | F | CS | 2022-01-01 | $99"}],
        }
        out = structure_reconstruction_node(state)
        assert out["reconstruction_report"]["skipped"] is False
        assert out["raw_records"][0]["Student_ID"] == "201"
        assert out["reconstruction_row_confidences"]

    def test_node_skips_when_already_tabular(self):
        state = {
            "reconstruction_spec": STUDENT_SPEC.to_dict(),
            "raw_records": [
                {
                    "Student_ID": "1",
                    "First_Name": "Ana",
                    "Last_Name": "West",
                    "Age": "24",
                    "Gender": "F",
                    "Course": "Math",
                    "Enrollment_Date": "2020-01-01",
                    "Total_Payments": "100",
                }
            ],
        }
        out = structure_reconstruction_node(state)
        assert out["reconstruction_report"]["skipped"] is True
        assert "raw_records" not in out
