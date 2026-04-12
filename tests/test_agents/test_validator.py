"""Tests for the Validation Agent."""

from src.agents.validator import validator_node


class TestValidator:
    def test_passes_clean_data(self):
        records = [{"name": "Alice", "age": "25"} for _ in range(20)]
        state = {
            "cleaned_records": records,
            "raw_records": records,
            "inferred_schema": {
                "name": {"dtype": "string"},
                "age": {"dtype": "integer"},
            },
        }
        result = validator_node(state)
        assert result["validation_passed"] is True

    def test_detects_type_mismatch(self):
        records = [{"age": "25"} for _ in range(10)]
        records.append({"age": "not_a_number"})
        state = {
            "cleaned_records": records,
            "raw_records": records,
            "inferred_schema": {"age": {"dtype": "integer"}},
        }
        result = validator_node(state)
        type_errors = [e for e in result["validation_errors"] if e["anomaly_type"] == "type_mismatch"]
        assert len(type_errors) >= 1

    def test_empty_records(self):
        result = validator_node({"cleaned_records": [], "raw_records": [], "inferred_schema": {}})
        assert result["validation_passed"] is False
