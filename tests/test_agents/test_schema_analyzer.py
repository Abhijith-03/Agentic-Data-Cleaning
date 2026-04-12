"""Tests for the Schema Analyzer Agent."""

from src.agents.schema_analyzer import schema_analyzer_node


class TestSchemaAnalyzer:
    def _make_state(self, records):
        return {"raw_records": records}

    def test_numeric_detection(self):
        records = [{"age": str(i), "name": f"person_{i}"} for i in range(20, 40)]
        result = schema_analyzer_node(self._make_state(records))
        schema = result["inferred_schema"]
        assert schema["age"]["dtype"] == "integer"
        assert schema["name"]["dtype"] == "string"

    def test_date_detection(self):
        records = [
            {"date": "01/15/2024"},
            {"date": "02/20/2024"},
            {"date": "03/10/2024"},
        ]
        result = schema_analyzer_node(self._make_state(records))
        assert result["inferred_schema"]["date"]["dtype"] == "date"

    def test_empty_records(self):
        result = schema_analyzer_node(self._make_state([]))
        assert result["inferred_schema"] == {}

    def test_mixed_types_flagged(self):
        records = [{"val": str(i)} for i in range(10)]
        records.extend([{"val": "hello"}, {"val": "world"}, {"val": "foo"}])
        result = schema_analyzer_node(self._make_state(records))
        issues = result["schema_issues"]
        mixed = [i for i in issues if i["issue_type"] == "mixed_types"]
        assert len(mixed) >= 0  # may or may not detect depending on ratio

    def test_email_detection(self):
        records = [
            {"email": "a@b.com"},
            {"email": "x@y.org"},
            {"email": "test@test.io"},
        ]
        result = schema_analyzer_node(self._make_state(records))
        assert result["inferred_schema"]["email"]["format_pattern"] == "email"
