"""Tests for regex-based tools."""

from src.tools.regex_tools import detect_format_pattern, find_pattern_violations, normalize_format


class TestDetectFormatPattern:
    def test_email_detection(self):
        values = ["a@b.com", "x@y.org", "test@test.io", "foo@bar.net"]
        result = detect_format_pattern.invoke({"values": values})
        assert result["pattern"] == "email"
        assert result["match_rate"] == 1.0

    def test_phone_detection(self):
        values = ["555-123-4567", "(555) 987-6543", "5551234567"]
        result = detect_format_pattern.invoke({"values": values})
        assert result["pattern"] == "phone_us"

    def test_date_iso(self):
        values = ["2024-01-15", "2024-02-20", "2024-03-10"]
        result = detect_format_pattern.invoke({"values": values})
        assert result["pattern"] == "date_iso"

    def test_empty_values(self):
        result = detect_format_pattern.invoke({"values": []})
        assert result["pattern"] is None


class TestFindPatternViolations:
    def test_email_violations(self):
        records = [
            {"email": "a@b.com"},
            {"email": "not-an-email"},
            {"email": "x@y.org"},
        ]
        violations = find_pattern_violations.invoke({
            "records": records,
            "column": "email",
            "pattern_name": "email",
        })
        assert len(violations) == 1
        assert violations[0]["row"] == 1

    def test_unknown_pattern(self):
        result = find_pattern_violations.invoke({
            "records": [{"a": "1"}],
            "column": "a",
            "pattern_name": "nonexistent_pattern",
        })
        assert result[0].get("error")


class TestNormalizeFormat:
    def test_email_lowercase(self):
        assert normalize_format.invoke({"value": "HELLO@WORLD.COM", "target_format": "email"}) == "hello@world.com"

    def test_phone_formatting(self):
        assert normalize_format.invoke({"value": "5551234567", "target_format": "phone_us"}) == "(555) 123-4567"

    def test_boolean_yes(self):
        assert normalize_format.invoke({"value": "Yes", "target_format": "boolean"}) == "true"

    def test_boolean_no(self):
        assert normalize_format.invoke({"value": "N", "target_format": "boolean"}) == "false"
