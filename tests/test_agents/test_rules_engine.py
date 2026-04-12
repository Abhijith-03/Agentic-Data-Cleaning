"""Tests for the deterministic rules engine."""

from src.knowledge.rules_engine import (
    apply_format_rules,
    coerce_boolean,
    coerce_numeric_string,
    impute_categorical_mode,
    impute_numeric_median,
    normalize_date_to_iso,
    normalize_email,
    normalize_phone_us,
    trim_whitespace,
)


class TestMissingValueImputation:
    def test_median_imputation(self):
        values = ["10", "20", "30", "40", "50"]
        result = impute_numeric_median(values, "", "test_col")
        assert result is not None
        assert result.new_value == "30.0"
        assert result.confidence >= 0.85

    def test_mode_imputation(self):
        values = ["A", "B", "A", "A", "C"]
        result = impute_categorical_mode(values, "", "test_col")
        assert result is not None
        assert result.new_value == "A"

    def test_no_imputation_needed(self):
        result = impute_numeric_median(["10", "20"], "15", "test_col")
        assert result is None


class TestFormatNormalization:
    def test_date_us_to_iso(self):
        result = normalize_date_to_iso("01/15/2024", "date_col")
        assert result is not None
        assert result.new_value == "2024-01-15"

    def test_already_iso(self):
        result = normalize_date_to_iso("2024-01-15", "date_col")
        assert result is None

    def test_phone_normalization(self):
        result = normalize_phone_us("5551234567", "phone")
        assert result is not None
        assert result.new_value == "(555) 123-4567"

    def test_phone_with_country_code(self):
        result = normalize_phone_us("15551234567", "phone")
        assert result is not None
        assert result.new_value == "(555) 123-4567"

    def test_email_normalize(self):
        result = normalize_email("  HELLO@WORLD.COM  ", "email")
        assert result is not None
        assert result.new_value == "hello@world.com"

    def test_boolean_coerce(self):
        result = coerce_boolean("Yes", "flag")
        assert result is not None
        assert result.new_value == "true"

    def test_boolean_already_clean(self):
        result = coerce_boolean("true", "flag")
        assert result is None

    def test_numeric_coerce(self):
        result = coerce_numeric_string("$1,234.56", "salary")
        assert result is not None
        assert result.new_value == "1234.56"

    def test_trim_whitespace(self):
        result = trim_whitespace("  hello   world  ", "name")
        assert result is not None
        assert result.new_value == "hello world"


class TestApplyFormatRules:
    def test_applies_first_matching_rule(self):
        result = apply_format_rules("  HELLO@WORLD.COM  ", "email")
        assert result is not None
        # Should trigger trim first
        assert result.rule_name == "rule:trim_whitespace"
