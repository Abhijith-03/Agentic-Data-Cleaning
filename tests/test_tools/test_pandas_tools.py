"""Tests for pandas-based tools."""

from src.tools.pandas_tools import detect_duplicates, find_null_cells, get_column_info, get_value_counts


class TestGetColumnInfo:
    def test_basic_info(self):
        records = [{"name": "Alice"}, {"name": "Bob"}, {"name": ""}]
        result = get_column_info.invoke({"records": records, "column": "name"})
        assert result["total"] == 3
        assert result["null_count"] == 1
        assert result["unique_count"] == 3  # pandas counts "" as a distinct value

    def test_missing_column(self):
        result = get_column_info.invoke({"records": [{"a": "1"}], "column": "b"})
        assert "error" in result


class TestDetectDuplicates:
    def test_finds_duplicates(self):
        records = [{"a": "1", "b": "x"}, {"a": "2", "b": "y"}, {"a": "1", "b": "x"}]
        result = detect_duplicates.invoke({"records": records})
        assert result["duplicate_count"] == 1

    def test_no_duplicates(self):
        records = [{"a": "1"}, {"a": "2"}, {"a": "3"}]
        result = detect_duplicates.invoke({"records": records})
        assert result["duplicate_count"] == 0


class TestFindNullCells:
    def test_finds_nulls(self):
        records = [{"a": "1", "b": None}, {"a": "", "b": "2"}]
        result = find_null_cells.invoke({"records": records})
        assert len(result) == 2


class TestGetValueCounts:
    def test_counts(self):
        records = [{"c": "a"}, {"c": "b"}, {"c": "a"}, {"c": "a"}]
        result = get_value_counts.invoke({"records": records, "column": "c"})
        assert result["a"] == 3
        assert result["b"] == 1
