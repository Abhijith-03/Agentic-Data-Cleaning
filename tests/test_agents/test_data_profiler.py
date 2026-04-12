"""Tests for the Data Profiling Agent."""

from src.agents.data_profiler import data_profiler_node


class TestDataProfiler:
    def test_basic_profiling(self):
        records = [
            {"name": "Alice", "age": "25", "city": "NYC"},
            {"name": "Bob", "age": "30", "city": "LA"},
            {"name": "", "age": "35", "city": "NYC"},
            {"name": "Dave", "age": "", "city": "LA"},
        ]
        result = data_profiler_node({"raw_records": records})

        profile = result["profile_report"]
        assert "name" in profile
        assert "age" in profile
        assert profile["name"]["null_count"] == 1
        assert profile["age"]["null_count"] == 1

    def test_quality_score_range(self):
        records = [{"a": "1", "b": "x"} for _ in range(10)]
        result = data_profiler_node({"raw_records": records})
        score = result["data_quality_score"]
        assert 0.0 <= score <= 1.0

    def test_empty_records(self):
        result = data_profiler_node({"raw_records": []})
        assert result["profile_report"] == {}
        assert result["data_quality_score"] == 0.0

    def test_numeric_stats(self):
        records = [{"val": str(i)} for i in range(1, 101)]
        result = data_profiler_node({"raw_records": records})
        profile = result["profile_report"]["val"]
        assert profile["is_numeric"] is True
        assert profile["mean"] == 50.5
