"""Tests for statistical analysis tools."""

from src.tools.stats_tools import compute_column_statistics, detect_iqr_outliers, detect_zscore_outliers


class TestComputeColumnStatistics:
    def test_numeric_column(self):
        records = [{"val": str(i)} for i in range(1, 11)]
        result = compute_column_statistics.invoke({"records": records, "column": "val"})
        assert result["count"] == 10
        assert result["mean"] == 5.5
        assert result["min"] == 1.0
        assert result["max"] == 10.0

    def test_non_numeric_column(self):
        records = [{"val": "hello"}, {"val": "world"}]
        result = compute_column_statistics.invoke({"records": records, "column": "val"})
        assert result["numeric_count"] == 0

    def test_missing_column(self):
        result = compute_column_statistics.invoke({"records": [{"a": "1"}], "column": "b"})
        assert "error" in result


class TestZscoreOutliers:
    def test_detects_extreme_outlier(self):
        records = [{"val": str(i)} for i in range(100)]
        records.append({"val": "1000"})
        outliers = detect_zscore_outliers.invoke({"records": records, "column": "val"})
        assert len(outliers) >= 1
        assert any(o["value"] == "1000" for o in outliers)

    def test_no_outliers_in_uniform_data(self):
        records = [{"val": "5"} for _ in range(50)]
        outliers = detect_zscore_outliers.invoke({"records": records, "column": "val"})
        assert len(outliers) == 0


class TestIqrOutliers:
    def test_detects_extreme_outlier(self):
        records = [{"val": str(i)} for i in range(100)]
        records.append({"val": "500"})
        outliers = detect_iqr_outliers.invoke({"records": records, "column": "val"})
        assert len(outliers) >= 1
