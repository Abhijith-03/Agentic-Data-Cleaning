"""Tests for the Anomaly Detection Agent."""

from src.agents.anomaly_detector import anomaly_detector_node


class TestAnomalyDetector:
    def test_detects_numeric_outlier(self):
        records = [{"val": str(i)} for i in range(100)]
        records.append({"val": "10000"})  # extreme outlier

        state = {
            "raw_records": records,
            "inferred_schema": {"val": {"dtype": "integer", "format_pattern": "integer"}},
        }
        result = anomaly_detector_node(state)
        anomalies = result["anomalies"]

        outlier_anomalies = [a for a in anomalies if a["anomaly_type"] == "numeric_outlier"]
        assert len(outlier_anomalies) >= 1
        assert any(a["value"] == "10000" for a in outlier_anomalies)

    def test_no_anomalies_in_clean_data(self):
        records = [{"name": "Alice", "age": str(25 + i)} for i in range(20)]
        state = {
            "raw_records": records,
            "inferred_schema": {
                "name": {"dtype": "string", "format_pattern": None},
                "age": {"dtype": "integer", "format_pattern": "integer"},
            },
        }
        result = anomaly_detector_node(state)
        critical = [a for a in result["anomalies"] if a["severity"] == "critical"]
        assert len(critical) == 0

    def test_empty_records(self):
        result = anomaly_detector_node({"raw_records": [], "inferred_schema": {}})
        assert result["anomalies"] == []
