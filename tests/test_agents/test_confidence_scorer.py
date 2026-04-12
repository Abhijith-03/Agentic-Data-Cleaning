"""Tests for the Confidence Scoring Agent."""

from src.agents.confidence_scorer import confidence_scorer_node


class TestConfidenceScorer:
    def test_scores_fixes(self):
        state = {
            "raw_records": [{"a": "1"}],
            "cleaned_records": [{"a": "2"}],
            "cleaning_actions": [
                {"row": 0, "column": "a", "old_value": "1", "new_value": "2",
                 "rule": "rule:test", "confidence": 0.95, "reasoning": "test",
                 "issue_type": "format"},
            ],
            "anomalies": [],
            "data_quality_score": 0.5,
            "iteration_count": 1,
            "validation_passed": True,
        }
        result = confidence_scorer_node(state)
        assert len(result["audit_log"]) == 1
        assert result["low_confidence_fixes"] == []
        report = result["final_report"]
        assert report["overall_confidence"] == 0.95

    def test_routes_low_confidence(self):
        state = {
            "raw_records": [{"a": "1"}],
            "cleaned_records": [{"a": "2"}],
            "cleaning_actions": [
                {"row": 0, "column": "a", "old_value": "1", "new_value": "2",
                 "rule": "llm:gpt-4o", "confidence": 0.5, "reasoning": "uncertain",
                 "issue_type": "outlier"},
            ],
            "anomalies": [],
            "data_quality_score": 0.5,
            "iteration_count": 1,
            "validation_passed": True,
        }
        result = confidence_scorer_node(state)
        assert len(result["low_confidence_fixes"]) == 1

    def test_empty_actions(self):
        state = {
            "raw_records": [],
            "cleaned_records": [],
            "cleaning_actions": [],
            "anomalies": [],
        }
        result = confidence_scorer_node(state)
        assert result["low_confidence_fixes"] == []
        assert result["final_report"]["overall_confidence"] == 0.0
