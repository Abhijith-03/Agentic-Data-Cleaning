"""Tests for the evaluation metrics module."""

from src.evaluation.metrics import coverage, evaluate, f1_score, false_positive_rate, precision, recall


class TestMetrics:
    def _gt(self):
        return [
            {"row": 0, "column": "a", "expected_value": "fixed_a"},
            {"row": 1, "column": "b", "expected_value": "fixed_b"},
            {"row": 2, "column": "c", "expected_value": "fixed_c"},
        ]

    def test_perfect_precision_and_recall(self):
        applied = [
            {"row": 0, "column": "a", "new_value": "fixed_a"},
            {"row": 1, "column": "b", "new_value": "fixed_b"},
            {"row": 2, "column": "c", "new_value": "fixed_c"},
        ]
        gt = self._gt()
        assert precision(applied, gt) == 1.0
        assert recall(applied, gt) == 1.0
        assert f1_score(applied, gt) == 1.0

    def test_partial_recall(self):
        applied = [
            {"row": 0, "column": "a", "new_value": "fixed_a"},
        ]
        gt = self._gt()
        assert recall(applied, gt) == 1 / 3

    def test_false_positives(self):
        applied = [
            {"row": 0, "column": "a", "new_value": "fixed_a"},
            {"row": 5, "column": "z", "new_value": "wrong"},
        ]
        gt = self._gt()
        assert false_positive_rate(applied, gt) == 0.5

    def test_coverage(self):
        applied = [
            {"row": 0, "column": "a", "new_value": "wrong_value"},
            {"row": 1, "column": "b", "new_value": "also_wrong"},
        ]
        gt = self._gt()
        assert coverage(applied, gt) == 2 / 3

    def test_evaluate_full(self):
        applied = [
            {"row": 0, "column": "a", "new_value": "fixed_a"},
            {"row": 1, "column": "b", "new_value": "fixed_b"},
        ]
        gt = self._gt()
        result = evaluate(applied, gt)
        assert "precision" in result
        assert "recall" in result
        assert "f1_score" in result
        assert result["total_applied"] == 2
        assert result["total_ground_truth"] == 3

    def test_empty_inputs(self):
        assert precision([], []) == 0.0
        assert recall([], []) == 0.0
        assert f1_score([], []) == 0.0
