"""Evaluation metrics for measuring cleaning accuracy."""

from __future__ import annotations

from typing import Any


def precision(
    applied_fixes: list[dict[str, Any]],
    ground_truth_fixes: list[dict[str, Any]],
) -> float:
    """Proportion of applied fixes that are correct.

    A fix is 'correct' if (row, column, new_value) matches ground truth.
    """
    if not applied_fixes:
        return 0.0

    gt_set = {
        (f["row"], f["column"], str(f["expected_value"]))
        for f in ground_truth_fixes
    }

    correct = sum(
        1
        for f in applied_fixes
        if (f.get("row"), f.get("column"), str(f.get("new_value"))) in gt_set
    )

    return correct / len(applied_fixes)


def recall(
    applied_fixes: list[dict[str, Any]],
    ground_truth_fixes: list[dict[str, Any]],
) -> float:
    """Proportion of ground-truth issues that were detected and correctly fixed."""
    if not ground_truth_fixes:
        return 0.0

    applied_set = {
        (f.get("row"), f.get("column"), str(f.get("new_value")))
        for f in applied_fixes
    }

    detected = sum(
        1
        for f in ground_truth_fixes
        if (f["row"], f["column"], str(f["expected_value"])) in applied_set
    )

    return detected / len(ground_truth_fixes)


def f1_score(
    applied_fixes: list[dict[str, Any]],
    ground_truth_fixes: list[dict[str, Any]],
) -> float:
    """Harmonic mean of precision and recall."""
    p = precision(applied_fixes, ground_truth_fixes)
    r = recall(applied_fixes, ground_truth_fixes)
    if p + r == 0:
        return 0.0
    return 2 * (p * r) / (p + r)


def false_positive_rate(
    applied_fixes: list[dict[str, Any]],
    ground_truth_fixes: list[dict[str, Any]],
) -> float:
    """Proportion of applied fixes that made data worse (not in ground truth)."""
    if not applied_fixes:
        return 0.0

    gt_set = {
        (f["row"], f["column"])
        for f in ground_truth_fixes
    }

    false_positives = sum(
        1
        for f in applied_fixes
        if (f.get("row"), f.get("column")) not in gt_set
    )

    return false_positives / len(applied_fixes)


def coverage(
    applied_fixes: list[dict[str, Any]],
    ground_truth_fixes: list[dict[str, Any]],
) -> float:
    """Proportion of ground-truth issues that were addressed (regardless of correctness)."""
    if not ground_truth_fixes:
        return 0.0

    applied_cells = {
        (f.get("row"), f.get("column"))
        for f in applied_fixes
    }

    gt_cells = {
        (f["row"], f["column"])
        for f in ground_truth_fixes
    }

    addressed = len(applied_cells & gt_cells)
    return addressed / len(gt_cells)


def evaluate(
    applied_fixes: list[dict[str, Any]],
    ground_truth_fixes: list[dict[str, Any]],
) -> dict[str, float]:
    """Compute all evaluation metrics."""
    return {
        "precision": round(precision(applied_fixes, ground_truth_fixes), 4),
        "recall": round(recall(applied_fixes, ground_truth_fixes), 4),
        "f1_score": round(f1_score(applied_fixes, ground_truth_fixes), 4),
        "false_positive_rate": round(false_positive_rate(applied_fixes, ground_truth_fixes), 4),
        "coverage": round(coverage(applied_fixes, ground_truth_fixes), 4),
        "total_applied": len(applied_fixes),
        "total_ground_truth": len(ground_truth_fixes),
    }
