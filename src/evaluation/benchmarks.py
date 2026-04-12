"""Benchmark datasets and automated evaluation runner."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from src.evaluation.metrics import evaluate
from src.main import run_pipeline

logger = logging.getLogger(__name__)


def run_benchmark(
    dirty_path: str,
    ground_truth_path: str,
    output_dir: str = "data/benchmark_results",
) -> dict[str, Any]:
    """Run the pipeline on a dirty dataset and compare against ground truth.

    Ground truth file should be a JSON list of expected fixes:
    [{"row": 0, "column": "name", "expected_value": "John Doe"}, ...]
    """
    with open(ground_truth_path, "r", encoding="utf-8") as f:
        ground_truth = json.load(f)

    dataset_id = Path(dirty_path).stem
    output_path = str(Path(output_dir) / f"{dataset_id}_cleaned.csv")

    result = run_pipeline(
        dirty_path,
        output_path=output_path,
        dataset_id=dataset_id,
    )

    applied_fixes = result["cleaning_actions"]
    metrics = evaluate(applied_fixes, ground_truth)

    report = {
        "dataset": dataset_id,
        "pipeline_report": result["report"],
        "evaluation_metrics": metrics,
    }

    report_path = Path(output_dir) / f"{dataset_id}_eval.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(
        "Benchmark %s: P=%.3f R=%.3f F1=%.3f FPR=%.3f Coverage=%.3f",
        dataset_id,
        metrics["precision"],
        metrics["recall"],
        metrics["f1_score"],
        metrics["false_positive_rate"],
        metrics["coverage"],
    )

    return report


def run_all_benchmarks(
    benchmark_dir: str = "tests/fixtures",
    output_dir: str = "data/benchmark_results",
) -> list[dict[str, Any]]:
    """Discover and run all benchmark pairs in a directory.

    Expects pairs: <name>_dirty.csv + <name>_ground_truth.json
    """
    bench_path = Path(benchmark_dir)
    results = []

    dirty_files = sorted(bench_path.glob("*_dirty.csv"))
    for dirty_file in dirty_files:
        stem = dirty_file.stem.replace("_dirty", "")
        gt_file = bench_path / f"{stem}_ground_truth.json"
        if not gt_file.exists():
            logger.warning("No ground truth for %s, skipping", dirty_file.name)
            continue

        try:
            result = run_benchmark(str(dirty_file), str(gt_file), output_dir)
            results.append(result)
        except Exception as e:
            logger.error("Benchmark failed for %s: %s", dirty_file.name, e)
            results.append({"dataset": stem, "error": str(e)})

    return results
