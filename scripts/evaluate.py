#!/usr/bin/env python3
"""Run the evaluation benchmark suite."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.evaluation.benchmarks import run_all_benchmarks


def main() -> None:
    benchmark_dir = sys.argv[1] if len(sys.argv) > 1 else "tests/fixtures"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/benchmark_results"

    results = run_all_benchmarks(benchmark_dir, output_dir)

    print("\n" + "=" * 70)
    print("  BENCHMARK RESULTS")
    print("=" * 70)

    for r in results:
        if "error" in r:
            print(f"  {r['dataset']:30s}  ERROR: {r['error']}")
        else:
            m = r["evaluation_metrics"]
            print(
                f"  {r['dataset']:30s}  "
                f"P={m['precision']:.3f}  R={m['recall']:.3f}  "
                f"F1={m['f1_score']:.3f}  FPR={m['false_positive_rate']:.3f}"
            )

    print("=" * 70)


if __name__ == "__main__":
    main()
