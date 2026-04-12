#!/usr/bin/env python3
"""Quick-start script to run the cleaning pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.main import run_pipeline


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_cleaning.py <input_file> [output_file]")
        sys.exit(1)

    source = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else str(Path(source).with_suffix(".cleaned.csv"))

    result = run_pipeline(source, output_path=output)
    report = result["report"]

    print(f"\nCleaned data saved to: {output}")
    print(f"Fixes applied: {report.get('total_fixes', 0)}")
    print(f"Confidence: {report.get('overall_confidence', 0):.1%}")


if __name__ == "__main__":
    main()
