"""CLI and programmatic entrypoint for the agentic data cleaning pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from src.config import settings
from src.graph.checkpointer import get_checkpointer
from src.graph.workflow import compile_graph
from src.ingestion.chunker import chunk_records
from src.ingestion.loader import dataframe_to_records, load
from src.tools.db_tools import save_results_csv, save_results_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline(
    source: str,
    *,
    output_path: str | None = None,
    output_format: str = "csv",
    dataset_id: str | None = None,
    sql_query: str | None = None,
    connection_string: str | None = None,
) -> dict[str, Any]:
    """Run the full data cleaning pipeline and return the final report."""
    start = time.time()
    logger.info("Starting data cleaning pipeline for: %s", source)

    # Load data
    df = load(source, sql_query=sql_query, connection_string=connection_string)
    all_records = dataframe_to_records(df)
    logger.info("Loaded %d records, %d columns", len(all_records), len(df.columns))

    # Chunk for large datasets
    chunks = chunk_records(all_records)
    logger.info("Split into %d chunk(s) of up to %d rows", len(chunks), settings.chunk_size)

    # Compile graph
    checkpointer = get_checkpointer()
    app = compile_graph(checkpointer=checkpointer)

    all_cleaned: list[dict[str, Any]] = []
    all_actions: list[dict[str, Any]] = []
    all_audit: list[dict[str, Any]] = []
    final_report: dict[str, Any] = {}

    for i, chunk in enumerate(chunks):
        logger.info("Processing chunk %d/%d (%d records)", i + 1, len(chunks), len(chunk))

        initial_state = {
            "raw_data_path": source,
            "raw_records": chunk,
            "dataset_id": dataset_id or Path(source).stem,
            "iteration_count": 0,
            "chunk_index": i,
            "total_chunks": len(chunks),
        }

        config = {"configurable": {"thread_id": f"{dataset_id or 'run'}_{i}"}}
        result = app.invoke(initial_state, config=config)

        all_cleaned.extend(result.get("cleaned_records", chunk))
        all_actions.extend(result.get("cleaning_actions", []))
        all_audit.extend(result.get("audit_log", []))
        final_report = result.get("final_report", {})

    # Aggregate report
    elapsed = time.time() - start
    final_report["total_rows"] = len(all_records)
    final_report["total_fixes"] = len(all_actions)
    final_report["duration_seconds"] = round(elapsed, 2)
    final_report["chunks_processed"] = len(chunks)

    # Save output
    if output_path:
        if output_format == "json":
            save_results_json(all_cleaned, output_path)
        else:
            save_results_csv(all_cleaned, output_path)

        # Save audit log alongside
        audit_path = Path(output_path).with_suffix(".audit.json")
        with open(audit_path, "w", encoding="utf-8") as f:
            json.dump(all_audit, f, indent=2, default=str)
        logger.info("Audit log saved to %s", audit_path)

        # Save report
        report_path = Path(output_path).with_suffix(".report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(final_report, f, indent=2, default=str)
        logger.info("Report saved to %s", report_path)

    logger.info(
        "Pipeline complete in %.1fs — %d records, %d fixes, confidence=%.3f",
        elapsed,
        len(all_records),
        len(all_actions),
        final_report.get("overall_confidence", 0.0),
    )

    return {
        "cleaned_records": all_cleaned,
        "cleaning_actions": all_actions,
        "audit_log": all_audit,
        "report": final_report,
    }


def cli_main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Agentic Data Cleaning Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("source", help="Path to input file (CSV, Excel, Parquet)")
    parser.add_argument("-o", "--output", help="Path for cleaned output file")
    parser.add_argument(
        "-f", "--format", choices=["csv", "json"], default="csv", help="Output format"
    )
    parser.add_argument("--dataset-id", help="Unique identifier for this dataset")
    parser.add_argument("--sql-query", help="SQL query (when source is a connection string)")
    parser.add_argument("--connection-string", help="Database connection string")

    args = parser.parse_args()

    result = run_pipeline(
        args.source,
        output_path=args.output,
        output_format=args.format,
        dataset_id=args.dataset_id,
        sql_query=args.sql_query,
        connection_string=args.connection_string,
    )

    report = result["report"]
    print("\n" + "=" * 60)
    print("  DATA CLEANING REPORT")
    print("=" * 60)
    print(f"  Rows processed:     {report.get('total_rows', 0)}")
    print(f"  Issues detected:    {report.get('issues_detected', 0)}")
    print(f"  Fixes applied:      {report.get('total_fixes', 0)}")
    print(f"  Overall confidence: {report.get('overall_confidence', 0.0):.1%}")
    print(f"  Duration:           {report.get('duration_seconds', 0.0):.1f}s")
    print(f"  Validation passed:  {report.get('validation_passed', False)}")
    print("=" * 60)


if __name__ == "__main__":
    cli_main()
