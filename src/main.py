"""CLI and programmatic entrypoint for the agentic data cleaning pipeline."""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

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


def _build_preview(records: list[dict[str, Any]], limit: int = 100) -> dict[str, Any]:
    preview_rows = records[:limit]
    return {
        "rows": preview_rows,
        "row_count": len(records),
        "column_names": list(preview_rows[0].keys()) if preview_rows else [],
        "truncated": len(records) > len(preview_rows),
    }


def _aggregate_pipeline_stages(
    stage_runs: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    for stage_name, runs in stage_runs.items():
        durations = [float(run.get("duration_ms", 0.0)) for run in runs]
        confidences = [run.get("confidence_score") for run in runs if run.get("confidence_score") is not None]
        aggregated[stage_name] = {
            "name": stage_name,
            "status": "success" if all(run.get("status") == "success" for run in runs) else "partial",
            "duration_ms": round(sum(durations), 2),
            "chunk_runs": len(runs),
            "confidence_score": round(sum(confidences) / len(confidences), 4) if confidences else None,
            "summary": runs[-1].get("summary", {}) if runs else {},
        }
    return aggregated


def _build_mock_review_queue(low_confidence_fixes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queue: list[dict[str, Any]] = []
    for index, fix in enumerate(low_confidence_fixes):
        queue.append({
            "id": f"review-{index}",
            "status": "pending",
            "row": fix.get("row", -1),
            "column": fix.get("column", ""),
            "old_value": fix.get("old_value"),
            "suggested_value": fix.get("new_value"),
            "confidence": fix.get("confidence", 0.0),
            "reasoning": fix.get("reasoning", ""),
            "issue_type": fix.get("issue_type", "unknown"),
            "fix_method": fix.get("rule", "unknown"),
        })
    return queue


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
    all_anomalies: list[dict[str, Any]] = []
    all_low_confidence: list[dict[str, Any]] = []
    all_llm_logs: list[dict[str, Any]] = []
    final_report: dict[str, Any] = {}
    pipeline_stage_runs: dict[str, list[dict[str, Any]]] = {}
    stage_previews: dict[str, dict[str, Any]] = {}
    chunk_results: list[dict[str, Any]] = []
    latest_schema: dict[str, Any] = {}
    latest_schema_issues: list[dict[str, Any]] = []
    latest_profile_report: dict[str, Any] = {}
    latest_reconstruction_report: dict[str, Any] = {}

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
        all_anomalies.extend(result.get("anomalies", []))
        all_low_confidence.extend(result.get("low_confidence_fixes", []))
        all_llm_logs.extend(result.get("llm_logs", []))
        final_report = result.get("final_report", {})
        latest_schema = result.get("inferred_schema", latest_schema)
        latest_schema_issues = result.get("schema_issues", latest_schema_issues)
        latest_profile_report = result.get("profile_report", latest_profile_report)
        latest_reconstruction_report = result.get("reconstruction_report", latest_reconstruction_report)
        for stage_name, stage_info in (result.get("pipeline_stages", {}) or {}).items():
            pipeline_stage_runs.setdefault(stage_name, []).append(stage_info)

        for stage_name, preview in (result.get("stage_previews", {}) or {}).items():
            stage_previews.setdefault(stage_name, preview)

        chunk_results.append({
            "chunk_index": i,
            "row_count": len(chunk),
            "pipeline_stages": result.get("pipeline_stages", {}),
            "stage_previews": result.get("stage_previews", {}),
            "anomalies": result.get("anomalies", []),
            "cleaning_actions": result.get("cleaning_actions", []),
            "audit_log": result.get("audit_log", []),
            "llm_logs": result.get("llm_logs", []),
            "review_queue": result.get("review_queue", []),
        })

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
        "anomalies": all_anomalies,
        "low_confidence_fixes": all_low_confidence,
        "llm_logs": all_llm_logs,
        "review_queue": _build_mock_review_queue(all_low_confidence),
        "inferred_schema": latest_schema,
        "schema_issues": latest_schema_issues,
        "profile_report": latest_profile_report,
        "reconstruction_report": latest_reconstruction_report,
        "pipeline_stages": _aggregate_pipeline_stages(pipeline_stage_runs),
        "stage_previews": stage_previews,
        "raw_preview": _build_preview(all_records),
        "cleaned_preview": _build_preview(all_cleaned),
        "chunk_results": chunk_results,
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
