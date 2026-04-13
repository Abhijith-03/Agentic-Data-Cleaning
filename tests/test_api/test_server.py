from __future__ import annotations

from fastapi.testclient import TestClient

from src.api import server


def _completed_job() -> dict:
    return {
        "job_id": "job123",
        "status": "completed",
        "progress": "Done",
        "dataset_id": "demo_dataset",
        "created_at": 1.0,
        "completed_at": 2.0,
        "result": {
            "dataset_id": "demo_dataset",
            "overall_confidence": 0.91,
            "validation_passed": True,
            "duration_seconds": 1.23,
            "pipeline_stages": {
                "ingest": {"status": "success", "duration_ms": 10.0},
                "cleaning": {"status": "success", "duration_ms": 22.0, "confidence_score": 0.88},
            },
            "raw_preview": {
                "rows": [{"a": "1", "b": "x"}],
                "row_count": 1,
                "column_names": ["a", "b"],
                "truncated": False,
            },
            "stage_previews": {
                "schema_analysis": {
                    "rows": [{"a": "1", "b": "x"}],
                    "row_count": 1,
                    "column_names": ["a", "b"],
                    "truncated": False,
                }
            },
            "cleaned_preview": {
                "rows": [{"a": "1", "b": "clean"}],
                "row_count": 1,
                "column_names": ["a", "b"],
                "truncated": False,
            },
            "cleaned_records": [{"a": "1", "b": "clean"}],
            "profile_report": {"a": {"null_pct": 0.0}},
            "inferred_schema": {"a": {"dtype": "integer"}},
            "schema_issues": [],
            "reconstruction_report": {"rows_parsed": 1, "rows_dropped": 0},
            "anomalies": [{"row": 0, "column": "b", "severity": "warning"}],
            "cleaning_actions": [
                {
                    "row": 0,
                    "column": "b",
                    "old_value": "x",
                    "new_value": "clean",
                    "rule": "llm:gpt-4o",
                    "confidence": 0.62,
                    "reasoning": "Normalized by model",
                }
            ],
            "audit_log": [],
            "llm_logs": [
                {
                    "row": 0,
                    "column": "b",
                    "prompt": "Column: b",
                    "structured_output": {"corrected_value": "clean"},
                    "confidence": 0.62,
                }
            ],
            "review_queue": [
                {
                    "id": "review-0",
                    "status": "pending",
                    "row": 0,
                    "column": "b",
                    "old_value": "x",
                    "suggested_value": "clean",
                    "confidence": 0.62,
                    "reasoning": "Normalized by model",
                    "issue_type": "format_violation",
                    "fix_method": "llm:gpt-4o",
                }
            ],
        },
    }


def test_pipeline_status_and_preview_endpoints():
    client = TestClient(server.app)
    server._jobs.clear()
    server._jobs["job123"] = _completed_job()

    status = client.get("/pipeline/status", params={"job_id": "job123"})
    assert status.status_code == 200
    assert "pipeline_stages" in status.json()

    preview = client.get("/data/preview", params={"job_id": "job123", "stage": "cleaned"})
    assert preview.status_code == 200
    assert preview.json()["rows"][0]["b"] == "clean"

    profiling = client.get("/profiling", params={"job_id": "job123"})
    assert profiling.status_code == 200
    assert "profile_report" in profiling.json()


def test_anomaly_and_llm_log_endpoints():
    client = TestClient(server.app)
    server._jobs.clear()
    server._jobs["job123"] = _completed_job()

    anomalies = client.get("/anomalies", params={"job_id": "job123", "severity": "warning"})
    assert anomalies.status_code == 200
    assert anomalies.json()["count"] == 1

    llm_logs = client.get("/llm/logs", params={"job_id": "job123"})
    assert llm_logs.status_code == 200
    assert llm_logs.json()["llm_logs"][0]["column"] == "b"


def test_mock_review_edit_updates_result_and_audit():
    client = TestClient(server.app)
    server._jobs.clear()
    server._jobs["job123"] = _completed_job()

    response = client.post(
        "/review",
        params={"job_id": "job123"},
        json={"item_id": "review-0", "action": "edit", "new_value": "approved"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["review_item"]["status"] == "edited"
    assert payload["review_item"]["reviewed_value"] == "approved"

    job = server._jobs["job123"]
    assert job["result"]["cleaned_records"][0]["b"] == "approved"
    assert job["result"]["audit_log"][-1]["fix_method"] == "review:edit"


def test_export_endpoint_returns_download_metadata():
    client = TestClient(server.app)
    server._jobs.clear()
    server._jobs["job123"] = _completed_job()

    export = client.get("/export", params={"job_id": "job123"})
    assert export.status_code == 200
    assert export.json()["downloads"]["cleaned_data"] == "/api/download/job123"
