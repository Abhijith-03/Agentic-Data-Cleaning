"""FastAPI server with async task processing, download support, and premium UI."""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Literal, Optional

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.main import run_pipeline

logger = logging.getLogger(__name__)

UPLOAD_DIR = Path("data/uploads")
OUTPUT_DIR = Path("data/outputs")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# In-memory job store (use Redis in production)
_jobs: dict[str, dict[str, Any]] = {}

app = FastAPI(
    title="Agentic Data Cleaning Platform",
    description="Production-grade multi-agent platform for automated structured data cleaning",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── Models ────────────────────────────────────────────────────────────────

class JobStatus(BaseModel):
    job_id: str
    status: str  # pending | running | completed | failed
    progress: Optional[str] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    created_at: float = 0.0
    completed_at: Optional[float] = None


class SQLRequest(BaseModel):
    sql_query: str
    connection_string: str
    dataset_id: Optional[str] = None


class ReviewDecision(BaseModel):
    item_id: str
    action: Literal["accept", "reject", "edit"]
    new_value: Optional[str] = None


def _require_job(job_id: str) -> dict[str, Any]:
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


def _require_completed_job(job_id: str) -> dict[str, Any]:
    job = _require_job(job_id)
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job is {job['status']}, not completed")
    return job


def _paginate_rows(rows: list[dict[str, Any]], page: int, page_size: int) -> dict[str, Any]:
    page = max(1, page)
    page_size = max(1, min(page_size, 200))
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]
    return {
        "rows": page_rows,
        "page": page,
        "page_size": page_size,
        "total_rows": len(rows),
        "total_pages": (len(rows) + page_size - 1) // page_size if rows else 0,
        "column_names": list(page_rows[0].keys()) if page_rows else [],
    }


def _update_reviewed_value(result: dict[str, Any], row: int, column: str, value: Any) -> None:
    cleaned_records = result.get("cleaned_records", [])
    if 0 <= row < len(cleaned_records) and column in cleaned_records[row]:
        cleaned_records[row][column] = value

    cleaned_preview = result.get("cleaned_preview", {})
    preview_rows = cleaned_preview.get("rows", [])
    if 0 <= row < len(preview_rows) and column in preview_rows[row]:
        preview_rows[row][column] = value


def _append_review_audit(result: dict[str, Any], item: dict[str, Any], action: str, new_value: Any) -> None:
    result.setdefault("audit_log", []).append({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "row_index": item.get("row", -1),
        "column_name": item.get("column", ""),
        "original_value": item.get("old_value"),
        "new_value": new_value,
        "issue_type": item.get("issue_type", "low_confidence_review"),
        "fix_method": f"review:{action}",
        "confidence": 1.0,
        "reasoning": f"Mock human review action: {action}",
        "agent_name": "human_review",
        "tier": "human_review",
        "trace_id": "",
    })


# ── Background runner ─────────────────────────────────────────────────────

def _run_job(job_id: str, source: str, dataset_id: str, output_format: str = "csv",
             sql_query: Optional[str] = None, connection_string: Optional[str] = None) -> None:
    """Execute the pipeline in a background thread."""
    job = _jobs[job_id]
    job["status"] = "running"
    job["progress"] = "Loading data..."

    output_path = str(OUTPUT_DIR / f"{dataset_id}_cleaned.{output_format}")

    try:
        job["progress"] = "Running agent pipeline..."
        result = run_pipeline(
            source,
            output_path=output_path,
            output_format=output_format,
            dataset_id=dataset_id,
            sql_query=sql_query,
            connection_string=connection_string,
        )

        report = result["report"]
        job["status"] = "completed"
        job["progress"] = "Done"
        job["completed_at"] = time.time()
        job["result"] = {
            "dataset_id": dataset_id,
            "total_rows": report.get("total_rows", 0),
            "issues_detected": report.get("issues_detected", 0),
            "fixes_applied": report.get("total_fixes", 0),
            "overall_confidence": report.get("overall_confidence", 0.0),
            "validation_passed": report.get("validation_passed", False),
            "duration_seconds": report.get("duration_seconds", 0.0),
            "fix_breakdown": report.get("fix_breakdown", {}),
            "iterations": report.get("iterations", 1),
            "output_path": output_path,
            "pipeline_stages": result.get("pipeline_stages", {}),
            "stage_previews": result.get("stage_previews", {}),
            "raw_preview": result.get("raw_preview", {}),
            "cleaned_preview": result.get("cleaned_preview", {}),
            "reconstruction_report": result.get("reconstruction_report", {}),
            "inferred_schema": result.get("inferred_schema", {}),
            "schema_issues": result.get("schema_issues", []),
            "profile_report": result.get("profile_report", {}),
            "anomalies": result.get("anomalies", []),
            "audit_log": result.get("audit_log", []),
            "cleaning_actions": result.get("cleaning_actions", []),
            "llm_logs": result.get("llm_logs", []),
            "review_queue": result.get("review_queue", []),
            "low_confidence_fixes": result.get("low_confidence_fixes", []),
            "cleaned_records": result.get("cleaned_records", []),
            "chunk_results": result.get("chunk_results", []),
        }

    except Exception as e:
        logger.exception("Job %s failed", job_id)
        job["status"] = "failed"
        job["error"] = str(e)
        job["completed_at"] = time.time()


# ── Endpoints ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="UI not built")
    return index.read_text(encoding="utf-8")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "agentic-data-cleaning", "jobs_active": len(_jobs)}


@app.post("/upload")
@app.post("/api/clean/upload")
async def upload_and_clean(file: UploadFile = File(...)):
    """Upload a file and start async cleaning."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    suffix = Path(file.filename).suffix
    if suffix.lower() not in {".csv", ".tsv", ".xlsx", ".xls", ".parquet"}:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {suffix}")

    job_id = uuid.uuid4().hex[:12]
    dataset_id = f"{Path(file.filename).stem}_{job_id[:6]}"

    upload_path = UPLOAD_DIR / f"{job_id}{suffix}"
    content = await file.read()
    upload_path.write_bytes(content)

    _jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "progress": "Queued",
        "result": None,
        "error": None,
        "created_at": time.time(),
        "completed_at": None,
        "filename": file.filename,
        "dataset_id": dataset_id,
    }

    thread = threading.Thread(
        target=_run_job,
        args=(job_id, str(upload_path), dataset_id),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "dataset_id": dataset_id, "status": "pending"}


@app.post("/api/clean/sql")
async def clean_sql(request: SQLRequest):
    """Start async cleaning from a SQL query."""
    job_id = uuid.uuid4().hex[:12]
    dataset_id = request.dataset_id or f"sql_{job_id[:6]}"

    _jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "progress": "Queued",
        "result": None,
        "error": None,
        "created_at": time.time(),
        "completed_at": None,
        "dataset_id": dataset_id,
    }

    thread = threading.Thread(
        target=_run_job,
        args=(job_id, "", dataset_id, "csv", request.sql_query, request.connection_string),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "dataset_id": dataset_id, "status": "pending"}


@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Poll job status."""
    job = _require_job(job_id)
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "progress": job.get("progress"),
        "result": job.get("result"),
        "error": job.get("error"),
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
    }


@app.get("/pipeline/status")
async def pipeline_status(job_id: str = Query(...)):
    """Frontend-oriented pipeline status endpoint."""
    job = _require_job(job_id)
    result = job.get("result") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "progress": job.get("progress"),
        "dataset_id": job.get("dataset_id", ""),
        "pipeline_stages": result.get("pipeline_stages", {}),
        "overall_confidence": result.get("overall_confidence", 0.0),
        "validation_passed": result.get("validation_passed", False),
        "duration_seconds": result.get("duration_seconds", 0.0),
    }


@app.get("/data/preview")
async def data_preview(
    job_id: str = Query(...),
    stage: str = Query("cleaned"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=200),
):
    """Return preview rows for a stage or final cleaned data."""
    job = _require_completed_job(job_id)
    result = job["result"]

    if stage == "cleaned":
        rows = result.get("cleaned_records", [])
        return _paginate_rows(rows, page, page_size)
    if stage == "raw":
        preview = result.get("raw_preview", {})
        return _paginate_rows(preview.get("rows", []), page, page_size)

    stage_preview = result.get("stage_previews", {}).get(stage)
    if stage_preview is None:
        raise HTTPException(status_code=404, detail=f"No preview found for stage '{stage}'")
    return _paginate_rows(stage_preview.get("rows", []), page, page_size)


@app.get("/profiling")
async def profiling(job_id: str = Query(...)):
    """Return profiling and schema summary data."""
    job = _require_completed_job(job_id)
    result = job["result"]
    return {
        "profile_report": result.get("profile_report", {}),
        "inferred_schema": result.get("inferred_schema", {}),
        "schema_issues": result.get("schema_issues", []),
        "reconstruction_report": result.get("reconstruction_report", {}),
    }


@app.get("/anomalies")
async def anomalies(
    job_id: str = Query(...),
    severity: Optional[str] = Query(None),
    column: Optional[str] = Query(None),
):
    """Return anomaly list with optional filters."""
    job = _require_completed_job(job_id)
    items = list(job["result"].get("anomalies", []))
    if severity:
        items = [item for item in items if str(item.get("severity", "")).lower() == severity.lower()]
    if column:
        items = [item for item in items if str(item.get("column", "")).lower() == column.lower()]
    return {"anomalies": items, "count": len(items)}


@app.get("/cleaning/logs")
async def cleaning_logs(
    job_id: str = Query(...),
    method: Optional[str] = Query(None),
):
    """Return cleaning actions with optional method filter."""
    job = _require_completed_job(job_id)
    items = list(job["result"].get("cleaning_actions", []))
    if method:
        items = [item for item in items if str(item.get("rule", "")).startswith(method)]
    return {"cleaning_logs": items, "count": len(items)}


@app.get("/audit")
async def audit(job_id: str = Query(...)):
    """Return full audit log."""
    job = _require_completed_job(job_id)
    items = job["result"].get("audit_log", [])
    return {"audit": items, "count": len(items)}


@app.get("/llm/logs")
async def llm_logs(job_id: str = Query(...)):
    """Return only LLM-assisted fixes for the insights panel."""
    job = _require_completed_job(job_id)
    items = job["result"].get("llm_logs", [])
    return {"llm_logs": items, "count": len(items)}


@app.get("/review")
async def get_review_queue(job_id: str = Query(...)):
    """Return the mock human-review queue."""
    job = _require_completed_job(job_id)
    items = job["result"].get("review_queue", [])
    return {"review_queue": items, "count": len(items)}


@app.post("/review")
async def submit_review(job_id: str = Query(...), payload: ReviewDecision = ...):
    """Apply a mock review action to the queued low-confidence item."""
    job = _require_completed_job(job_id)
    result = job["result"]
    queue = result.get("review_queue", [])
    item = next((entry for entry in queue if entry.get("id") == payload.item_id), None)
    if item is None:
        raise HTTPException(status_code=404, detail="Review item not found")

    item["reviewed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    if payload.action == "accept":
        item["status"] = "accepted"
        item["reviewed_value"] = item.get("suggested_value")
        _append_review_audit(result, item, "accept", item.get("suggested_value"))
    elif payload.action == "reject":
        item["status"] = "rejected"
        item["reviewed_value"] = item.get("old_value")
        _update_reviewed_value(result, int(item.get("row", -1)), str(item.get("column", "")), item.get("old_value"))
        _append_review_audit(result, item, "reject", item.get("old_value"))
    else:
        if payload.new_value is None:
            raise HTTPException(status_code=400, detail="new_value is required for edit action")
        item["status"] = "edited"
        item["reviewed_value"] = payload.new_value
        _update_reviewed_value(result, int(item.get("row", -1)), str(item.get("column", "")), payload.new_value)
        _append_review_audit(result, item, "edit", payload.new_value)

    return {"review_item": item, "review_queue": queue}


@app.get("/export")
async def export_info(job_id: str = Query(...)):
    """Return download URLs and export metadata for the UI."""
    job = _require_completed_job(job_id)
    return {
        "dataset_id": job.get("dataset_id", ""),
        "downloads": {
            "cleaned_data": f"/api/download/{job_id}",
            "audit_report": f"/api/download/{job_id}/audit",
        },
        "api_endpoints": {
            "status": f"/pipeline/status?job_id={job_id}",
            "preview": f"/data/preview?job_id={job_id}&stage=cleaned",
            "profiling": f"/profiling?job_id={job_id}",
            "anomalies": f"/anomalies?job_id={job_id}",
            "cleaning_logs": f"/cleaning/logs?job_id={job_id}",
            "audit": f"/audit?job_id={job_id}",
            "llm_logs": f"/llm/logs?job_id={job_id}",
            "review": f"/review?job_id={job_id}",
        },
    }


@app.get("/api/jobs")
async def list_jobs():
    """List all jobs."""
    return sorted(
        [
            {
                "job_id": j["job_id"],
                "status": j["status"],
                "dataset_id": j.get("dataset_id", ""),
                "filename": j.get("filename", ""),
                "created_at": j.get("created_at", 0),
            }
            for j in _jobs.values()
        ],
        key=lambda x: x["created_at"],
        reverse=True,
    )


@app.get("/api/download/{job_id}")
async def download_cleaned(job_id: str):
    """Download the cleaned output file."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job is {job['status']}, not completed")

    output_path = job["result"].get("output_path", "")
    if not output_path or not Path(output_path).exists():
        raise HTTPException(status_code=404, detail="Output file not found")

    dataset_id = job.get("dataset_id", "cleaned")
    return FileResponse(
        path=output_path,
        filename=f"{dataset_id}_cleaned.csv",
        media_type="text/csv",
    )


@app.get("/api/download/{job_id}/audit")
async def download_audit(job_id: str):
    """Download the audit log as JSON."""
    job = _jobs.get(job_id)
    if not job or job["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")

    audit = job["result"].get("audit_log", [])
    audit_path = OUTPUT_DIR / f"{job.get('dataset_id', job_id)}_audit.json"
    audit_path.write_text(json.dumps(audit, indent=2, default=str), encoding="utf-8")

    return FileResponse(
        path=str(audit_path),
        filename=f"{job.get('dataset_id', 'audit')}_audit.json",
        media_type="application/json",
    )


@app.get("/api/patterns")
async def list_patterns(domain: Optional[str] = None):
    """List learned patterns from the pattern store."""
    from src.knowledge.pattern_store import PatternStore
    store = PatternStore()
    patterns = store.list_patterns(domain=domain)
    store.close()
    return [p.model_dump() for p in patterns]


@app.get("/{full_path:path}", response_class=HTMLResponse)
async def serve_spa(full_path: str):
    """Serve the SPA entrypoint for client-side routes in production."""
    if full_path.startswith("api/") or full_path in {"health", "upload", "review", "export", "pipeline/status", "data/preview", "profiling", "anomalies", "cleaning/logs", "audit", "llm/logs"}:
        raise HTTPException(status_code=404, detail="Not found")
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="UI not built")
    return index.read_text(encoding="utf-8")
