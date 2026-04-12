"""FastAPI server with async task processing, download support, and premium UI."""

from __future__ import annotations

import json
import logging
import tempfile
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Optional

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
            "audit_log": result.get("audit_log", []),
            "cleaning_actions": result.get("cleaning_actions", []),
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
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "progress": job.get("progress"),
        "result": job.get("result"),
        "error": job.get("error"),
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
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
