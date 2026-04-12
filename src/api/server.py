"""FastAPI server for HTTP access to the data cleaning pipeline."""

from __future__ import annotations

import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.main import run_pipeline

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Agentic Data Cleaning Platform",
    description="Production-grade multi-agent platform for automated structured data cleaning",
    version="0.1.0",
)


class CleaningRequest(BaseModel):
    """Request body for cleaning via file path or SQL."""
    source_path: str = Field(description="Path to the input file")
    dataset_id: str | None = None
    output_format: str = "csv"
    sql_query: str | None = None
    connection_string: str | None = None


class CleaningResponse(BaseModel):
    dataset_id: str
    total_rows: int
    issues_detected: int
    fixes_applied: int
    overall_confidence: float
    validation_passed: bool
    duration_seconds: float
    output_path: str | None = None
    fix_breakdown: dict[str, int] = Field(default_factory=dict)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "healthy", "service": "agentic-data-cleaning"}


@app.post("/clean/file", response_model=CleaningResponse)
async def clean_from_path(request: CleaningRequest) -> CleaningResponse:
    """Run the cleaning pipeline on a file specified by path."""
    source = request.source_path
    if not Path(source).exists():
        raise HTTPException(status_code=404, detail=f"File not found: {source}")

    dataset_id = request.dataset_id or Path(source).stem
    output_path = str(Path(source).with_name(f"{dataset_id}_cleaned.{request.output_format}"))

    try:
        result = run_pipeline(
            source,
            output_path=output_path,
            output_format=request.output_format,
            dataset_id=dataset_id,
            sql_query=request.sql_query,
            connection_string=request.connection_string,
        )
    except Exception as e:
        logger.exception("Pipeline failed for %s", source)
        raise HTTPException(status_code=500, detail=str(e))

    report = result["report"]
    return CleaningResponse(
        dataset_id=dataset_id,
        total_rows=report.get("total_rows", 0),
        issues_detected=report.get("issues_detected", 0),
        fixes_applied=report.get("total_fixes", 0),
        overall_confidence=report.get("overall_confidence", 0.0),
        validation_passed=report.get("validation_passed", False),
        duration_seconds=report.get("duration_seconds", 0.0),
        output_path=output_path,
        fix_breakdown=report.get("fix_breakdown", {}),
    )


@app.post("/clean/upload", response_model=CleaningResponse)
async def clean_uploaded_file(file: UploadFile = File(...)) -> CleaningResponse:
    """Upload a file and run the cleaning pipeline."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    suffix = Path(file.filename).suffix
    if suffix.lower() not in {".csv", ".tsv", ".xlsx", ".xls", ".parquet"}:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {suffix}")

    dataset_id = f"{Path(file.filename).stem}_{uuid.uuid4().hex[:6]}"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="upload_") as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    output_path = str(Path(tmp_path).with_name(f"{dataset_id}_cleaned.csv"))

    try:
        result = run_pipeline(
            tmp_path,
            output_path=output_path,
            output_format="csv",
            dataset_id=dataset_id,
        )
    except Exception as e:
        logger.exception("Pipeline failed for uploaded file %s", file.filename)
        raise HTTPException(status_code=500, detail=str(e))

    report = result["report"]
    return CleaningResponse(
        dataset_id=dataset_id,
        total_rows=report.get("total_rows", 0),
        issues_detected=report.get("issues_detected", 0),
        fixes_applied=report.get("total_fixes", 0),
        overall_confidence=report.get("overall_confidence", 0.0),
        validation_passed=report.get("validation_passed", False),
        duration_seconds=report.get("duration_seconds", 0.0),
        output_path=output_path,
        fix_breakdown=report.get("fix_breakdown", {}),
    )


@app.get("/patterns")
async def list_patterns(domain: str | None = None) -> list[dict[str, Any]]:
    """List learned patterns from the pattern store."""
    from src.knowledge.pattern_store import PatternStore

    store = PatternStore()
    patterns = store.list_patterns(domain=domain)
    store.close()
    return [p.model_dump() for p in patterns]
