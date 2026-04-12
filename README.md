# Agentic Data Cleaning Platform

Production-grade, multi-agent platform for automated structured data cleaning powered by LangGraph, LangChain, and LangSmith.

## Features

- **6 specialized agents**: Schema Analyzer, Data Profiler, Anomaly Detector, Cleaner, Validator, Confidence Scorer
- **Hybrid cleaning engine**: Deterministic rules first, auto-learned patterns second, LLM fallback third
- **Confidence scoring**: Three-tier system (rules 0.95+, patterns 0.85+, LLM 0.5-0.85) with human-in-the-loop routing
- **Auto-learning pattern store**: LLM fixes that pass validation become reusable rules, reducing cost over time
- **Full audit trail**: Every change logged with reasoning, confidence, and LangSmith trace ID
- **Cyclic validation loop**: Up to 3 clean-validate iterations with automatic convergence
- **LangSmith observability**: Every agent decision traced and searchable

## Architecture

```
Ingest → Schema Analysis → Data Profiling → Anomaly Detection → Cleaning → Validation
                                                                    ↑           |
                                                                    +-----------+
                                                                   (if failed & iterations < 3)
                                                                        |
                                                                  Confidence Scoring → Output
                                                                        |
                                                                  [Human Review] (optional)
```

## Quick Start

### 1. Install

```bash
pip install -e ".[dev]"
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your OpenAI and LangSmith API keys
```

### 3. Run

**CLI:**
```bash
python -m src.main data/input.csv -o data/output.csv
```

**Python API:**
```python
from src.main import run_pipeline

result = run_pipeline("data/dirty.csv", output_path="data/cleaned.csv")
print(result["report"])
```

**FastAPI server:**
```bash
uvicorn src.api.server:app --reload
# POST /clean/upload with a file, or POST /clean/file with {"source_path": "..."}
```

## Project Structure

```
src/
├── config.py              # Central configuration
├── main.py                # CLI and programmatic entrypoint
├── ingestion/             # CSV/Excel/SQL loader + chunker
├── agents/                # 6 specialized LangGraph agent nodes
│   ├── schema_analyzer.py
│   ├── data_profiler.py
│   ├── anomaly_detector.py
│   ├── cleaner.py         # Hybrid rules + pattern + LLM engine
│   ├── validator.py
│   └── confidence_scorer.py
├── graph/                 # LangGraph state + workflow + checkpointing
├── tools/                 # LangChain tools (pandas, stats, regex)
├── knowledge/             # Rules engine + auto-learning pattern store
├── audit/                 # Structured audit logging
├── evaluation/            # Precision/recall/F1 metrics + benchmark runner
└── api/                   # FastAPI HTTP interface
```

## Testing

```bash
pytest tests/ -v
```

## Evaluation

Place benchmark pairs in `tests/fixtures/` as `<name>_dirty.csv` + `<name>_ground_truth.json`, then:

```bash
python scripts/evaluate.py
```

## Configuration

All parameters are configurable via environment variables or `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | OpenAI API key |
| `PRIMARY_MODEL` | `gpt-4o` | Primary LLM for complex repairs |
| `FAST_MODEL` | `gpt-4o-mini` | Fast LLM for simple tasks |
| `CONFIDENCE_THRESHOLD` | `0.7` | Below this, fixes go to human review |
| `MAX_CLEANING_ITERATIONS` | `3` | Max clean-validate loops |
| `CHUNK_SIZE` | `50000` | Rows per processing chunk |
| `POLARS_THRESHOLD` | `500000` | Switch to Polars above this row count |
| `HUMAN_IN_LOOP_ENABLED` | `false` | Enable human review queue |

## Tech Stack

- **LangGraph** — Stateful agent workflow with cyclic graph support
- **LangChain** — Tool abstraction and LLM integration
- **LangSmith** — Tracing, observability, and evaluation
- **Pandas / Polars** — Data processing
- **FastAPI** — HTTP API
- **SQLite** — Pattern store and audit persistence
