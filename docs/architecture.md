# Architecture Documentation

## System Overview

The Agentic Data Cleaning Platform is a multi-agent system built on LangGraph that processes structured tabular data through a pipeline of specialized agents. Each agent handles a specific phase of data quality assessment and repair.

## Agent Pipeline

### 1. Ingest Node
- Loads CSV, Excel, TSV, Parquet, or SQL data
- Converts to list-of-dicts (JSON-serializable state)
- Chunks large datasets for parallel processing

### 2. Schema Analyzer Agent
- Samples rows (first N + random M) to infer column types
- Detects: integers, floats, booleans, dates, emails, phones, URLs, UUIDs
- Flags mixed-type columns and empty columns
- Output: `inferred_schema`, `schema_issues`

### 3. Data Profiling Agent
- Per-column statistics: null%, unique%, mean, median, std, skew, quartiles
- Categorical profiling: top values, string lengths
- Computes overall data quality score (0.0 - 1.0)
- Output: `profile_report`, `data_quality_score`

### 4. Anomaly Detection Agent
- Numeric outliers: Z-score AND IQR intersection (high precision)
- Categorical anomalies: extremely rare values
- Format violations: values not matching the column's detected pattern
- Cross-column: logical constraint violations (e.g., start_date > end_date)
- Output: `anomalies` (each tagged with severity: critical/warning/info)

### 5. Cleaning/Repair Agent (Hybrid Engine)

Three-tier fix strategy:

```
Issue → Tier 1: Rules Engine (fast, confidence 0.95+)
         ├─ Match → Apply
         └─ No match → Tier 2: Pattern Store (learned, confidence 0.85+)
                         ├─ Match → Apply
                         └─ No match → Tier 3: LLM (GPT-4o, confidence 0.5-0.85)
                                        └─ Store as new pattern if successful
```

Rules include: median/mode imputation, format normalization (dates, phones, emails),
boolean coercion, numeric string cleaning, whitespace trimming, fuzzy dedup.

### 6. Validation Agent
- Re-checks schema compliance on cleaned data
- Detects remaining high-null columns
- KS-test for distribution drift (ensures cleaning didn't distort data)
- Purely deterministic (no LLM)

### 7. Confidence Scoring Agent
- Aggregates per-fix confidence into dataset-level score
- Routes fixes below threshold to human review queue
- Builds the final quality report

## Conditional Routing

```
Validation → Cleaning:            if failed AND iteration < 3
Validation → Confidence Scoring:  if passed OR iteration >= 3
Confidence → Human Review:        if low-confidence fixes AND human-in-loop enabled
Confidence → Output:              otherwise
```

## Auto-Learning Pattern Store

The pattern store is the platform's core flywheel:

1. When the LLM generates a fix with confidence >= 0.7, the fix is stored as a pattern
2. Pattern = (column_name_regex, dirty_value_regex, fix_template)
3. Next time a similar issue appears, Tier 2 handles it without LLM cost
4. Human feedback adjusts success/fail counts, refining confidence over time
5. Patterns are domain-indexed for domain-specific priority

## Scalability

- **Chunking**: Datasets split into configurable chunks (default 50K rows)
- **Schema once, clean per-chunk**: Schema analysis runs on the full dataset; all other stages are chunked
- **Polars backend**: Auto-switches from Pandas to Polars for datasets > 500K rows
- **Cloud-ready**: Designed for ECS Fargate + SQS + S3 + Redis deployment

## Observability

Every agent node is decorated with `@traceable` from LangSmith:
- Custom metadata: dataset_id, chunk_id, agent_name
- LLM calls: full prompt/response/token/latency tracking
- Audit entries carry trace_id for drill-down from any fix to its reasoning chain
