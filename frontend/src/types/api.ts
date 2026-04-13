export type JobStatus = 'pending' | 'running' | 'completed' | 'failed'

export type PipelineStageName =
  | 'ingest'
  | 'reconstruction_schema_planner'
  | 'structure_reconstruction'
  | 'schema_analysis'
  | 'data_profiling'
  | 'anomaly_detection'
  | 'cleaning'
  | 'validation'
  | 'confidence_scoring'
  | 'human_review'
  | 'output'

export interface PipelineStage {
  name: string
  status: 'success' | 'partial' | 'failed' | 'idle'
  started_at?: number
  completed_at?: number
  duration_ms?: number
  chunk_runs?: number
  confidence_score?: number | null
  summary?: Record<string, unknown>
}

export interface PreviewPayload {
  rows: Record<string, unknown>[]
  row_count?: number
  total_rows?: number
  column_names: string[]
  truncated?: boolean
  page?: number
  page_size?: number
  total_pages?: number
}

export interface ReconstructionReport {
  rows_parsed: number
  rows_dropped: number
  duplicates_removed: number
  output_rows?: number
  skipped?: boolean
  reason?: string
}

export interface AnomalyItem {
  row: number
  column: string
  value?: string
  anomaly_type: string
  severity: 'critical' | 'warning' | 'info'
  details?: string
}

export interface CleaningAction {
  row: number
  column: string
  old_value: unknown
  new_value: unknown
  rule: string
  confidence: number
  reasoning: string
  issue_type: string
  metadata?: Record<string, unknown>
}

export interface AuditEntry {
  timestamp?: string
  row_index: number
  column_name: string
  original_value: unknown
  new_value: unknown
  issue_type: string
  fix_method: string
  confidence: number
  reasoning: string
  agent_name: string
  tier?: string
  trace_id?: string
}

export interface LlmLogEntry {
  row: number
  column: string
  model?: string
  prompt?: string
  structured_output?: Record<string, unknown>
  confidence?: number
  reasoning?: string
  old_value?: unknown
  new_value?: unknown
  issue_type?: string
}

export interface ReviewItem {
  id: string
  status: 'pending' | 'accepted' | 'rejected' | 'edited'
  row: number
  column: string
  old_value: unknown
  suggested_value: unknown
  reviewed_value?: unknown
  confidence: number
  reasoning: string
  issue_type: string
  fix_method: string
  reviewed_at?: string
}

export interface ExportResponse {
  dataset_id: string
  downloads: {
    cleaned_data: string
    audit_report: string
  }
  api_endpoints: Record<string, string>
}

export interface ProfileColumn {
  column: string
  total?: number
  null_count?: number
  null_pct?: number
  unique_count?: number
  unique_pct?: number
  is_numeric?: boolean
  mean?: number
  median?: number
  std?: number
  min?: number
  max?: number
  q1?: number
  q3?: number
  top_values?: Record<string, number>
  min_length?: number
  max_length?: number
  mean_length?: number
}

export interface PipelineStatusResponse {
  job_id: string
  status: JobStatus
  progress?: string
  dataset_id: string
  pipeline_stages: Record<string, PipelineStage>
  overall_confidence: number
  validation_passed: boolean
  duration_seconds: number
}

export interface JobSummary {
  job_id: string
  status: JobStatus
  dataset_id: string
  filename?: string
  created_at: number
}

export interface JobResult {
  dataset_id: string
  total_rows: number
  issues_detected: number
  fixes_applied: number
  overall_confidence: number
  validation_passed: boolean
  duration_seconds: number
  fix_breakdown: Record<string, number>
  iterations: number
  output_path: string
  pipeline_stages: Record<string, PipelineStage>
  stage_previews: Record<string, PreviewPayload>
  raw_preview: PreviewPayload
  cleaned_preview: PreviewPayload
  reconstruction_report: ReconstructionReport
  inferred_schema: Record<string, Record<string, unknown>>
  schema_issues: Record<string, unknown>[]
  profile_report: Record<string, ProfileColumn>
  anomalies: AnomalyItem[]
  audit_log: AuditEntry[]
  cleaning_actions: CleaningAction[]
  llm_logs: LlmLogEntry[]
  review_queue: ReviewItem[]
  low_confidence_fixes: CleaningAction[]
  cleaned_records: Record<string, unknown>[]
  chunk_results: Record<string, unknown>[]
}

export interface JobStatusResponse {
  job_id: string
  status: JobStatus
  progress?: string
  result?: JobResult
  error?: string
  created_at?: number
  completed_at?: number
}

export interface UploadResponse {
  job_id: string
  dataset_id: string
  status: JobStatus
}

export interface ReviewResponse {
  review_item: ReviewItem
  review_queue: ReviewItem[]
}
