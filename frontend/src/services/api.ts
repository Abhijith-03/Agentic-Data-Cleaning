import type {
  ExportResponse,
  JobStatusResponse,
  JobSummary,
  PipelineStatusResponse,
  PreviewPayload,
  ReviewResponse,
  UploadResponse,
} from '../types/api'

const JSON_HEADERS = {
  'Content-Type': 'application/json',
}

async function request<T>(input: string, init?: RequestInit): Promise<T> {
  const response = await fetch(input, init)
  if (!response.ok) {
    let detail = response.statusText
    try {
      const body = (await response.json()) as { detail?: string }
      detail = body.detail ?? detail
    } catch {
      // ignore
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export const api = {
  uploadFile: async (file: File) => {
    const formData = new FormData()
    formData.append('file', file)
    return request<UploadResponse>('/upload', {
      method: 'POST',
      body: formData,
    })
  },
  getJobStatus: (jobId: string) => request<JobStatusResponse>(`/api/jobs/${jobId}`),
  listJobs: () => request<JobSummary[]>('/api/jobs'),
  getPipelineStatus: (jobId: string) =>
    request<PipelineStatusResponse>(`/pipeline/status?job_id=${encodeURIComponent(jobId)}`),
  getPreview: (jobId: string, stage: string, page = 1, pageSize = 100) =>
    request<PreviewPayload>(
      `/data/preview?job_id=${encodeURIComponent(jobId)}&stage=${encodeURIComponent(stage)}&page=${page}&page_size=${pageSize}`,
    ),
  getProfiling: (jobId: string) =>
    request<{
      profile_report: Record<string, Record<string, unknown>>
      inferred_schema: Record<string, Record<string, unknown>>
      schema_issues: Record<string, unknown>[]
      reconstruction_report: Record<string, unknown>
    }>(`/profiling?job_id=${encodeURIComponent(jobId)}`),
  getAnomalies: (jobId: string, severity?: string, column?: string) => {
    const params = new URLSearchParams({ job_id: jobId })
    if (severity) params.set('severity', severity)
    if (column) params.set('column', column)
    return request<{ anomalies: Record<string, unknown>[]; count: number }>(`/anomalies?${params}`)
  },
  getCleaningLogs: (jobId: string, method?: string) => {
    const params = new URLSearchParams({ job_id: jobId })
    if (method) params.set('method', method)
    return request<{ cleaning_logs: Record<string, unknown>[]; count: number }>(
      `/cleaning/logs?${params}`,
    )
  },
  getAudit: (jobId: string) =>
    request<{ audit: Record<string, unknown>[]; count: number }>(
      `/audit?job_id=${encodeURIComponent(jobId)}`,
    ),
  getLlmLogs: (jobId: string) =>
    request<{ llm_logs: Record<string, unknown>[]; count: number }>(
      `/llm/logs?job_id=${encodeURIComponent(jobId)}`,
    ),
  getReviewQueue: (jobId: string) =>
    request<{ review_queue: Record<string, unknown>[]; count: number }>(
      `/review?job_id=${encodeURIComponent(jobId)}`,
    ),
  submitReview: (jobId: string, payload: { item_id: string; action: string; new_value?: string }) =>
    request<ReviewResponse>(`/review?job_id=${encodeURIComponent(jobId)}`, {
      method: 'POST',
      headers: JSON_HEADERS,
      body: JSON.stringify(payload),
    }),
  getExport: (jobId: string) =>
    request<ExportResponse>(`/export?job_id=${encodeURIComponent(jobId)}`),
}
