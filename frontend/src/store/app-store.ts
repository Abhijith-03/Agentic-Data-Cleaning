import { create } from 'zustand'

import type {
  AuditEntry,
  CleaningAction,
  JobResult,
  JobStatusResponse,
  LlmLogEntry,
  PipelineStage,
  PreviewPayload,
  ReviewItem,
  UploadResponse,
  AnomalyItem,
} from '../types/api'

type ThemeMode = 'dark' | 'light'

interface UploadPreviewState {
  fileName: string
  size: number
  rowCount: number
  columnCount: number
  columns: string[]
  rows: Record<string, unknown>[]
}

interface AppState {
  theme: ThemeMode
  selectedJobId: string | null
  selectedStage: string | null
  latestUpload: UploadResponse | null
  uploadPreview: UploadPreviewState | null
  dataset: JobResult | null
  pipelineStatus: Record<string, PipelineStage>
  anomalies: AnomalyItem[]
  cleaningLogs: CleaningAction[]
  auditLogs: AuditEntry[]
  llmLogs: LlmLogEntry[]
  reviewQueue: ReviewItem[]
  rawPreview: PreviewPayload | null
  cleanedPreview: PreviewPayload | null
  setTheme: (theme: ThemeMode) => void
  setSelectedJobId: (jobId: string | null) => void
  setSelectedStage: (stage: string | null) => void
  setLatestUpload: (upload: UploadResponse | null) => void
  setUploadPreview: (preview: UploadPreviewState | null) => void
  hydrateFromJobStatus: (status: JobStatusResponse | null) => void
  hydrateFromJobResult: (result: JobResult | null) => void
}

export const useAppStore = create<AppState>((set) => ({
  theme: 'dark',
  selectedJobId: null,
  selectedStage: 'ingest',
  latestUpload: null,
  uploadPreview: null,
  dataset: null,
  pipelineStatus: {},
  anomalies: [],
  cleaningLogs: [],
  auditLogs: [],
  llmLogs: [],
  reviewQueue: [],
  rawPreview: null,
  cleanedPreview: null,
  setTheme: (theme) => set({ theme }),
  setSelectedJobId: (selectedJobId) => set({ selectedJobId }),
  setSelectedStage: (selectedStage) => set({ selectedStage }),
  setLatestUpload: (latestUpload) => set({ latestUpload }),
  setUploadPreview: (uploadPreview) => set({ uploadPreview }),
  hydrateFromJobStatus: (status) =>
    set((state) => ({
      pipelineStatus: status?.result?.pipeline_stages ?? status?.result?.pipeline_stages ?? state.pipelineStatus,
    })),
  hydrateFromJobResult: (dataset) =>
    set({
      dataset,
      pipelineStatus: dataset?.pipeline_stages ?? {},
      anomalies: dataset?.anomalies ?? [],
      cleaningLogs: dataset?.cleaning_actions ?? [],
      auditLogs: dataset?.audit_log ?? [],
      llmLogs: dataset?.llm_logs ?? [],
      reviewQueue: dataset?.review_queue ?? [],
      rawPreview: dataset?.raw_preview ?? null,
      cleanedPreview: dataset?.cleaned_preview ?? null,
    }),
}))
