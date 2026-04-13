import { useEffect } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { api } from '../services/api'
import { mockJobResult } from '../services/mock-data'
import { useAppStore } from '../store/app-store'
import type { JobResult, JobStatusResponse } from '../types/api'

export function useJobs() {
  return useQuery({
    queryKey: ['jobs'],
    queryFn: api.listJobs,
    refetchInterval: 5000,
  })
}

export function useJobStatus(jobId: string | null) {
  const hydrateFromJobResult = useAppStore((state) => state.hydrateFromJobResult)

  const query = useQuery({
    queryKey: ['job-status', jobId],
    queryFn: () => api.getJobStatus(jobId as string),
    enabled: Boolean(jobId),
    refetchInterval: (query) => {
      const data = query.state.data as JobStatusResponse | undefined
      if (!data) return 2500
      return data.status === 'completed' || data.status === 'failed' ? false : 2500
    },
  })

  useEffect(() => {
    if (query.data?.result) {
      hydrateFromJobResult(query.data.result)
    }
  }, [hydrateFromJobResult, query.data])

  return query
}

export function usePipelineStatus(jobId: string | null) {
  return useQuery({
    queryKey: ['pipeline-status', jobId],
    queryFn: () => api.getPipelineStatus(jobId as string),
    enabled: Boolean(jobId),
    refetchInterval: 2500,
  })
}

export function usePreview(jobId: string | null, stage: string, page = 1, pageSize = 100) {
  return useQuery({
    queryKey: ['preview', jobId, stage, page, pageSize],
    queryFn: () => api.getPreview(jobId as string, stage, page, pageSize),
    enabled: Boolean(jobId),
  })
}

export function useProfiling(jobId: string | null) {
  return useQuery({
    queryKey: ['profiling', jobId],
    queryFn: () => api.getProfiling(jobId as string),
    enabled: Boolean(jobId),
  })
}

export function useAnomalies(jobId: string | null, severity?: string, column?: string) {
  return useQuery({
    queryKey: ['anomalies', jobId, severity, column],
    queryFn: () => api.getAnomalies(jobId as string, severity, column),
    enabled: Boolean(jobId),
  })
}

export function useCleaningLogs(jobId: string | null, method?: string) {
  return useQuery({
    queryKey: ['cleaning-logs', jobId, method],
    queryFn: () => api.getCleaningLogs(jobId as string, method),
    enabled: Boolean(jobId),
  })
}

export function useAudit(jobId: string | null) {
  return useQuery({
    queryKey: ['audit', jobId],
    queryFn: () => api.getAudit(jobId as string),
    enabled: Boolean(jobId),
  })
}

export function useLlmLogs(jobId: string | null) {
  return useQuery({
    queryKey: ['llm-logs', jobId],
    queryFn: () => api.getLlmLogs(jobId as string),
    enabled: Boolean(jobId),
  })
}

export function useReviewQueue(jobId: string | null) {
  return useQuery({
    queryKey: ['review-queue', jobId],
    queryFn: () => api.getReviewQueue(jobId as string),
    enabled: Boolean(jobId),
  })
}

export function useExport(jobId: string | null) {
  return useQuery({
    queryKey: ['export', jobId],
    queryFn: () => api.getExport(jobId as string),
    enabled: Boolean(jobId),
  })
}

export function useUploadFile() {
  const queryClient = useQueryClient()
  const setLatestUpload = useAppStore((state) => state.setLatestUpload)
  const setSelectedJobId = useAppStore((state) => state.setSelectedJobId)

  return useMutation({
    mutationFn: api.uploadFile,
    onSuccess: (data) => {
      setLatestUpload(data)
      setSelectedJobId(data.job_id)
      queryClient.invalidateQueries({ queryKey: ['jobs'] })
    },
  })
}

export function useSubmitReview(jobId: string | null) {
  const queryClient = useQueryClient()
  const hydrateFromJobResult = useAppStore((state) => state.hydrateFromJobResult)

  return useMutation({
    mutationFn: (payload: { item_id: string; action: string; new_value?: string }) =>
      api.submitReview(jobId as string, payload),
    onSuccess: async () => {
      if (!jobId) return
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['review-queue', jobId] }),
        queryClient.invalidateQueries({ queryKey: ['audit', jobId] }),
        queryClient.invalidateQueries({ queryKey: ['job-status', jobId] }),
      ])
      const status = await queryClient.fetchQuery({
        queryKey: ['job-status', jobId],
        queryFn: () => api.getJobStatus(jobId),
      })
      hydrateFromJobResult(status.result ?? null)
    },
  })
}

export function useResolvedJobResult(jobId: string | null): JobResult {
  const dataset = useAppStore((state) => state.dataset)
  if (jobId && dataset) return dataset
  return mockJobResult
}
