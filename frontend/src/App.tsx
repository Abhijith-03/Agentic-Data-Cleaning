import { useEffect, useState } from 'react'
import { Download, ExternalLink, RefreshCw, Sparkles } from 'lucide-react'
import { Navigate, Route, Routes } from 'react-router-dom'

import { AuditTimeline } from './components/audit/AuditTimeline'
import { ConfidenceBreakdownChart } from './components/charts/ConfidenceBreakdownChart'
import { HistogramChart } from './components/charts/HistogramChart'
import { NullHeatmap } from './components/charts/NullHeatmap'
import { DataDiffViewer } from './components/DataDiffViewer'
import { AppShell } from './components/layout/AppShell'
import { PipelineGraph } from './components/pipeline/PipelineGraph'
import { AnomalyTable } from './components/tables/AnomalyTable'
import { ChangeLogTable } from './components/tables/ChangeLogTable'
import { DataPreviewTable } from './components/tables/DataPreviewTable'
import { ReviewQueueTable } from './components/tables/ReviewQueueTable'
import { FileUpload } from './components/upload/FileUpload'
import { Badge, Button, Card, CardContent, CardDescription, CardHeader, CardTitle, SectionHeader } from './components/ui'
import {
  useAnomalies,
  useAudit,
  useCleaningLogs,
  useExport,
  useJobStatus,
  useJobs,
  useLlmLogs,
  usePipelineStatus,
  usePreview,
  useProfiling,
  useResolvedJobResult,
  useReviewQueue,
  useSubmitReview,
} from './hooks/use-app-data'
import { formatNumber, formatPercent, titleCase } from './lib/utils'
import { useAppStore } from './store/app-store'
import type { AnomalyItem, AuditEntry, CleaningAction, LlmLogEntry, PreviewPayload, ProfileColumn, ReviewItem } from './types/api'

function useCurrentDataset() {
  const selectedJobId = useAppStore((state) => state.selectedJobId)
  return {
    selectedJobId,
    dataset: useResolvedJobResult(selectedJobId),
  }
}

function DashboardPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const pipelineStatus = usePipelineStatus(selectedJobId)
  const stages = pipelineStatus.data?.pipeline_stages ?? dataset.pipeline_stages
  const completedStages = Object.values(stages).filter((stage) => stage.status === 'success').length

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Dashboard"
        description="High-level health, confidence, and operational state for the current dataset."
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {[
          ['Rows processed', formatNumber(dataset.total_rows)],
          ['Issues detected', formatNumber(dataset.issues_detected)],
          ['Fixes applied', formatNumber(dataset.fixes_applied)],
          ['Overall confidence', formatPercent(dataset.overall_confidence)],
        ].map(([label, value]) => (
          <Card key={label}>
            <CardContent className="p-5">
              <p className="text-sm text-slate-500 dark:text-slate-400">{label}</p>
              <p className="mt-3 text-3xl font-semibold tracking-tight">{value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
        <ConfidenceBreakdownChart breakdown={dataset.fix_breakdown} />
        <Card>
          <CardHeader>
            <CardTitle>Pipeline Snapshot</CardTitle>
            <CardDescription>Summary of the live execution graph exposed by the backend.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="rounded-2xl bg-slate-100 p-4 dark:bg-slate-900">
                <p className="text-xs uppercase text-slate-400">Completed stages</p>
                <p className="mt-2 text-2xl font-semibold">{completedStages}</p>
              </div>
              <div className="rounded-2xl bg-slate-100 p-4 dark:bg-slate-900">
                <p className="text-xs uppercase text-slate-400">Validation</p>
                <p className="mt-2 text-2xl font-semibold">
                  {dataset.validation_passed ? 'Pass' : 'Review required'}
                </p>
              </div>
            </div>
            <div className="space-y-3">
              {Object.values(stages).map((stage) => (
                <div key={stage.name} className="flex items-center justify-between rounded-xl border border-slate-200 px-4 py-3 dark:border-slate-800">
                  <div>
                    <p className="font-medium">{titleCase(stage.name)}</p>
                    <p className="text-xs text-slate-500 dark:text-slate-400">{stage.duration_ms?.toFixed(0)} ms</p>
                  </div>
                  <Badge variant={stage.status === 'success' ? 'success' : stage.status === 'partial' ? 'warning' : 'neutral'}>
                    {stage.status}
                  </Badge>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function UploadPage() {
  const uploadPreview = useAppStore((state) => state.uploadPreview)

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Upload"
        description="Preview incoming data locally, then send it to the FastAPI backend for cleaning."
      />
      <FileUpload />
      {uploadPreview ? (
        <DataPreviewTable
          title="Source Preview"
          description="First 100 rows parsed in the browser before upload."
          rows={uploadPreview.rows}
        />
      ) : null}
    </div>
  )
}

function PipelinePage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const selectedStage = useAppStore((state) => state.selectedStage)
  const setSelectedStage = useAppStore((state) => state.setSelectedStage)
  const pipelineStatus = usePipelineStatus(selectedJobId)
  const stageName = selectedStage ?? 'ingest'
  const stagePreviewQuery = usePreview(selectedJobId, stageName, 1, 25)
  const stages = pipelineStatus.data?.pipeline_stages ?? dataset.pipeline_stages
  const fallbackPreview = dataset.stage_previews[stageName] ?? dataset.cleaned_preview
  const previewRows = (stagePreviewQuery.data?.rows ?? fallbackPreview.rows) as Record<string, unknown>[]

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Pipeline"
        description="Visualize node-by-node execution and inspect backend-generated stage output previews."
        action={
          <Button variant="outline" onClick={() => pipelineStatus.refetch()}>
            <RefreshCw className="h-4 w-4" />
            Refresh status
          </Button>
        }
      />
      <PipelineGraph stages={stages} selectedStage={stageName} onSelectStage={setSelectedStage} />
      <DataPreviewTable
        title={`${titleCase(stageName)} Preview`}
        description="Rows sampled from the selected stage."
        rows={previewRows}
      />
    </div>
  )
}

function DataPreviewPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const [tab, setTab] = useState<'raw' | 'cleaned'>('cleaned')
  const previewQuery = usePreview(selectedJobId, tab, 1, 100)
  const preview = (previewQuery.data ?? (tab === 'raw' ? dataset.raw_preview : dataset.cleaned_preview)) as PreviewPayload

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Data Preview"
        description="Inspect source and cleaned rows with search, sorting, and null highlighting."
      />
      <div className="flex gap-2">
        <Button variant={tab === 'raw' ? 'default' : 'outline'} onClick={() => setTab('raw')}>
          Original
        </Button>
        <Button variant={tab === 'cleaned' ? 'default' : 'outline'} onClick={() => setTab('cleaned')}>
          Cleaned
        </Button>
      </div>
      <DataPreviewTable
        title={tab === 'raw' ? 'Original Data' : 'Cleaned Data'}
        description={`Showing ${preview.rows.length} rows returned by the backend for the ${tab} dataset.`}
        rows={preview.rows as Record<string, unknown>[]}
      />
    </div>
  )
}

function ProfilingPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const profilingQuery = useProfiling(selectedJobId)
  const profileReport = (profilingQuery.data?.profile_report ?? dataset.profile_report) as Record<string, ProfileColumn>
  const columns = Object.values(profileReport)
  const selectedColumn = columns[0]
  const histogramData = selectedColumn?.top_values
    ? Object.entries(selectedColumn.top_values).map(([label, value]) => ({ label, value }))
    : [
        { label: 'Min', value: selectedColumn?.min ?? 0 },
        { label: 'Median', value: selectedColumn?.median ?? 0 },
        { label: 'Max', value: selectedColumn?.max ?? 0 },
      ]

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Profiling"
        description="Column-level null rate, uniqueness, and summary statistics generated by the profiling agent."
      />
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {columns.map((column) => (
          <Card key={column.column}>
            <CardHeader>
              <CardTitle>{column.column}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex justify-between"><span>Null %</span><span>{formatPercent(column.null_pct, 1)}</span></div>
              <div className="flex justify-between"><span>Unique %</span><span>{formatPercent(column.unique_pct, 1)}</span></div>
              <div className="flex justify-between"><span>Mean</span><span>{formatNumber(column.mean)}</span></div>
              <div className="flex justify-between"><span>Median</span><span>{formatNumber(column.median)}</span></div>
              <div className="flex justify-between"><span>Std</span><span>{formatNumber(column.std)}</span></div>
            </CardContent>
          </Card>
        ))}
      </div>
      <div className="grid gap-6 xl:grid-cols-2">
        <HistogramChart title={`Distribution · ${selectedColumn?.column ?? 'Column'}`} data={histogramData} />
        <NullHeatmap columns={columns} />
      </div>
    </div>
  )
}

function AnomaliesPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const anomaliesQuery = useAnomalies(selectedJobId)
  const anomalies = (anomaliesQuery.data?.anomalies ?? dataset.anomalies) as AnomalyItem[]
  return (
    <div className="space-y-6">
      <SectionHeader
        title="Anomalies"
        description="Filter by severity and column to inspect flagged records before or after cleaning."
      />
      <AnomalyTable anomalies={anomalies} />
    </div>
  )
}

function CleaningPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const cleaningQuery = useCleaningLogs(selectedJobId)
  const cleaningActions = (cleaningQuery.data?.cleaning_logs ?? dataset.cleaning_actions) as CleaningAction[]
  return (
    <div className="space-y-6">
      <SectionHeader
        title="Cleaning"
        description="Every fix with method, confidence, before/after values, and a quick diff viewer."
      />
      <ChangeLogTable actions={cleaningActions} />
      <DataDiffViewer
        original={dataset.raw_preview.rows as Record<string, unknown>[]}
        cleaned={dataset.cleaned_preview.rows as Record<string, unknown>[]}
      />
    </div>
  )
}

function AuditPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const auditQuery = useAudit(selectedJobId)
  const auditItems = (auditQuery.data?.audit ?? dataset.audit_log) as AuditEntry[]
  return (
    <div className="space-y-6">
      <SectionHeader
        title="Audit Trail"
        description="A timeline of agent actions designed to explain system decisions to operators."
      />
      <AuditTimeline items={auditItems} />
    </div>
  )
}

function LLMInsightsPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const llmQuery = useLlmLogs(selectedJobId)
  const llmLogs = (llmQuery.data?.llm_logs ?? dataset.llm_logs) as LlmLogEntry[]
  const [openPrompts, setOpenPrompts] = useState<Record<number, boolean>>({})

  return (
    <div className="space-y-6">
      <SectionHeader
        title="LLM Insights"
        description="Inspect the exact rows where the LLM participated, its structured output, and prompt context."
      />
      <div className="grid gap-4">
        {llmLogs.map((entry, index) => (
          <Card key={`${entry.row}-${entry.column}-${index}`}>
            <CardHeader>
              <div className="flex items-center gap-2">
                <Sparkles className="h-4 w-4 text-violet-500" />
                <CardTitle>
                  Row {entry.row} · {entry.column}
                </CardTitle>
              </div>
              <CardDescription>
                Model {entry.model ?? 'unknown'} · Confidence {formatPercent(entry.confidence)}
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <div className="grid gap-3 md:grid-cols-3">
                <div><span className="font-medium">Old:</span> {String(entry.old_value ?? '—')}</div>
                <div><span className="font-medium">New:</span> {String(entry.new_value ?? '—')}</div>
                <div><span className="font-medium">Issue:</span> {entry.issue_type ?? '—'}</div>
              </div>
              <pre className="overflow-x-auto rounded-2xl bg-slate-950 p-4 text-xs text-slate-100">
                {JSON.stringify(entry.structured_output ?? {}, null, 2)}
              </pre>
              <Button
                variant="outline"
                size="sm"
                onClick={() =>
                  setOpenPrompts((current) => ({ ...current, [index]: !current[index] }))
                }
              >
                {openPrompts[index] ? 'Hide Prompt' : 'Show Prompt'}
              </Button>
              {openPrompts[index] ? (
                <pre className="overflow-x-auto rounded-2xl bg-slate-900 p-4 text-xs text-slate-100">
                  {entry.prompt ?? 'No prompt captured'}
                </pre>
              ) : null}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}

function HumanReviewPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const reviewQuery = useReviewQueue(selectedJobId)
  const submitReview = useSubmitReview(selectedJobId)
  const items = (reviewQuery.data?.review_queue ?? dataset.review_queue) as ReviewItem[]

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Human Review"
        description="Mocked review flow backed by FastAPI endpoints so the React app can exercise the operator workflow today."
      />
      <ReviewQueueTable
        items={items}
        isSubmitting={submitReview.isPending}
        onAction={(payload) => submitReview.mutate(payload)}
      />
    </div>
  )
}

function ExportPage() {
  const { selectedJobId, dataset } = useCurrentDataset()
  const exportQuery = useExport(selectedJobId)
  const exportInfo =
    exportQuery.data ??
    (selectedJobId
      ? null
      : {
          dataset_id: dataset.dataset_id,
          downloads: {
            cleaned_data: '/api/download/demo',
            audit_report: '/api/download/demo/audit',
          },
          api_endpoints: {
            status: '/pipeline/status?job_id=demo',
            preview: '/data/preview?job_id=demo&stage=cleaned',
          },
        })

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Export"
        description="Download the cleaned dataset, audit report, and inspect the API contract powering the UI."
      />
      <div className="grid gap-6 xl:grid-cols-[0.8fr_1.2fr]">
        <Card>
          <CardHeader>
            <CardTitle>Downloads</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <Button asChild className="w-full justify-between">
              <a href={exportInfo?.downloads.cleaned_data ?? '#'} target="_blank" rel="noreferrer">
                Cleaned dataset
                <Download className="h-4 w-4" />
              </a>
            </Button>
            <Button asChild variant="outline" className="w-full justify-between">
              <a href={exportInfo?.downloads.audit_report ?? '#'} target="_blank" rel="noreferrer">
                Audit report
                <ExternalLink className="h-4 w-4" />
              </a>
            </Button>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>API Endpoint Info</CardTitle>
            <CardDescription>These URLs are ready for React Query hooks and external integrations.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {Object.entries(exportInfo?.api_endpoints ?? {}).map(([key, value]) => (
              <div key={key} className="rounded-xl bg-slate-100 px-4 py-3 font-mono text-xs dark:bg-slate-900">
                <span className="mr-2 font-sans font-medium text-slate-500 dark:text-slate-400">{titleCase(key)}:</span>
                {value}
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

export default function App() {
  const jobsQuery = useJobs()
  const selectedJobId = useAppStore((state) => state.selectedJobId)
  const theme = useAppStore((state) => state.theme)

  useJobStatus(selectedJobId)

  useEffect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark')
  }, [theme])

  return (
    <AppShell jobs={jobsQuery.data ?? []}>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/upload" element={<UploadPage />} />
        <Route path="/pipeline" element={<PipelinePage />} />
        <Route path="/data-preview" element={<DataPreviewPage />} />
        <Route path="/profiling" element={<ProfilingPage />} />
        <Route path="/anomalies" element={<AnomaliesPage />} />
        <Route path="/cleaning" element={<CleaningPage />} />
        <Route path="/audit" element={<AuditPage />} />
        <Route path="/llm-insights" element={<LLMInsightsPage />} />
        <Route path="/human-review" element={<HumanReviewPage />} />
        <Route path="/export" element={<ExportPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  )
}
