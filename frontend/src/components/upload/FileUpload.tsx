import { useMemo, useState } from 'react'
import { FileSpreadsheet, LoaderCircle, UploadCloud } from 'lucide-react'
import * as XLSX from 'xlsx'

import { Button, Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui'
import { useUploadFile } from '../../hooks/use-app-data'
import { useAppStore } from '../../store/app-store'
import { formatNumber } from '../../lib/utils'

type PreviewState = {
  fileName: string
  size: number
  rowCount: number
  columnCount: number
  columns: string[]
  rows: Record<string, unknown>[]
}

async function buildPreview(file: File): Promise<PreviewState> {
  const buffer = await file.arrayBuffer()
  const workbook = XLSX.read(buffer, { type: 'array' })
  const firstSheet = workbook.Sheets[workbook.SheetNames[0]]
  const rows = XLSX.utils.sheet_to_json<Record<string, unknown>>(firstSheet, {
    defval: '',
  })
  return {
    fileName: file.name,
    size: file.size,
    rowCount: rows.length,
    columnCount: rows.length ? Object.keys(rows[0]).length : 0,
    columns: rows.length ? Object.keys(rows[0]) : [],
    rows: rows.slice(0, 100),
  }
}

export function FileUpload() {
  const [dragActive, setDragActive] = useState(false)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [error, setError] = useState<string | null>(null)
  const uploadMutation = useUploadFile()
  const uploadPreview = useAppStore((state) => state.uploadPreview)
  const setUploadPreview = useAppStore((state) => state.setUploadPreview)

  const previewMeta = useMemo(() => {
    if (!uploadPreview) return []
    return [
      { label: 'Rows', value: formatNumber(uploadPreview.rowCount) },
      { label: 'Columns', value: formatNumber(uploadPreview.columnCount) },
      { label: 'Preview rows', value: formatNumber(uploadPreview.rows.length) },
      { label: 'Size', value: `${(uploadPreview.size / 1024).toFixed(1)} KB` },
    ]
  }, [uploadPreview])

  async function handleFile(file: File) {
    setError(null)
    setSelectedFile(file)
    try {
      const preview = await buildPreview(file)
      setUploadPreview(preview)
    } catch (previewError) {
      console.error(previewError)
      setError('Unable to preview this file. You can still try uploading it.')
      setUploadPreview(null)
    }
  }

  async function startUpload() {
    if (!selectedFile) return
    setError(null)
    try {
      await uploadMutation.mutateAsync(selectedFile)
    } catch (uploadError) {
      const message = uploadError instanceof Error ? uploadError.message : 'Upload failed'
      setError(message)
    }
  }

  return (
    <Card className="border-dashed">
      <CardHeader>
        <div>
          <CardTitle>File Upload</CardTitle>
          <CardDescription>
            Drag and drop a CSV or Excel workbook to preview the source and launch the backend pipeline.
          </CardDescription>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        <label
          htmlFor="dataset-upload"
          onDragEnter={() => setDragActive(true)}
          onDragLeave={() => setDragActive(false)}
          onDragOver={(event) => {
            event.preventDefault()
            setDragActive(true)
          }}
          onDrop={(event) => {
            event.preventDefault()
            setDragActive(false)
            const file = event.dataTransfer.files?.[0]
            if (file) void handleFile(file)
          }}
          className={`flex min-h-56 cursor-pointer flex-col items-center justify-center rounded-2xl border-2 border-dashed px-6 py-10 text-center transition ${
            dragActive
              ? 'border-sky-500 bg-sky-500/5'
              : 'border-slate-300 bg-slate-50 hover:border-sky-400 dark:border-slate-700 dark:bg-slate-950 dark:hover:border-sky-500'
          }`}
        >
          <UploadCloud className="mb-4 h-12 w-12 text-sky-500" />
          <p className="text-lg font-semibold">Drop your dataset here</p>
          <p className="mt-2 max-w-xl text-sm text-slate-500 dark:text-slate-400">
            Supported formats: CSV, TSV, XLSX, XLS, Parquet. Preview is generated locally before upload.
          </p>
          <input
            id="dataset-upload"
            type="file"
            className="hidden"
            accept=".csv,.tsv,.xlsx,.xls,.parquet"
            onChange={(event) => {
              const file = event.target.files?.[0]
              if (file) void handleFile(file)
            }}
          />
        </label>

        {uploadPreview ? (
          <div className="grid gap-3 md:grid-cols-4">
            {previewMeta.map((item) => (
              <div
                key={item.label}
                className="rounded-2xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-900"
              >
                <p className="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">
                  {item.label}
                </p>
                <p className="mt-1 text-lg font-semibold">{item.value}</p>
              </div>
            ))}
          </div>
        ) : null}

        <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-slate-200 p-4 dark:border-slate-800">
          <div className="flex items-center gap-3">
            <div className="rounded-xl bg-sky-500/10 p-2 text-sky-500">
              <FileSpreadsheet className="h-5 w-5" />
            </div>
            <div>
              <p className="text-sm font-medium">{selectedFile?.name ?? 'No file selected yet'}</p>
              <p className="text-xs text-slate-500 dark:text-slate-400">
                Upload launches the full agent pipeline and starts live polling.
              </p>
            </div>
          </div>

          <Button onClick={() => void startUpload()} disabled={!selectedFile || uploadMutation.isPending}>
            {uploadMutation.isPending ? (
              <>
                <LoaderCircle className="h-4 w-4 animate-spin" />
                Uploading...
              </>
            ) : (
              'Start Cleaning'
            )}
          </Button>
        </div>

        {error ? (
          <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-300">
            {error}
          </div>
        ) : null}
      </CardContent>
    </Card>
  )
}
