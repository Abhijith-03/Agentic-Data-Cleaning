import { DatabaseZap, Eye, FileOutput, FileSearch, FlaskConical, LayoutDashboard, Network, ScrollText, ShieldCheck, Sparkles, UploadCloud, WandSparkles } from 'lucide-react'
import { Link, NavLink } from 'react-router-dom'

import { Badge, Button, Card, Input } from '../ui'
import { useAppStore } from '../../store/app-store'
import { cn } from '../../lib/utils'
import type { JobSummary } from '../../types/api'
import type { BadgeProps } from '../ui'

const navItems = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/upload', label: 'Upload', icon: UploadCloud },
  { to: '/pipeline', label: 'Pipeline', icon: Network },
  { to: '/data-preview', label: 'Data Preview', icon: Eye },
  { to: '/profiling', label: 'Profiling', icon: FileSearch },
  { to: '/anomalies', label: 'Anomalies', icon: FlaskConical },
  { to: '/cleaning', label: 'Cleaning', icon: WandSparkles },
  { to: '/audit', label: 'Audit Trail', icon: ScrollText },
  { to: '/llm-insights', label: 'LLM Insights', icon: Sparkles },
  { to: '/human-review', label: 'Human Review', icon: ShieldCheck },
  { to: '/export', label: 'Export', icon: FileOutput },
]

function statusVariant(status?: string): BadgeProps['variant'] {
  switch (status) {
    case 'completed':
      return 'success'
    case 'running':
      return 'info'
    case 'failed':
      return 'danger'
    default:
      return 'warning'
  }
}

export function AppShell({
  children,
  jobs,
}: {
  children: React.ReactNode
  jobs: JobSummary[]
}) {
  const selectedJobId = useAppStore((state) => state.selectedJobId)
  const setSelectedJobId = useAppStore((state) => state.setSelectedJobId)
  const dataset = useAppStore((state) => state.dataset)
  const theme = useAppStore((state) => state.theme)
  const setTheme = useAppStore((state) => state.setTheme)

  return (
    <div className="min-h-screen bg-slate-50 text-slate-950 dark:bg-slate-950 dark:text-slate-50">
      <div className="grid min-h-screen lg:grid-cols-[280px_1fr]">
        <aside className="border-r border-slate-200 bg-white/90 p-4 dark:border-slate-800 dark:bg-slate-950/80">
          <div className="mb-6 flex items-center gap-3 px-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-sky-500/15 text-sky-500">
              <DatabaseZap className="h-6 w-6" />
            </div>
            <div>
              <Link to="/" className="font-semibold tracking-tight">
                Agentic Control Center
              </Link>
              <p className="text-xs text-slate-500 dark:text-slate-400">
                Multi-agent data cleaning UI
              </p>
            </div>
          </div>

          <nav className="space-y-1">
            {navItems.map((item) => {
              const Icon = item.icon
              return (
                <NavLink
                  key={item.to}
                  to={item.to}
                  className={({ isActive }) =>
                    cn(
                      'flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium transition',
                      isActive
                        ? 'bg-sky-500/10 text-sky-600 dark:text-sky-400'
                        : 'text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-900',
                    )
                  }
                >
                  <Icon className="h-4 w-4" />
                  {item.label}
                </NavLink>
              )
            })}
          </nav>

          <Card className="mt-6 overflow-hidden">
            <div className="p-4">
              <p className="text-sm font-semibold">Current Dataset</p>
              <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
                Select a job to inspect pipeline state and outputs.
              </p>
            </div>
            <div className="border-t border-slate-200 p-4 dark:border-slate-800">
              <select
                className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-950"
                value={selectedJobId ?? ''}
                onChange={(event) => setSelectedJobId(event.target.value || null)}
              >
                <option value="">Use demo data</option>
                {jobs.map((job) => (
                  <option key={job.job_id} value={job.job_id}>
                    {job.filename || job.dataset_id} · {job.status}
                  </option>
                ))}
              </select>
              {selectedJobId ? (
                <div className="mt-3 flex items-center justify-between text-xs">
                  <span className="truncate text-slate-500 dark:text-slate-400">
                    {dataset?.dataset_id ?? selectedJobId}
                  </span>
                  <Badge variant={statusVariant(jobs.find((job) => job.job_id === selectedJobId)?.status)}>
                    {jobs.find((job) => job.job_id === selectedJobId)?.status ?? 'selected'}
                  </Badge>
                </div>
              ) : null}
            </div>
          </Card>
        </aside>

        <main className="flex min-h-screen flex-col">
          <header className="sticky top-0 z-20 border-b border-slate-200 bg-slate-50/95 px-4 py-3 backdrop-blur dark:border-slate-800 dark:bg-slate-950/90 sm:px-6">
            <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
              <div>
                <h1 className="text-lg font-semibold tracking-tight">Explainable Data Cleaning</h1>
                <p className="text-sm text-slate-500 dark:text-slate-400">
                  Observe every agent decision, review uncertain outputs, and export verified results.
                </p>
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <Input
                  value={dataset?.dataset_id ?? ''}
                  readOnly
                  className="w-[220px] bg-transparent"
                  placeholder="No dataset selected"
                />
                <Button
                  variant="outline"
                  onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
                >
                  {theme === 'dark' ? 'Light mode' : 'Dark mode'}
                </Button>
              </div>
            </div>
          </header>

          <div className="flex-1 px-4 py-6 sm:px-6">{children}</div>
        </main>
      </div>
    </div>
  )
}
