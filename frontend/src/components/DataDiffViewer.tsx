import { Card, CardContent, CardHeader, CardTitle } from './ui'
import { safeString } from '../lib/utils'

export function DataDiffViewer({
  original,
  cleaned,
}: {
  original: Record<string, unknown>[]
  cleaned: Record<string, unknown>[]
}) {
  const base = original[0] ?? {}
  const next = cleaned[0] ?? {}
  const columns = Array.from(new Set([...Object.keys(base), ...Object.keys(next)]))

  return (
    <Card>
      <CardHeader>
        <CardTitle>Data Diff Viewer</CardTitle>
      </CardHeader>
      <CardContent className="grid gap-4 lg:grid-cols-2">
        {[{ title: 'Original', row: base }, { title: 'Cleaned', row: next }].map((panel) => (
          <div key={panel.title} className="rounded-2xl border border-slate-200 dark:border-slate-800">
            <div className="border-b border-slate-200 px-4 py-3 font-medium dark:border-slate-800">
              {panel.title}
            </div>
            <div className="divide-y divide-slate-200 dark:divide-slate-800">
              {columns.map((column) => {
                const value = panel.row[column]
                const changed = safeString(base[column]) !== safeString(next[column])
                return (
                  <div
                    key={column}
                    className={`grid grid-cols-[140px_1fr] gap-3 px-4 py-3 text-sm ${
                      changed ? 'bg-sky-500/5' : ''
                    }`}
                  >
                    <span className="font-medium">{column}</span>
                    <span>{safeString(value)}</span>
                  </div>
                )
              })}
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}
