import type { AuditEntry } from '../../types/api'
import { Badge, Card, CardContent, CardHeader, CardTitle } from '../ui'
import { formatPercent, safeString } from '../../lib/utils'

function badgeVariant(method: string) {
  if (method.startsWith('llm:')) return 'llm'
  if (method.startsWith('pattern:')) return 'warning'
  if (method.startsWith('review:')) return 'info'
  return 'success'
}

export function AuditTimeline({ items }: { items: AuditEntry[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Audit Trail</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {items.map((item, index) => (
            <div key={`${item.row_index}-${item.column_name}-${index}`} className="flex gap-4">
              <div className="flex flex-col items-center">
                <div className="h-3 w-3 rounded-full bg-sky-500" />
                {index < items.length - 1 ? <div className="mt-2 h-full w-px bg-slate-200 dark:bg-slate-800" /> : null}
              </div>
              <div className="flex-1 rounded-2xl border border-slate-200 p-4 dark:border-slate-800">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant={badgeVariant(item.fix_method)}>{item.fix_method}</Badge>
                  <span className="text-sm font-medium">{item.agent_name}</span>
                  <span className="text-xs text-slate-500 dark:text-slate-400">
                    {item.timestamp ?? 'No timestamp'}
                  </span>
                </div>
                <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
                  Row {item.row_index}, column <span className="font-medium">{item.column_name}</span> changed from{' '}
                  <span className="font-medium">{safeString(item.original_value)}</span> to{' '}
                  <span className="font-medium">{safeString(item.new_value)}</span>.
                </p>
                <p className="mt-2 text-sm text-slate-500 dark:text-slate-400">{item.reasoning}</p>
                <div className="mt-3 flex flex-wrap gap-4 text-xs text-slate-500 dark:text-slate-400">
                  <span>Issue: {item.issue_type}</span>
                  <span>Confidence: {formatPercent(item.confidence)}</span>
                  <span>Trace ID: {item.trace_id || '—'}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
