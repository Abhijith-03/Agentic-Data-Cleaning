import { useMemo, useState } from 'react'

import type { AnomalyItem } from '../../types/api'
import { Badge, Card, CardContent, CardHeader, CardTitle } from '../ui'
import type { BadgeProps } from '../ui'
import { safeString } from '../../lib/utils'

function badgeVariant(severity: string): BadgeProps['variant'] {
  switch (severity) {
    case 'critical':
      return 'danger'
    case 'warning':
      return 'warning'
    default:
      return 'info'
  }
}

export function AnomalyTable({ anomalies }: { anomalies: AnomalyItem[] }) {
  const [severity, setSeverity] = useState('all')
  const [column, setColumn] = useState('')

  const columns = useMemo(
    () => Array.from(new Set(anomalies.map((item) => item.column))).sort(),
    [anomalies],
  )

  const filtered = useMemo(
    () =>
      anomalies.filter((item) => {
        const severityMatch = severity === 'all' || item.severity === severity
        const columnMatch = !column || item.column === column
        return severityMatch && columnMatch
      }),
    [anomalies, column, severity],
  )

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <CardTitle>Anomaly Detection Panel</CardTitle>
          <div className="flex flex-wrap gap-2">
            <select
              className="h-10 rounded-xl border border-slate-300 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-950"
              value={severity}
              onChange={(event) => setSeverity(event.target.value)}
            >
              <option value="all">All severities</option>
              <option value="critical">Critical</option>
              <option value="warning">Warning</option>
              <option value="info">Info</option>
            </select>
            <select
              className="h-10 rounded-xl border border-slate-300 bg-white px-3 text-sm dark:border-slate-700 dark:bg-slate-950"
              value={column}
              onChange={(event) => setColumn(event.target.value)}
            >
              <option value="">All columns</option>
              {columns.map((columnOption) => (
                <option key={columnOption} value={columnOption}>
                  {columnOption}
                </option>
              ))}
            </select>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto rounded-2xl border border-slate-200 dark:border-slate-800">
          <table className="min-w-full text-left text-sm">
            <thead className="bg-slate-100/70 dark:bg-slate-900">
              <tr>
                <th className="px-4 py-3">Severity</th>
                <th className="px-4 py-3">Row</th>
                <th className="px-4 py-3">Column</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3">Value</th>
                <th className="px-4 py-3">Details</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((item, index) => (
                <tr key={`${item.row}-${item.column}-${index}`} className="border-t border-slate-200 dark:border-slate-800">
                  <td className="px-4 py-3">
                    <Badge variant={badgeVariant(item.severity)}>{item.severity}</Badge>
                  </td>
                  <td className="px-4 py-3">{item.row}</td>
                  <td className="px-4 py-3 font-medium">{item.column}</td>
                  <td className="px-4 py-3">{item.anomaly_type}</td>
                  <td className="px-4 py-3">{safeString(item.value)}</td>
                  <td className="max-w-[320px] px-4 py-3">
                    <div className="truncate" title={item.details}>
                      {safeString(item.details)}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
}
