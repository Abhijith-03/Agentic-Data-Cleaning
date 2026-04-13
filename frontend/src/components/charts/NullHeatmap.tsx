import type { ProfileColumn } from '../../types/api'
import { Card, CardContent, CardHeader, CardTitle } from '../ui'
import { formatPercent } from '../../lib/utils'

function heatColor(nullPct = 0) {
  if (nullPct > 50) return 'bg-rose-500/20 text-rose-300 border-rose-500/30'
  if (nullPct > 20) return 'bg-amber-500/20 text-amber-300 border-amber-500/30'
  return 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30'
}

export function NullHeatmap({
  columns,
}: {
  columns: ProfileColumn[]
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Null Heatmap</CardTitle>
      </CardHeader>
      <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {columns.map((column) => (
          <div
            key={column.column}
            className={`rounded-2xl border p-4 ${heatColor(column.null_pct)}`}
          >
            <p className="text-sm font-medium">{column.column}</p>
            <p className="mt-2 text-2xl font-semibold">{formatPercent(column.null_pct, 1)}</p>
            <p className="text-xs opacity-80">Null values detected in this column</p>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}
