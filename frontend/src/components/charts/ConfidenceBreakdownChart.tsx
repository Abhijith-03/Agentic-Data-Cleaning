import { Pie, PieChart, ResponsiveContainer, Tooltip, Cell } from 'recharts'

import { Card, CardContent, CardHeader, CardTitle } from '../ui'
import { titleCase } from '../../lib/utils'

const COLORS = ['#0ea5e9', '#a855f7', '#10b981', '#f59e0b', '#f43f5e']

export function ConfidenceBreakdownChart({
  breakdown,
}: {
  breakdown: Record<string, number>
}) {
  const data = Object.entries(breakdown).map(([key, value]) => ({
    name: titleCase(key),
    value,
  }))

  return (
    <Card>
      <CardHeader>
        <CardTitle>Confidence Breakdown</CardTitle>
      </CardHeader>
      <CardContent className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={data} dataKey="value" nameKey="name" innerRadius={65} outerRadius={105}>
              {data.map((entry, index) => (
                <Cell key={entry.name} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
