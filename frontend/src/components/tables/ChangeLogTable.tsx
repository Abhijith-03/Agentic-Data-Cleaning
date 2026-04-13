import type { CleaningAction } from '../../types/api'
import { Badge, Card, CardContent, CardHeader, CardTitle } from '../ui'
import type { BadgeProps } from '../ui'
import { formatPercent, safeString } from '../../lib/utils'

function methodVariant(rule: string): BadgeProps['variant'] {
  if (rule.startsWith('llm:')) return 'llm'
  if (rule.startsWith('pattern:')) return 'warning'
  return 'success'
}

export function ChangeLogTable({ actions }: { actions: CleaningAction[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Cleaning Actions</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto rounded-2xl border border-slate-200 dark:border-slate-800">
          <table className="min-w-full text-left text-sm">
            <thead className="bg-slate-100/70 dark:bg-slate-900">
              <tr>
                <th className="px-4 py-3">Row</th>
                <th className="px-4 py-3">Column</th>
                <th className="px-4 py-3">Before</th>
                <th className="px-4 py-3">After</th>
                <th className="px-4 py-3">Method</th>
                <th className="px-4 py-3">Confidence</th>
                <th className="px-4 py-3">Reasoning</th>
              </tr>
            </thead>
            <tbody>
              {actions.map((action, index) => (
                <tr key={`${action.row}-${action.column}-${index}`} className="border-t border-slate-200 dark:border-slate-800">
                  <td className="px-4 py-3">{action.row}</td>
                  <td className="px-4 py-3 font-medium">{action.column}</td>
                  <td className="px-4 py-3">{safeString(action.old_value)}</td>
                  <td className="px-4 py-3">{safeString(action.new_value)}</td>
                  <td className="px-4 py-3">
                    <Badge variant={methodVariant(action.rule)}>{action.rule}</Badge>
                  </td>
                  <td className="px-4 py-3">{formatPercent(action.confidence)}</td>
                  <td className="max-w-[320px] px-4 py-3">
                    <div className="truncate" title={action.reasoning}>
                      {safeString(action.reasoning)}
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
