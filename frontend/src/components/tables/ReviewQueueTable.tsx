import { useState } from 'react'

import type { ReviewItem } from '../../types/api'
import { Badge, Button, Card, CardContent, CardHeader, CardTitle, Input } from '../ui'
import { formatPercent, safeString } from '../../lib/utils'

export function ReviewQueueTable({
  items,
  onAction,
  isSubmitting,
}: {
  items: ReviewItem[]
  onAction: (payload: { item_id: string; action: 'accept' | 'reject' | 'edit'; new_value?: string }) => void
  isSubmitting: boolean
}) {
  const [drafts, setDrafts] = useState<Record<string, string>>({})

  return (
    <Card>
      <CardHeader>
        <CardTitle>Mock Human Review Queue</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {items.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-slate-300 p-8 text-center text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
            No low-confidence items are waiting for review.
          </div>
        ) : null}

        {items.map((item) => (
          <div
            key={item.id}
            className="rounded-2xl border border-slate-200 p-4 dark:border-slate-800"
          >
            <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <span className="font-medium">
                    Row {item.row} · {item.column}
                  </span>
                  <Badge variant={item.status === 'pending' ? 'warning' : 'success'}>{item.status}</Badge>
                  <Badge variant="llm">{item.fix_method}</Badge>
                </div>
                <p className="text-sm text-slate-500 dark:text-slate-400">{item.reasoning}</p>
                <div className="grid gap-2 md:grid-cols-3">
                  <div>
                    <p className="text-xs uppercase text-slate-400">Original</p>
                    <p className="font-medium">{safeString(item.old_value)}</p>
                  </div>
                  <div>
                    <p className="text-xs uppercase text-slate-400">Suggested</p>
                    <p className="font-medium">{safeString(item.suggested_value)}</p>
                  </div>
                  <div>
                    <p className="text-xs uppercase text-slate-400">Confidence</p>
                    <p className="font-medium">{formatPercent(item.confidence)}</p>
                  </div>
                </div>
              </div>

              <div className="space-y-2 xl:w-[360px]">
                <Input
                  placeholder="Edit reviewed value"
                  value={drafts[item.id] ?? ''}
                  onChange={(event) =>
                    setDrafts((current) => ({ ...current, [item.id]: event.target.value }))
                  }
                />
                <div className="flex flex-wrap gap-2">
                  <Button
                    size="sm"
                    variant="secondary"
                    disabled={isSubmitting}
                    onClick={() => onAction({ item_id: item.id, action: 'accept' })}
                  >
                    Accept
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={isSubmitting}
                    onClick={() => onAction({ item_id: item.id, action: 'reject' })}
                  >
                    Reject
                  </Button>
                  <Button
                    size="sm"
                    variant="default"
                    disabled={isSubmitting || !(drafts[item.id] ?? '').trim()}
                    onClick={() =>
                      onAction({
                        item_id: item.id,
                        action: 'edit',
                        new_value: (drafts[item.id] ?? '').trim(),
                      })
                    }
                  >
                    Apply Edit
                  </Button>
                </div>
              </div>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}
