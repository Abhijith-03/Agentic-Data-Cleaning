import { useMemo, useState } from 'react'
import { Search } from 'lucide-react'

import { Card, CardContent, CardDescription, CardHeader, CardTitle, Input } from '../ui'
import { cn, safeString } from '../../lib/utils'

export function DataPreviewTable({
  title,
  description,
  rows,
  pageSize = 12,
  highlightColumns = [],
}: {
  title: string
  description?: string
  rows: Record<string, unknown>[]
  pageSize?: number
  highlightColumns?: string[]
}) {
  const [query, setQuery] = useState('')
  const [page, setPage] = useState(1)
  const [sortKey, setSortKey] = useState<string | null>(null)
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc')

  const columns = useMemo(() => (rows[0] ? Object.keys(rows[0]) : []), [rows])

  const filteredRows = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase()
    let nextRows = rows.filter((row) =>
      normalizedQuery
        ? Object.values(row).some((value) => safeString(value).toLowerCase().includes(normalizedQuery))
        : true,
    )
    if (sortKey) {
      nextRows = [...nextRows].sort((left, right) => {
        const a = safeString(left[sortKey]).toLowerCase()
        const b = safeString(right[sortKey]).toLowerCase()
        return sortDirection === 'asc' ? a.localeCompare(b) : b.localeCompare(a)
      })
    }
    return nextRows
  }, [query, rows, sortDirection, sortKey])

  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize))
  const pageRows = filteredRows.slice((page - 1) * pageSize, page * pageSize)

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <CardTitle>{title}</CardTitle>
            {description ? <CardDescription>{description}</CardDescription> : null}
          </div>
          <div className="relative w-full md:w-72">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
            <Input
              value={query}
              onChange={(event) => {
                setPage(1)
                setQuery(event.target.value)
              }}
              placeholder="Search rows"
              className="pl-9"
            />
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="overflow-x-auto rounded-2xl border border-slate-200 dark:border-slate-800">
          <table className="min-w-full text-left text-sm">
            <thead className="bg-slate-100/70 text-slate-600 dark:bg-slate-900 dark:text-slate-300">
              <tr>
                {columns.map((column) => (
                  <th key={column} className="px-4 py-3 font-medium">
                    <button
                      className="inline-flex items-center gap-1"
                      onClick={() => {
                        if (sortKey === column) {
                          setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')
                        } else {
                          setSortKey(column)
                          setSortDirection('asc')
                        }
                      }}
                    >
                      {column}
                      {sortKey === column ? (sortDirection === 'asc' ? '↑' : '↓') : ''}
                    </button>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {pageRows.map((row, rowIndex) => (
                <tr key={`${rowIndex}-${JSON.stringify(row)}`} className="border-t border-slate-200 dark:border-slate-800">
                  {columns.map((column) => {
                    const value = row[column]
                    const isEmpty = value === null || value === undefined || value === ''
                    const shouldHighlight = highlightColumns.includes(column)
                    return (
                      <td
                        key={column}
                        className={cn(
                          'max-w-[220px] px-4 py-3 align-top',
                          isEmpty && 'bg-amber-50 text-amber-700 dark:bg-amber-950/30 dark:text-amber-300',
                          shouldHighlight && 'font-medium text-sky-600 dark:text-sky-300',
                        )}
                      >
                        <div className="truncate" title={safeString(value)}>
                          {safeString(value)}
                        </div>
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="flex items-center justify-between text-sm text-slate-500 dark:text-slate-400">
          <span>
            Showing {pageRows.length} of {filteredRows.length} rows
          </span>
          <div className="flex items-center gap-2">
            <button
              className="rounded-lg border border-slate-300 px-3 py-1.5 disabled:opacity-50 dark:border-slate-700"
              disabled={page <= 1}
              onClick={() => setPage((current) => Math.max(1, current - 1))}
            >
              Previous
            </button>
            <span>
              {page} / {totalPages}
            </span>
            <button
              className="rounded-lg border border-slate-300 px-3 py-1.5 disabled:opacity-50 dark:border-slate-700"
              disabled={page >= totalPages}
              onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
            >
              Next
            </button>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
