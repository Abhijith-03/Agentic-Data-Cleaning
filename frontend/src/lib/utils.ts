import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDuration(durationMs?: number) {
  if (!durationMs) return '—'
  if (durationMs < 1000) return `${durationMs.toFixed(0)} ms`
  return `${(durationMs / 1000).toFixed(2)} s`
}

export function formatPercent(value?: number | null, digits = 0) {
  if (value === undefined || value === null || Number.isNaN(value)) return '—'
  const normalized = value <= 1 ? value * 100 : value
  return `${normalized.toFixed(digits)}%`
}

export function formatNumber(value?: number | null) {
  if (value === undefined || value === null || Number.isNaN(value)) return '—'
  return new Intl.NumberFormat().format(value)
}

export function titleCase(value: string) {
  return value
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
}

export function safeString(value: unknown) {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}

export function readValue(row: Record<string, unknown>, key: string) {
  return row[key]
}
