/**
 * Shared formatting utilities used across multiple views.
 *
 * formatTime normalizes UTC-naive datetime strings from the backend
 * (no tz suffix) by appending 'Z' so dayjs parses them as UTC and
 * converts to the browser's local timezone automatically.
 */
import dayjs from 'dayjs'

export function formatTime(t, fmt = 'YYYY-MM-DD HH:mm') {
  if (!t) return '-'
  const s = String(t)
  const normalized = /[Zz]$|[+-]\d{2}:?\d{2}$/.test(s) ? s : s + 'Z'
  return dayjs(normalized).format(fmt)
}

export function formatSize(bytes) {
  const value = Number(bytes || 0)
  if (!value) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let next = value
  let index = 0
  while (next >= 1024 && index < units.length - 1) {
    next /= 1024
    index++
  }
  return `${next.toFixed(index === 0 ? 0 : 1)} ${units[index]}`
}

export function extractDirName(path) {
  if (!path) return '-'
  const parts = path.split(/[/\\]/).filter(Boolean)
  return parts[parts.length - 1] || '-'
}

export function extractFilename(path) {
  return String(path || '').split(/[/\\]/).pop() || ''
}
