import { h } from 'vue'

export function buildConfirmMessage(lines) {
  const visibleLines = (lines || []).map((line) => String(line || '').trim()).filter(Boolean)
  return h(
    'div',
    {
      style: {
        display: 'flex',
        flexDirection: 'column',
        gap: '8px',
        lineHeight: '1.6',
      },
    },
    visibleLines.map((line, index) => h(
      'div',
      {
        key: `${index}-${line}`,
        style: {
          whiteSpace: 'pre-wrap',
          color: index === 0 ? 'var(--el-text-color-primary)' : 'var(--el-text-color-regular)',
        },
      },
      line,
    )),
  )
}

export function buildConfirmDialogOptions(options = {}) {
  return {
    type: 'warning',
    confirmButtonText: '确定',
    cancelButtonText: '取消',
    customClass: 'amm-confirm-box',
    ...options,
  }
}