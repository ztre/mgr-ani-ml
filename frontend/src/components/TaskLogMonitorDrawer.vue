<template>
  <el-drawer
    :model-value="modelValue"
    :title="title"
    :direction="direction"
    :size="size"
    :append-to-body="appendToBody"
    :destroy-on-close="destroyOnClose"
    :class="['task-log-monitor-drawer', drawerClass]"
    :before-close="beforeClose || undefined"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div class="logs-drawer-body">
      <div class="log-toolbar">
        <div class="log-toolbar-left">
          <el-tag v-if="showStatusTag" size="small" :type="resolvedTaskStatusTagType">{{ resolvedTaskStatusText }}</el-tag>
          <span v-if="taskId" class="log-meta">任务 #{{ taskId }}</span>
          <span v-if="targetLabel" class="log-meta">{{ targetLabel }}</span>
          <span v-if="lastRefreshedAt" class="log-meta">上次刷新 {{ lastRefreshedAt }}</span>
          <slot name="toolbar-left-extra" />
        </div>
        <div class="log-toolbar-right">
          <slot name="toolbar-right-extra" />
          <el-switch
            :model-value="autoRefresh"
            active-text="实时刷新"
            inactive-text="暂停刷新"
            @update:model-value="emit('update:autoRefresh', $event)"
          />
          <el-button size="small" :disabled="manualRefreshDisabled" @click="emit('refresh')">手动刷新</el-button>
        </div>
      </div>
      <div v-if="showSummary" class="log-monitor-summary">
        <span v-if="waitingForTask || isWaitingTaskStatus(taskStatus)" class="log-monitor-waiting">{{ waitingText }}</span>
        <span v-else-if="isTerminalTaskStatus(taskStatus)" class="log-monitor-finished">{{ finishedText }}</span>
        <span v-else class="log-monitor-running">{{ runningText }}</span>
      </div>
      <div ref="logContainerRef" v-loading="logsLoading" class="log-container">
        <pre v-if="displayLogs.length">{{ displayLogs.join('\n') }}</pre>
        <div v-else class="empty-logs">{{ waitingForTask || isWaitingTaskStatus(taskStatus) ? waitingEmptyText : emptyText }}</div>
      </div>
    </div>
  </el-drawer>
</template>

<script setup>
import { computed, nextTick, ref, watch } from 'vue'

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  title: { type: String, default: '任务日志' },
  direction: { type: String, default: 'rtl' },
  size: { type: [String, Number], default: '46%' },
  appendToBody: { type: Boolean, default: false },
  destroyOnClose: { type: Boolean, default: false },
  drawerClass: { type: String, default: '' },
  beforeClose: { type: Function, default: null },
  taskId: { type: [Number, String], default: null },
  taskStatus: { type: String, default: 'pending' },
  targetLabel: { type: String, default: '' },
  lastRefreshedAt: { type: String, default: '' },
  autoRefresh: { type: Boolean, default: true },
  logs: { type: Array, default: () => [] },
  logsLoading: { type: Boolean, default: false },
  waitingForTask: { type: Boolean, default: false },
  manualRefreshDisabled: { type: Boolean, default: false },
  showStatusTag: { type: Boolean, default: true },
  showSummary: { type: Boolean, default: true },
  reverseLogs: { type: Boolean, default: true },
  stickToBottom: { type: Boolean, default: false },
  waitingText: { type: String, default: '正在等待任务写入日志...' },
  runningText: { type: String, default: '任务进行中，可实时查看日志进度。' },
  finishedText: { type: String, default: '任务已结束，日志面板保持打开，可继续查看输出。' },
  waitingEmptyText: { type: String, default: '等待任务启动...' },
  emptyText: { type: String, default: '暂无日志' },
})

const emit = defineEmits(['update:modelValue', 'update:autoRefresh', 'refresh'])
const logContainerRef = ref(null)

function isTerminalTaskStatus(status) {
  return ['completed', 'failed', 'cancelled'].includes(String(status || ''))
}

function isWaitingTaskStatus(status) {
  return ['queued'].includes(String(status || ''))
}

const resolvedTaskStatusText = computed(() => {
  const value = String(props.taskStatus || '')
  if (value === 'queued') return '等待中'
  if (value === 'running') return '运行中'
  if (value === 'completed') return '已完成'
  if (value === 'failed') return '失败'
  if (value === 'cancelled') return '已取消'
  if (value === 'cancelling') return '取消中'
  return props.waitingForTask ? '启动中' : '待确认'
})

const resolvedTaskStatusTagType = computed(() => {
  const value = String(props.taskStatus || '')
  if (value === 'queued') return 'warning'
  if (value === 'running') return 'primary'
  if (value === 'completed') return 'success'
  if (value === 'failed') return 'danger'
  if (value === 'cancelled') return 'info'
  if (value === 'cancelling') return 'warning'
  return 'warning'
})

const displayLogs = computed(() => {
  const list = Array.isArray(props.logs) ? props.logs : []
  return props.reverseLogs ? [...list].reverse() : [...list]
})

async function scrollToBottom() {
  if (!props.stickToBottom || !props.modelValue) return
  await nextTick()
  const container = logContainerRef.value
  if (!container) return
  container.scrollTop = container.scrollHeight
}

watch(
  () => props.modelValue,
  (visible) => {
    if (!visible) return
    void scrollToBottom()
  },
)

watch(
  () => props.logs,
  () => {
    void scrollToBottom()
  },
  { deep: true },
)
</script>

<style scoped>
.logs-drawer-body {
  display: flex;
  flex-direction: column;
  gap: 14px;
  height: 100%;
  min-height: 0;
}

.log-toolbar,
.log-toolbar-left,
.log-toolbar-right,
.log-monitor-summary {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.log-toolbar {
  justify-content: space-between;
}

.log-meta,
.log-monitor-waiting,
.log-monitor-running,
.log-monitor-finished {
  color: #64748b;
  font-size: 13px;
}

.log-container {
  min-height: 340px;
  flex: 1;
  padding: 16px;
  border-radius: 18px;
  background: linear-gradient(180deg, #0f172a, #172033);
  color: #e2e8f0;
  box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.12);
  overflow: auto;
}

.log-container pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 12px;
  line-height: 1.6;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
}

.empty-logs {
  color: #cbd5e1;
  font-size: 13px;
}

:deep(.task-log-monitor-drawer.el-drawer) {
  max-width: 100%;
}

:deep(.task-log-monitor-drawer .el-drawer__body) {
  padding-top: 8px;
}
</style>