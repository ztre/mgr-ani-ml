<template>
  <div class="dashboard">
    <div class="header-actions">
      <h1 class="page-title">仪表盘</h1>
      <div class="actions">
        <el-switch
          class="theme-switch"
          v-model="themeDark"
          :active-icon="MoonNight"
          :inactive-icon="Sunny"
          @change="onThemeChange"
        />
        <el-button type="primary" :loading="scanning" @click="runFullScan">
          <el-icon><Refresh /></el-icon>
          全量扫描
        </el-button>
        <el-button type="success" :loading="refreshing" @click="refreshEmby">
          <el-icon><VideoPlay /></el-icon>
          刷新 Emby
        </el-button>
        <el-button @click="$router.push('/sync-groups')">
          <el-icon><Setting /></el-icon>
          管理同步组
        </el-button>
      </div>
    </div>

    <!-- 统计卡片 -->
    <el-row :gutter="24" class="stats-row">
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-content">
            <div class="stat-label">总媒体记录</div>
            <div class="stat-value">{{ stats.total_media }}</div>
          </div>
          <el-icon class="stat-icon" :size="40"><Film /></el-icon>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-content">
            <div class="stat-label">TV 剧集</div>
            <div class="stat-value">{{ stats.tv_count }}</div>
          </div>
          <el-icon class="stat-icon" :size="40"><Monitor /></el-icon>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-content">
            <div class="stat-label">电影</div>
            <div class="stat-value">{{ stats.movie_count }}</div>
          </div>
          <el-icon class="stat-icon" :size="40"><VideoCamera /></el-icon>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover" class="stat-card">
          <div class="stat-content">
            <div class="stat-label">总大小</div>
            <div class="stat-value">{{ formatSize(stats.total_size) }}</div>
          </div>
          <el-icon class="stat-icon" :size="40"><DataLine /></el-icon>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="hover" class="pending-card">
      <template #header>
        <div class="card-header-flex">
          <span>待办清单概览</span>
          <div style="display: flex; gap: 8px; align-items: center;">
            <el-tag type="warning" effect="dark">待办 {{ stats.pending_manual_count || 0 }}</el-tag>
            <el-button size="small" @click="$router.push('/pending')">进入待办清单</el-button>
          </div>
        </div>
      </template>
      <el-table :data="pendingItems" stripe style="width: 100%" empty-text="暂无待办">
        <el-table-column label="目录名" min-width="220">
          <template #default="{ row }">
            {{ extractDirName(row.original_path) }}
          </template>
        </el-table-column>
        <el-table-column prop="original_path" label="路径" min-width="380" show-overflow-tooltip />
        <el-table-column prop="type" label="类型" width="90" align="center">
          <template #default="{ row }">
            <el-tag size="small" :type="row.type === 'tv' ? 'primary' : 'success'">
              {{ row.type === 'tv' ? 'TV' : 'Movie' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="110" align="center" fixed="right">
          <template #default="{ row }">
            <el-button size="small" @click="$router.push({ path: '/pending', query: { search: row.original_path } })">
              查看
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 同步组卡片 -->
    <el-row :gutter="24" class="group-grid">
      <el-col :xs="24" :sm="12" :md="8" :lg="6" v-for="group in groups" :key="group.id">
        <el-card class="group-card" shadow="hover" :body-style="{ padding: '16px' }">
          <div class="card-header">
            <span class="group-name" :title="group.name">{{ group.name }}</span>
            <el-switch
              v-model="group.enabled"
              size="small"
              :loading="toggling === group.id"
              @change="(val) => toggleGroup(group, val)"
            />
          </div>
          <div class="card-content">
            <div class="path-item">
              <el-tag size="small" type="info">源</el-tag>
              <span class="path-text" :title="group.source">{{ group.source }}</span>
            </div>
            <div class="path-item">
              <el-tag size="small" type="success">标</el-tag>
              <span class="path-text" :title="group.target">{{ group.target }}</span>
            </div>
          </div>
          <div class="card-footer">
            <el-button 
              size="small" 
              type="primary" 
              plain 
              :loading="scanningGroup === group.id"
              :disabled="!group.enabled"
              @click="runGroupScan(group.id)"
            >
              扫描
            </el-button>
            <el-button size="small" @click="$router.push(`/sync-groups?edit=${group.id}`)">
              编辑
            </el-button>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 扫描任务历史 -->
    <el-card shadow="hover" class="history-card">
      <template #header>
        <div class="card-header-flex">
          <span>扫描历史</span>
          <el-button size="small" @click="loadTasks">刷新</el-button>
        </div>
      </template>
      <el-table :data="tasks" stripe style="width: 100%">
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column label="类型" width="180" show-overflow-tooltip>
          <template #default="{ row }">
            <el-tag :type="taskTypeTag(row)" size="small">
              {{ taskTypeText(row) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="目标" min-width="220" show-overflow-tooltip>
          <template #default="{ row }">
            {{ taskTargetText(row) }}
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="statusType(row.status)" size="small">
              {{ row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="created_at" label="开始时间" width="190">
          <template #default="{ row }">
            {{ formatTime(row.created_at) }}
          </template>
        </el-table-column>
        <el-table-column prop="finished_at" label="结束时间" width="190">
          <template #default="{ row }">
            {{ formatTime(row.finished_at) }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="180" fixed="right">
          <template #default="{ row }">
            <el-button
              v-if="isInterruptibleScanTask(row)"
              size="small"
              type="danger"
              plain
              :loading="interruptingTaskId === row.id"
              :disabled="row.status === 'cancelling'"
              @click="cancelTask(row)"
            >
              {{ row.status === 'cancelling' ? '中断中' : '中断' }}
            </el-button>
            <el-button size="small" @click="viewLogs(row)">查看日志</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 日志抽屉 -->
    <el-drawer v-model="drawerVisible" title="任务日志" size="50%">
      <div class="log-toolbar">
        <div class="log-toolbar-left">
          <el-switch
            v-model="logAutoRefresh"
            active-text="实时刷新"
            inactive-text="暂停刷新"
            @change="onLogAutoRefreshChange"
          />
          <span class="log-meta" v-if="currentLogTaskId">任务 #{{ currentLogTaskId }}</span>
          <span class="log-meta" v-if="lastLogRefreshedAt">上次刷新 {{ lastLogRefreshedAt }}</span>
        </div>
        <el-button size="small" @click="manualRefreshLogs">手动刷新</el-button>
      </div>
      <div v-loading="logsLoading" class="log-container" style="height: 750px; display: flex; flex-direction: column;">
        <pre v-if="currentLogs.length">{{ [...currentLogs].reverse().join('\n') }}</pre>
        <div v-else class="empty-logs">暂无日志</div>
      </div>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, onBeforeUnmount, onMounted, reactive, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { MoonNight, Sunny } from '@element-plus/icons-vue'
import { syncGroupsApi, scanApi, embyApi, tasksApi, mediaApi } from '../api/client'
import dayjs from 'dayjs'

const groups = ref([])
const tasks = ref([])
const scanning = ref(false)
const refreshing = ref(false)
const scanningGroup = ref(null)
const toggling = ref(null)
const themeDark = ref(false)
const interruptingTaskId = ref(null)


const stats = reactive({
  total_media: 0,
  tv_count: 0,
  movie_count: 0,
  pending_manual_count: 0,
  total_size: 0,
})
const pendingItems = ref([])

// Logs Drawer
const drawerVisible = ref(false)
const logsLoading = ref(false)
const currentLogs = ref([])
const currentLogTaskId = ref(null)
const logAutoRefresh = ref(true)
const lastLogRefreshedAt = ref('')
let logTimer = null
let tasksTimer = null

async function loadStats() {
  try {
    const { data } = await mediaApi.stats()
    stats.total_media = data.total_media
    stats.tv_count = data.tv_count
    stats.movie_count = data.movie_count
    stats.pending_manual_count = data.pending_manual_count || 0
    stats.total_size = data.total_size
  } catch {
    // ignore
  }
}
async function loadPending() {
  try {
    const { data } = await mediaApi.pending({ limit: 6, offset: 0 })
    pendingItems.value = data.items || []
  } catch {
    pendingItems.value = []
  }
}

async function loadGroups() {
  const { data } = await syncGroupsApi.list()
  groups.value = data
}

async function loadTasks() {
  try {
    const { data } = await tasksApi.list({ limit: 10 })
    tasks.value = data
  } catch {
    ElMessage.error('加载任务历史失败')
  } finally {
    restartTasksAutoRefresh()
  }
}

function clearTasksTimer() {
  if (tasksTimer) {
    clearTimeout(tasksTimer)
    tasksTimer = null
  }
}

function restartTasksAutoRefresh() {
  clearTasksTimer()
  if (!tasks.value.some((task) => ['running', 'cancelling'].includes(String(task?.status || '')))) return
  tasksTimer = setTimeout(async () => {
    await loadTasks()
  }, 2000)
}

async function toggleGroup(group, val) {
  toggling.value = group.id
  try {
    await syncGroupsApi.update(group.id, { ...group, enabled: val })
    ElMessage.success('更新成功')
  } catch {
    group.enabled = !val // revert
    ElMessage.error('更新失败')
  } finally {
    toggling.value = null
  }
}

async function runFullScan() {
  scanning.value = true
  try {
    await scanApi.run()
    ElMessage.success('全量扫描任务已启动')
    setTimeout(loadTasks, 1000)
  } catch (e) {
    ElMessage.error('启动失败')
  } finally {
    scanning.value = false
  }
}

async function runGroupScan(groupId) {
  scanningGroup.value = groupId
  try {
    await scanApi.runGroup(groupId)
    ElMessage.success('单组扫描任务已启动')
    setTimeout(loadTasks, 1000)
  } catch (e) {
    ElMessage.error('启动失败')
  } finally {
    scanningGroup.value = null
  }
}

function normalizedTaskType(row) {
  const rawType = String(row?.type || '')
  return rawType.startsWith('issue_sp:') ? rawType.slice('issue_sp:'.length) : rawType
}

function isInterruptibleScanTask(row) {
  const type = normalizedTaskType(row)
  return ['running', 'cancelling'].includes(String(row?.status || ''))
    && (type === 'full' || type === 'group' || type.startsWith('webhook_scan:') || type.startsWith('manual:'))
}

async function cancelTask(row) {
  if (!row?.id || interruptingTaskId.value === row.id || row.status === 'cancelling') return
  try {
    await ElMessageBox.confirm(
      `确认中断任务 #${row.id} 吗？当前正在处理的文件会在当前处理单元结束后停止。`,
      '中断扫描任务',
      {
        type: 'warning',
        confirmButtonText: '确认中断',
        cancelButtonText: '取消',
      },
    )
  } catch {
    return
  }

  interruptingTaskId.value = row.id
  try {
    const { data } = await tasksApi.cancel(row.id)
    row.status = 'cancelling'
    ElMessage.success(data?.message || '已发送中断请求')
    await loadTasks()
    if (currentLogTaskId.value === row.id) {
      await fetchLogs(row.id, { silent: true })
    }
  } catch (error) {
    const detail = error?.response?.data?.detail
    ElMessage.error(detail || '中断失败')
  } finally {
    interruptingTaskId.value = null
  }
}

async function refreshEmby() {
  refreshing.value = true
  try {
    await embyApi.refresh()
    ElMessage.success('Emby 刷新请求已发送')
  } catch (e) {
    ElMessage.error('刷新失败')
  } finally {
    refreshing.value = false
  }
}

async function viewLogs(task) {
  currentLogTaskId.value = task.id
  drawerVisible.value = true
  currentLogs.value = []
  await fetchLogs(task.id)
  restartLogAutoRefresh()
}

async function fetchLogs(taskId, { silent = false } = {}) {
  if (!taskId) return
  if (!silent) logsLoading.value = true
  try {
    const { data } = await tasksApi.getLogs(taskId)
    currentLogs.value = data.logs || []
    lastLogRefreshedAt.value = dayjs().format('HH:mm:ss')
  } catch {
    if (!silent) currentLogs.value = ['加载日志失败']
  } finally {
    if (!silent) logsLoading.value = false
  }
}

function clearLogTimer() {
  if (logTimer) {
    clearInterval(logTimer)
    logTimer = null
  }
}

function restartLogAutoRefresh() {
  clearLogTimer()
  if (!drawerVisible.value || !logAutoRefresh.value || !currentLogTaskId.value) return
  logTimer = setInterval(async () => {
    await fetchLogs(currentLogTaskId.value, { silent: true })
  }, 2000)
}

function onLogAutoRefreshChange() {
  restartLogAutoRefresh()
}

async function manualRefreshLogs() {
  if (!currentLogTaskId.value) return
  await fetchLogs(currentLogTaskId.value)
}

function statusType(status) {
  const map = { running: 'primary', cancelling: 'warning', cancelled: 'info', completed: 'success', failed: 'danger' }
  return map[status] || 'info'
}

function taskTypeText(row) {
  const rawType = String(row?.type || '')
  const issueTagged = rawType.startsWith('issue_sp:')
  const type = issueTagged ? rawType.slice('issue_sp:'.length) : rawType

  if (type.startsWith('manual:')) {
    const groupName = type.slice('manual:'.length) || '未知同步组'
    return issueTagged ? `${groupName} · 手动整理 · SP问题` : `${groupName} · 手动整理`
  }
  if (type.startsWith('manual_scan:')) {
    const groupName = type.slice('manual_scan:'.length) || '未知同步组'
    return issueTagged ? `${groupName} · 扫描整理 · SP问题` : `${groupName} · 扫描整理`
  }
  if (type.startsWith('webhook_scan:')) {
    const groupName = type.slice('webhook_scan:'.length) || '未知同步组'
    return issueTagged ? `${groupName} · webhook整理 · SP问题` : `${groupName} · webhook整理`
  }
  if (type === 'group') {
    const group = groups.value.find((g) => g.id === row?.target_id)
    const text = group?.name || '单组扫描'
    return issueTagged ? `${text} · SP问题` : text
  }
  if (type === 'full') {
    return issueTagged ? '全量扫描 · SP问题' : '全量扫描'
  }
  return issueTagged ? `${type || '-'} · SP问题` : (type || '-')
}

function taskTypeTag(row) {
  const rawType = String(row?.type || '')
  const issueTagged = rawType.startsWith('issue_sp:')
  const type = issueTagged ? rawType.slice('issue_sp:'.length) : rawType
  if (issueTagged) return 'danger'
  if (type.startsWith('manual:')) return 'warning'
  if (type.startsWith('manual_scan:')) return 'primary'
  if (type.startsWith('webhook_scan:')) return 'primary'
  if (type === 'full') return 'success'
  return 'info'
}

function taskTargetText(row) {
  return row?.target_name || '-'
}

function formatTime(t) {
  if (!t) return '-'
  return dayjs(t).format('YYYY-MM-DD HH:mm:ss')
}

function formatSize(bytes) {
  if (!bytes) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  while (bytes >= 1024 && i < units.length - 1) {
    bytes /= 1024
    i++
  }
  return bytes.toFixed(2) + ' ' + units[i]
}

function extractDirName(path) {
  if (!path) return '-'
  const parts = path.split(/[/\\]/).filter(Boolean)
  return parts[parts.length - 1] || '-'
}

function applyTheme(isDark) {
  document.body.classList.toggle('theme-dark', !!isDark)
  localStorage.setItem('amm_theme', isDark ? 'dark' : 'light')
}

function onThemeChange(v) {
  applyTheme(!!v)
}

onMounted(() => {
  const storedTheme = localStorage.getItem('amm_theme')
  themeDark.value = storedTheme === 'dark'
  applyTheme(themeDark.value)
  loadStats()
  loadPending()
  loadGroups()
  loadTasks()
})

watch(drawerVisible, (visible) => {
  if (!visible) {
    clearLogTimer()
    return
  }
  restartLogAutoRefresh()
})

onBeforeUnmount(() => {
  clearLogTimer()
  clearTasksTimer()
})
</script>

<style scoped>
.dashboard {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.header-actions {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 24px;
  flex-wrap: wrap;
}

.actions {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  align-items: center;
}

.stat-card {
  margin-bottom: 0;
  display: flex;
  align-items: center;
  position: relative;
  overflow: hidden;
  min-height: 118px;
}
.stat-content {
  z-index: 1;
}
.stat-label {
  font-size: 12px;
  color: #6b7280;
  margin-bottom: 4px;
}
.stat-value {
  font-size: 28px;
  font-weight: 600;
  color: #111827;
}
.stat-icon {
  position: absolute;
  right: 16px;
  bottom: 16px;
  opacity: 0.2;
  color: #9ca3af;
}

.group-card {
  margin-bottom: 0;
}
.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}
.group-name {
  font-weight: 600;
  font-size: 16px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 70%;
}
.card-content {
  margin-bottom: 16px;
  font-size: 13px;
  color: #6b7280;
}
.path-item {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
  gap: 8px;
}
.path-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  direction: rtl; /* Truncate from left for paths */
  text-align: left;
}
.card-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.history-card {
  margin-top: 0;
}
.pending-card {
  margin-bottom: 0;
}
.card-header-flex {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.log-container {
  padding: 16px;
  background: #f8fafc;
  height: 100%;
  overflow: auto;
  border-radius: 10px;
}
.log-container pre {
  color: #334155;
  font-family: "IBM Plex Mono", "Fira Code", monospace;
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-all;
  margin: 0;
}
.empty-logs {
  color: #94a3b8;
  text-align: center;
  margin-top: 40px;
}

.log-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
}

.log-toolbar-left {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.log-meta {
  color: #64748b;
  font-size: 12px;
}

.theme-switch {
  margin-right: 4px;
}
</style>
