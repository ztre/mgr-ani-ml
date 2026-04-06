<template>
  <div class="pending-page">
    <div class="header">
      <h1 class="page-title">待办清单</h1>
      <div class="header-actions">
        <el-input
          v-model="search"
          placeholder="按源路径筛选"
          clearable
          style="width: 320px"
          @keyup.enter="handleSearch"
          @clear="handleSearch"
        >
          <template #append>
            <el-button @click="handleSearch">
              <el-icon><Search /></el-icon>
            </el-button>
          </template>
        </el-input>
        <el-tag type="warning" effect="dark">共 {{ total }} 条</el-tag>
        <el-button plain @click="resetSearch">重置</el-button>
        <el-button @click="loadPending" :loading="loading">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
        <el-button @click="$router.push('/pending-logs')">
          <el-icon><Document /></el-icon>
          人工修正日志
        </el-button>
      </div>
    </div>

    <el-card shadow="never" class="table-card">
      <el-table :data="items" stripe v-loading="loading" style="width: 100%">
        <el-table-column label="目录名" min-width="220">
          <template #default="{ row }">
            <span class="dir-name">{{ extractDirName(row.original_path) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="original_path" label="源目录/文件路径" min-width="420" show-overflow-tooltip />
        <el-table-column prop="type" label="类型" width="100" align="center">
          <template #default="{ row }">
            <el-tag size="small" :type="row.type === 'tv' ? 'primary' : 'success'">
              {{ row.type === 'tv' ? 'TV' : 'Movie' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="创建时间" width="180" align="right">
          <template #default="{ row }">
            {{ formatTime(row.created_at) }}
          </template>
        </el-table-column>
        <el-table-column label="操作" width="260" fixed="right" align="center">
          <template #default="{ row }">
            <el-button type="primary" plain @click="openOrganizeDialog(row)">手动整理</el-button>
            <el-button
              type="danger"
              plain
              :loading="deletingId === row.id"
              @click="deletePending(row)"
            >
              删除
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="pagination-container">
        <el-pagination
          v-model:current-page="page"
          v-model:page-size="pageSize"
          :total="total"
          :page-sizes="[20, 50, 100]"
          layout="total, sizes, prev, pager, next"
          @size-change="loadPending"
          @current-change="loadPending"
        />
      </div>
    </el-card>

    <el-dialog
      v-model="dialogVisible"
      title="手动识别整理"
      width="980px"
      append-to-body
      destroy-on-close
      modal-class="organize-dialog-overlay"
      class="organize-dialog"
    >
      <div class="organize-dialog-body">
        <div class="organize-form-section">
          <el-form ref="organizeFormRef" :model="form" label-width="110px">
            <el-form-item label="待办目录">
              <div class="path-preview" :title="currentRow?.original_path">{{ currentRow?.original_path || '-' }}</div>
            </el-form-item>
            <el-form-item label="媒体类型" required>
              <el-select v-model="form.media_type" style="width: 180px">
                <el-option label="TV" value="tv" />
                <el-option label="电影" value="movie" />
              </el-select>
            </el-form-item>
            <el-form-item label="TMDB ID" required>
              <el-input v-model="form.tmdb_id" placeholder="可直接填写 TMDB ID">
                <template #append>
                  <el-button @click="openSearchDialog">
                    <el-icon><Search /></el-icon>
                  </el-button>
                </template>
              </el-input>
            </el-form-item>
            <el-form-item label="标题" required>
              <el-input v-model="form.title" placeholder="例如：刀剑神域：序列之争" />
            </el-form-item>
            <el-form-item label="年份">
              <el-input-number v-model="form.year" :min="1900" :max="2100" />
            </el-form-item>
            <el-form-item v-if="form.media_type === 'tv'" label="强制季号">
              <el-input-number v-model="form.season" :min="0" />
            </el-form-item>
            <el-form-item v-if="form.media_type === 'tv' && form.season === 0" label="强制集数" required>
              <el-input-number v-model="form.episode_override" :min="1" />
            </el-form-item>
            <el-form-item v-if="form.media_type === 'tv'" label="集号偏移">
              <el-input-number v-model="form.episode_offset" />
            </el-form-item>
          </el-form>
        </div>

        <div class="pending-files-section">
          <div class="pending-files-header">
            <div>
              <div class="pending-files-title">目录文件</div>
              <div class="pending-files-subtitle">这里只展示正片视频；不勾选则整理整个目录，勾选后会自动补齐对应附件。</div>
            </div>
            <div class="pending-files-actions">
              <div class="pending-filter-group" role="group" aria-label="目录文件过滤">
                <el-button plain :type="pendingFileView === 'all' ? 'primary' : 'default'" @click="setPendingFileView('all')">全部 {{ pendingFileItems.length }}</el-button>
                <el-button plain :type="pendingFileView === 'selected' ? 'warning' : 'default'" :disabled="pendingFilesLoading || !selectedPendingFileCount" @click="setPendingFileView('selected')">已选 {{ selectedPendingFileCount }}</el-button>
              </div>
              <div class="pending-select-group" role="group" aria-label="目录文件选择操作">
                <el-button plain @click="selectAllPendingFiles" :disabled="pendingFilesLoading || !pendingFileItems.length">全选</el-button>
                <el-button plain @click="clearPendingFileSelection" :disabled="pendingFilesLoading || !selectedPendingFileCount">清空</el-button>
              </div>
            </div>
          </div>

          <el-alert
            v-if="pendingFilesError"
            type="warning"
            :closable="false"
            show-icon
            class="pending-files-alert"
            :title="pendingFilesError"
          />

          <div v-else class="pending-files-table-wrap">
            <el-table
              :key="pendingFilesTableKey"
              :data="filteredPendingFileItems"
              row-key="relative_path"
              height="100%"
              stripe
              v-loading="pendingFilesLoading"
            >
              <el-table-column width="52" align="center">
                <template #header>
                  <el-checkbox
                    :model-value="allFilteredPendingFilesSelected"
                    :indeterminate="filteredPendingFilesSelectionIndeterminate"
                    @change="toggleAllFilteredPendingFiles"
                  />
                </template>
                <template #default="{ row }">
                  <el-checkbox
                    :model-value="isPendingFileSelected(row)"
                    @change="(checked) => togglePendingFileSelection(row, checked)"
                  />
                </template>
              </el-table-column>
              <el-table-column prop="name" label="文件名" min-width="260" show-overflow-tooltip />
              <el-table-column prop="parent_dir" label="所在目录" show-overflow-tooltip align="center">
                <template #default="{ row }">
                  {{ row.parent_dir === '.' ? '根目录' : row.parent_dir }}
                </template>
              </el-table-column>
              <el-table-column label="大小" width="120" align="center">
                <template #default="{ row }">
                  {{ formatFileSize(row.size) }}
                </template>
              </el-table-column>
              <template #empty>
                <el-empty description="目录下没有可整理视频文件" />
              </template>
            </el-table>
          </div>
        </div>
      </div>

      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitOrganize">
          {{ selectedPendingFileCount ? '整理选中文件' : '开始整理' }}
        </el-button>
      </template>
    </el-dialog>

    <TaskLogMonitorDrawer
      v-model="organizeMonitorVisible"
      title="整理任务日志"
      direction="rtl"
      size="46%"
      append-to-body
      destroy-on-close
      drawer-class="organize-monitor-drawer"
      :task-id="organizeMonitorTaskId"
      :task-status="organizeMonitorTaskStatus"
      :target-label="organizeMonitorTargetLabel"
      :last-refreshed-at="organizeMonitorLastRefreshedAt"
      :auto-refresh="organizeMonitorAutoRefresh"
      :logs="organizeMonitorLogs"
      :logs-loading="organizeMonitorLogsLoading"
      :waiting-for-task="organizeMonitorWaitingForTask"
      :manual-refresh-disabled="!organizeMonitorTaskId"
      waiting-text="正在等待整理任务写入日志..."
      running-text="整理进行中，可实时查看日志进度。"
      finished-text="整理已结束，日志面板保持打开，可继续查看输出。"
      @update:autoRefresh="organizeMonitorAutoRefresh = $event"
      @refresh="manualRefreshOrganizeMonitor"
    />

    <el-dialog
      v-model="searchDialogVisible"
      title="搜索 TMDB 条目"
      width="860px"
      class="tmdb-search-dialog"
    >
      <div class="search-bar">
        <el-input
          v-model="searchKeyword"
          placeholder="电影或电视剧名称 / 直接输入 TMDB ID"
          clearable
          @keyup.enter="runSearch"
        >
          <template #prepend>
            <el-icon><Search /></el-icon>
          </template>
          <template #append>
            <el-button :loading="searchLoading" @click="runSearch">搜索</el-button>
          </template>
        </el-input>
      </div>

      <div class="search-list" v-loading="searchLoading">
        <el-scrollbar max-height="560px">
          <div v-if="searchResults.length" class="search-items">
            <div
              v-for="item in searchResults"
              :key="item.tmdb_id"
              class="search-item"
              @click="selectSearchItem(item)"
            >
              <div class="poster-wrap">
                <img
                  v-if="posterUrl(item.poster_path)"
                  class="poster"
                  :src="posterUrl(item.poster_path)"
                  :alt="item.title || 'poster'"
                  loading="lazy"
                />
                <div v-else class="poster-placeholder">No Image</div>
              </div>
              <div class="meta">
                <div class="title-row">
                  <span class="title">{{ item.title || '-' }}</span>
                  <span class="year" v-if="item.year">({{ item.year }})</span>
                  <el-tag size="small" effect="plain" :type="item.media_type === 'tv' ? 'primary' : 'success'">
                    {{ item.media_type === 'tv' ? '电视剧' : '电影' }}
                  </el-tag>
                </div>
                <div class="overview">
                  {{ item.overview || '暂无简介' }}
                </div>
                <div class="id-line">
                  TMDB ID:
                  <a
                    class="tmdb-link"
                    :href="tmdbLink(item)"
                    target="_blank"
                    rel="noopener noreferrer"
                    @click.stop
                  >
                    {{ item.tmdb_id }}
                  </a>
                </div>
              </div>
              <div class="choose">
                <el-button type="primary" link>选择</el-button>
              </div>
            </div>
          </div>
          <el-empty v-else description="输入关键词后点击搜索" />
        </el-scrollbar>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { mediaApi, tasksApi } from '../api/client'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Document, Refresh, Search } from '@element-plus/icons-vue'
import TaskLogMonitorDrawer from '../components/TaskLogMonitorDrawer.vue'
import { buildConfirmDialogOptions, buildConfirmMessage } from '../utils/confirmMessage'
import dayjs from 'dayjs'

const route = useRoute()
const loading = ref(false)
const items = ref([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const search = ref('')
const dialogVisible = ref(false)
const submitting = ref(false)
const currentRow = ref(null)
const searchDialogVisible = ref(false)
const searchLoading = ref(false)
const searchKeyword = ref('')
const searchResults = ref([])
const deletingId = ref(null)
const pendingFilesLoading = ref(false)
const pendingFileItems = ref([])
const selectedPendingPaths = ref([])
const pendingFilesError = ref('')
const pendingFileView = ref('all')
const organizeFormRef = ref(null)
const organizeMonitorVisible = ref(false)
const organizeMonitorLogsLoading = ref(false)
const organizeMonitorLogs = ref([])
const organizeMonitorTaskId = ref(null)
const organizeMonitorTaskStatus = ref('pending')
const organizeMonitorTargetLabel = ref('')
const organizeMonitorLastRefreshedAt = ref('')
const organizeMonitorWaitingForTask = ref(false)
const organizeMonitorAutoRefresh = ref(true)
const organizeMonitorRequestPending = ref(false)
const organizeMonitorMatchSpec = ref(null)
const organizeMonitorHandledTerminalTaskId = ref(null)
const form = reactive({
  media_type: 'tv',
  tmdb_id: '',
  title: '',
  year: undefined,
  season: undefined,
  episode_override: undefined,
  episode_offset: undefined,
})

function normalizePendingPath(path) {
  return String(path || '').replace(/\\/g, '/').trim()
}

const selectedPendingPathSet = computed(() => new Set(selectedPendingPaths.value.map((path) => normalizePendingPath(path)).filter(Boolean)))

const selectedPendingFileItems = computed(() => pendingFileItems.value.filter((item) => selectedPendingPathSet.value.has(normalizePendingPath(item?.relative_path))))

const selectedPendingFileCount = computed(() => selectedPendingFileItems.value.length)

const selectedPendingPayloadPaths = computed(() => selectedPendingFileItems.value.map((item) => normalizePendingPath(item?.relative_path)).filter(Boolean))

const filteredPendingFileItems = computed(() => {
  if (pendingFileView.value === 'selected') {
    return [...selectedPendingFileItems.value]
  }
  return [...pendingFileItems.value]
})

const pendingFilesTableKey = computed(() => `${Number(currentRow.value?.id || 0)}:${pendingFileView.value}:${filteredPendingFileItems.value.length}:${selectedPendingFileCount.value}`)

const allFilteredPendingFilesSelected = computed(() => {
  if (!filteredPendingFileItems.value.length) return false
  return filteredPendingFileItems.value.every((item) => selectedPendingPathSet.value.has(normalizePendingPath(item?.relative_path)))
})

const filteredPendingFilesSelectionIndeterminate = computed(() => {
  if (!filteredPendingFileItems.value.length) return false
  const selectedCount = filteredPendingFileItems.value.filter((item) => selectedPendingPathSet.value.has(normalizePendingPath(item?.relative_path))).length
  return selectedCount > 0 && selectedCount < filteredPendingFileItems.value.length
})

let organizeMonitorTimer = null
let organizeDialogWheelOverlayEl = null

function isTerminalTaskStatus(status) {
  return ['completed', 'failed', 'cancelled'].includes(String(status || ''))
}

function clearOrganizeMonitorTimer() {
  if (organizeMonitorTimer) {
    clearInterval(organizeMonitorTimer)
    organizeMonitorTimer = null
  }
}

function isScrollableElement(element) {
  if (!(element instanceof HTMLElement)) return false
  const style = window.getComputedStyle(element)
  const overflowY = `${style.overflowY} ${style.overflow}`
  if (!/(auto|scroll|overlay)/.test(overflowY)) return false
  return element.scrollHeight > element.clientHeight + 1
}

function canElementScroll(element, deltaY) {
  if (!(element instanceof HTMLElement)) return false
  if (Math.abs(deltaY) < 0.5) return false
  if (deltaY < 0) {
    return element.scrollTop > 0
  }
  return element.scrollTop + element.clientHeight < element.scrollHeight - 1
}

function findDialogScrollableElement(target, dialogEl) {
  if (!(target instanceof HTMLElement) || !(dialogEl instanceof HTMLElement)) return null

  const tableWrapEl = target.closest('.pending-files-table-wrap')
  if (tableWrapEl instanceof HTMLElement) {
    const tableScrollEl = tableWrapEl.querySelector('.el-scrollbar__wrap, .el-table__body-wrapper')
    if (tableScrollEl instanceof HTMLElement && isScrollableElement(tableScrollEl)) {
      return tableScrollEl
    }
  }

  let current = target
  while (current && current !== dialogEl) {
    if (isScrollableElement(current)) {
      return current
    }
    current = current.parentElement
  }

  if (isScrollableElement(dialogEl)) {
    return dialogEl
  }

  return null
}

function handleOrganizeDialogWheel(event) {
  if (!dialogVisible.value || !(event.target instanceof HTMLElement)) {
    return
  }

  const overlayEl = organizeDialogWheelOverlayEl
  const dialogEl = overlayEl?.querySelector('.organize-dialog .el-dialog') || overlayEl?.querySelector('.el-dialog')
  if (!(dialogEl instanceof HTMLElement)) {
    event.preventDefault()
    return
  }

  if (!dialogEl.contains(event.target)) {
    event.preventDefault()
    return
  }

  const scrollableEl = findDialogScrollableElement(event.target, dialogEl)
  if (!scrollableEl || !canElementScroll(scrollableEl, event.deltaY)) {
    event.preventDefault()
  }
}

function detachOrganizeDialogWheelLock() {
  if (!organizeDialogWheelOverlayEl) return
  organizeDialogWheelOverlayEl.removeEventListener('wheel', handleOrganizeDialogWheel)
  organizeDialogWheelOverlayEl = null
}

async function attachOrganizeDialogWheelLock() {
  await nextTick()
  detachOrganizeDialogWheelLock()
  const overlayEl = document.querySelector('.organize-dialog-overlay')
  if (!(overlayEl instanceof HTMLElement)) return
  overlayEl.addEventListener('wheel', handleOrganizeDialogWheel, { passive: false })
  organizeDialogWheelOverlayEl = overlayEl
}

function resetOrganizeMonitorState() {
  clearOrganizeMonitorTimer()
  organizeMonitorLogsLoading.value = false
  organizeMonitorLogs.value = []
  organizeMonitorTaskId.value = null
  organizeMonitorTaskStatus.value = 'pending'
  organizeMonitorTargetLabel.value = ''
  organizeMonitorLastRefreshedAt.value = ''
  organizeMonitorWaitingForTask.value = false
  organizeMonitorAutoRefresh.value = true
  organizeMonitorRequestPending.value = false
  organizeMonitorMatchSpec.value = null
  organizeMonitorHandledTerminalTaskId.value = null
}

function buildOrganizeMonitorMatchSpec({ typePrefix, targetName, targetLabel }) {
  return {
    typePrefix,
    targetName,
    targetLabel,
    startedAtMs: Date.now(),
  }
}

function findMatchingOrganizeMonitorTask(tasks) {
  const spec = organizeMonitorMatchSpec.value
  if (!spec) return null
  return (tasks || []).find((task) => {
    const taskType = String(task?.type || '')
    const taskTargetName = String(task?.target_name || '')
    const createdAt = dayjs(task?.created_at).valueOf()
    return taskType.startsWith(spec.typePrefix)
      && taskTargetName === String(spec.targetName || '')
      && Number.isFinite(createdAt)
      && createdAt >= spec.startedAtMs - 5000
  }) || null
}

async function fetchOrganizeMonitorLogs(taskId, { silent = false } = {}) {
  if (!taskId) return
  if (!silent) {
    organizeMonitorLogsLoading.value = true
  }
  try {
    const { data } = await tasksApi.getLogs(taskId)
    organizeMonitorLogs.value = data?.logs || []
    organizeMonitorLastRefreshedAt.value = dayjs().format('HH:mm:ss')
  } catch {
    if (!silent) {
      organizeMonitorLogs.value = ['加载日志失败']
    }
  } finally {
    if (!silent) {
      organizeMonitorLogsLoading.value = false
    }
  }
}

async function refreshOrganizeMonitorTask({ silent = false } = {}) {
  if (!organizeMonitorVisible.value) return
  try {
    const { data } = await tasksApi.list({ limit: 30, offset: 0 })
    const tasks = data?.items || []
    if (!organizeMonitorTaskId.value) {
      const matched = findMatchingOrganizeMonitorTask(tasks)
      if (matched) {
        organizeMonitorTaskId.value = matched.id
        organizeMonitorTaskStatus.value = String(matched.status || 'running')
        organizeMonitorWaitingForTask.value = false
        await fetchOrganizeMonitorLogs(matched.id, { silent })
        return
      }
      organizeMonitorWaitingForTask.value = true
      return
    }

    const matched = tasks.find((task) => Number(task?.id) === Number(organizeMonitorTaskId.value))
    if (matched) {
      organizeMonitorTaskStatus.value = String(matched.status || organizeMonitorTaskStatus.value || 'running')
    }
  } catch {
    if (!organizeMonitorTaskId.value) {
      organizeMonitorWaitingForTask.value = true
    }
  }
}

function restartOrganizeMonitorAutoRefresh() {
  clearOrganizeMonitorTimer()
  if (!organizeMonitorVisible.value || !organizeMonitorAutoRefresh.value) return
  organizeMonitorTimer = setInterval(async () => {
    await refreshOrganizeMonitorTask({ silent: true })
    if (organizeMonitorTaskId.value) {
      await fetchOrganizeMonitorLogs(organizeMonitorTaskId.value, { silent: true })
    }
    if (!organizeMonitorRequestPending.value && isTerminalTaskStatus(organizeMonitorTaskStatus.value)) {
      clearOrganizeMonitorTimer()
    }
  }, 2000)
}

async function manualRefreshOrganizeMonitor() {
  await refreshOrganizeMonitorTask()
  if (organizeMonitorTaskId.value) {
    await fetchOrganizeMonitorLogs(organizeMonitorTaskId.value)
  }
}

async function attachOrganizeMonitorTask(taskId) {
  if (!taskId) return
  organizeMonitorTaskId.value = Number(taskId)
  organizeMonitorWaitingForTask.value = false
  await refreshOrganizeMonitorTask({ silent: true })
  await fetchOrganizeMonitorLogs(organizeMonitorTaskId.value)
  restartOrganizeMonitorAutoRefresh()
}

function setPendingFileView(nextView) {
  if (nextView === 'selected' && !selectedPendingFileCount.value) {
    pendingFileView.value = 'all'
    return
  }
  pendingFileView.value = nextView
}

function openOrganizeMonitor(spec) {
  resetOrganizeMonitorState()
  organizeMonitorMatchSpec.value = spec
  organizeMonitorTargetLabel.value = spec.targetLabel || spec.targetName || ''
  organizeMonitorVisible.value = true
  organizeMonitorWaitingForTask.value = true
  organizeMonitorRequestPending.value = true
  restartOrganizeMonitorAutoRefresh()
}

function extractDirName(path) {
  if (!path) return '-'
  const parts = path.split(/[/\\]/).filter(Boolean)
  return parts[parts.length - 1] || '-'
}

function formatTime(t) {
  if (!t) return '-'
  return dayjs(t).format('YYYY-MM-DD HH:mm')
}

function formatFileSize(size) {
  const value = Number(size || 0)
  if (value <= 0) return '-'
  if (value < 1024) return `${value} B`
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`
  if (value < 1024 ** 3) return `${(value / (1024 ** 2)).toFixed(1)} MB`
  return `${(value / (1024 ** 3)).toFixed(2)} GB`
}

async function openOrganizeDialog(row) {
  currentRow.value = row
  form.media_type = row?.type || 'tv'
  form.tmdb_id = ''
  form.title = ''
  form.year = undefined
  form.season = undefined
  form.episode_override = undefined
  form.episode_offset = undefined
  pendingFileItems.value = []
  pendingFilesError.value = ''
  selectedPendingPaths.value = []
  pendingFileView.value = 'all'
  dialogVisible.value = true
  await loadPendingFiles(row)
}

async function loadPendingFiles(row) {
  if (!row?.id) return
  pendingFilesLoading.value = true
  pendingFilesError.value = ''
  pendingFileItems.value = []
  selectedPendingPaths.value = []
  pendingFileView.value = 'all'
  try {
    const { data } = await mediaApi.pendingFiles(row.id)
    pendingFileItems.value = (data.items || []).filter((item) => item.file_type === 'video' && item.content_role === 'mainline')
    selectedPendingPaths.value = []
    pendingFileView.value = 'all'
  } catch (e) {
    pendingFilesError.value = e.response?.data?.detail || '目录文件加载失败'
  } finally {
    pendingFilesLoading.value = false
  }
}

function isPendingFileSelected(row) {
  return selectedPendingPathSet.value.has(normalizePendingPath(row?.relative_path))
}

function togglePendingFileSelection(row, checked) {
  const path = normalizePendingPath(row?.relative_path)
  if (!path) return

  const selected = new Set(selectedPendingPaths.value)
  if (checked) {
    selected.add(path)
  } else {
    selected.delete(path)
  }
  selectedPendingPaths.value = [...selected]

  if (pendingFileView.value === 'selected' && !selectedPendingFileCount.value) {
    pendingFileView.value = 'all'
  }
}

function clearPendingFileSelection() {
  selectedPendingPaths.value = []
  if (pendingFileView.value === 'selected') {
    pendingFileView.value = 'all'
  }
}

function selectPendingFiles(predicate) {
  selectedPendingPaths.value = pendingFileItems.value
    .filter((item) => predicate(item))
    .map((item) => normalizePendingPath(item?.relative_path))
    .filter(Boolean)
}

function toggleAllFilteredPendingFiles(checked) {
  const selected = new Set(selectedPendingPaths.value)
  filteredPendingFileItems.value.forEach((item) => {
    const path = normalizePendingPath(item?.relative_path)
    if (!path) return
    if (checked) {
      selected.add(path)
    } else {
      selected.delete(path)
    }
  })
  selectedPendingPaths.value = [...selected]

  if (pendingFileView.value === 'selected' && !selectedPendingFileCount.value) {
    pendingFileView.value = 'all'
  }
}

function selectAllPendingFiles() {
  selectPendingFiles(() => true)
}

function removePendingRowLocally(pendingId) {
  if (!pendingId) return
  const nextItems = items.value.filter((item) => Number(item?.id) !== Number(pendingId))
  const removedCount = items.value.length - nextItems.length
  if (!removedCount) return
  items.value = nextItems
  total.value = Math.max(0, total.value - removedCount)
}

async function submitOrganize() {
  if (!currentRow.value) return
  if (!form.tmdb_id) {
    ElMessage.warning('请填写 TMDB ID，或先用搜索选择条目')
    return
  }
  if (form.media_type === 'tv' && form.season === 0 && (form.episode_override === undefined || form.episode_override === null)) {
    ElMessage.warning('强制季号为 0 时必须填写强制集数')
    return
  }
  const targetName = extractDirName(currentRow.value?.original_path)
  submitting.value = true
  openOrganizeMonitor(
    buildOrganizeMonitorMatchSpec({
      typePrefix: 'manual:',
      targetName,
        targetLabel: selectedPendingFileCount.value
        ? `手动整理 · ${targetName} · ${selectedPendingFileCount.value} 项`
        : `手动整理 · ${targetName}`,
    }),
  )
  try {
    const payload = {
      tmdb_id: Number(form.tmdb_id),
      title: (form.title || '').trim() || null,
      year: form.year ?? null,
      media_type: form.media_type,
      season: form.media_type === 'tv' ? (form.season ?? null) : null,
      episode_override: (form.media_type === 'tv' && form.season === 0) ? (form.episode_override ?? null) : null,
      episode_offset: form.media_type === 'tv' ? (form.episode_offset ?? null) : null,
      selected_paths: selectedPendingFileCount.value ? [...selectedPendingPayloadPaths.value] : null,
    }
    dialogVisible.value = false
    const { data } = await mediaApi.manualOrganize(currentRow.value.id, payload)
    await attachOrganizeMonitorTask(data?.task_id)
    ElMessage.success(data?.message || '手动整理任务已进入队列')
  } catch (e) {
    await refreshOrganizeMonitorTask()
    if (organizeMonitorTaskId.value) {
      await fetchOrganizeMonitorLogs(organizeMonitorTaskId.value)
    }
    if (!organizeMonitorTaskId.value) {
      resetOrganizeMonitorState()
      organizeMonitorVisible.value = false
    }
    ElMessage.error(e.response?.data?.detail || '整理失败')
  } finally {
    submitting.value = false
    organizeMonitorRequestPending.value = false
    restartOrganizeMonitorAutoRefresh()
  }
}

async function deletePending(row) {
  if (!row?.id) return
  try {
    await ElMessageBox.confirm(
      buildConfirmMessage([
        '确认删除该待办记录？',
        extractDirName(row.original_path) || '-',
      ]),
      '删除待办',
      buildConfirmDialogOptions({ confirmButtonText: '删除' }),
    )
  } catch {
    return
  }

  deletingId.value = row.id
  try {
    const { data } = await mediaApi.batchDelete({
      ids: [row.id],
      delete_files: false,
    })
    ElMessage.success(`已删除 ${data.deleted_records || 0} 条待办记录`)
    await loadPending()
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '删除失败')
  } finally {
    deletingId.value = null
  }
}

function openSearchDialog() {
  searchKeyword.value = form.title || ''
  searchResults.value = []
  searchDialogVisible.value = true
}

async function runSearch() {
  const keyword = (searchKeyword.value || '').trim()
  if (!keyword) {
    ElMessage.warning('请输入关键词或 TMDB ID')
    return
  }
  searchLoading.value = true
  try {
    const { data } = await mediaApi.searchTmdb({
      q: keyword,
      media_type: form.media_type,
      limit: 20,
    })
    searchResults.value = data.items || []
    if (!searchResults.value.length) {
      ElMessage.info('未找到匹配结果')
    }
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '搜索失败')
  } finally {
    searchLoading.value = false
  }
}

function selectSearchItem(item) {
  form.tmdb_id = item.tmdb_id ? String(item.tmdb_id) : ''
  form.title = item.title || ''
  form.year = item.year ?? undefined
  searchDialogVisible.value = false
}

function posterUrl(posterPath) {
  if (!posterPath) return ''
  if (String(posterPath).startsWith('http')) return posterPath
  return `https://image.tmdb.org/t/p/w500${posterPath}`
}

function tmdbLink(item) {
  if (!item?.tmdb_id) return '#'
  const type = item.media_type === 'tv' ? 'tv' : 'movie'
  return `https://www.themoviedb.org/${type}/${item.tmdb_id}`
}

function handleSearch() {
  page.value = 1
  loadPending()
}

function resetSearch() {
  search.value = ''
  page.value = 1
  loadPending()
}

async function loadPending() {
  loading.value = true
  try {
    const { data } = await mediaApi.pending({
      offset: (page.value - 1) * pageSize.value,
      limit: pageSize.value,
      search: (search.value || '').trim() || undefined,
    })
    items.value = data.items || []
    total.value = data.total || 0
  } finally {
    loading.value = false
  }
}

watch(
  () => route.query.search,
  (v) => {
    const next = typeof v === 'string' ? v : ''
    if (next === search.value) return
    search.value = next
    page.value = 1
    loadPending()
  },
)

onMounted(() => {
  const q = route.query.search
  search.value = typeof q === 'string' ? q : ''
  loadPending()
})

onUnmounted(() => {
  detachOrganizeDialogWheelLock()
  clearOrganizeMonitorTimer()
})

watch(dialogVisible, async (visible) => {
  if (visible) {
    await attachOrganizeDialogWheelLock()
    return
  }
  detachOrganizeDialogWheelLock()
})

watch(organizeMonitorVisible, (visible) => {
  if (!visible) {
    resetOrganizeMonitorState()
    return
  }
  restartOrganizeMonitorAutoRefresh()
})

watch(organizeMonitorTaskStatus, async (status) => {
  if (!organizeMonitorTaskId.value || !isTerminalTaskStatus(status)) return
  if (organizeMonitorHandledTerminalTaskId.value === organizeMonitorTaskId.value) return
  organizeMonitorHandledTerminalTaskId.value = organizeMonitorTaskId.value
  await loadPending()
})
</script>

<style scoped>
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 24px;
  gap: 24px;
  flex-wrap: wrap;
}
.header-actions {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
}
.table-card {
  border: none;
  background: transparent;
}
.dir-name {
  font-weight: 600;
  color: #111827;
}
.path-preview {
  font-size: 12px;
  color: #64748b;
  word-break: break-all;
}
.organize-dialog-body {
  display: grid;
  grid-template-rows: auto minmax(0, 1fr);
  gap: 12px;
  height: 100%;
  min-height: 0;
  overflow: hidden;
}

.organize-form-section {
  min-height: 0;
}

.organize-form-section :deep(.el-form) {
  margin-bottom: 0;
}
.pending-files-section {
  display: flex;
  flex: 1 1 0;
  flex-direction: column;
  margin-top: 8px;
  border-top: 1px solid var(--amm-border, #e5e7eb);
  min-height: 0;
  padding-top: 18px;
}
.pending-files-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}
.pending-files-title {
  font-size: 15px;
  font-weight: 600;
  color: #111827;
}
.pending-files-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: #64748b;
}
.pending-files-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  justify-content: flex-end;
}
.pending-filter-group,
.pending-select-group {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.pending-filter-group :deep(.el-button),
.pending-select-group :deep(.el-button) {
  margin-left: 0;
  min-width: 74px;
  border-radius: 10px;
}
.pending-files-alert {
  margin-bottom: 12px;
}
.pending-files-table-wrap {
  flex: 1 1 0;
  min-height: 0;
  overflow: hidden;
  overscroll-behavior: contain;
}

.pending-files-table-wrap :deep(.el-table) {
  width: 100%;
  height: 100%;
}

.pending-files-table-wrap :deep(.el-scrollbar),
.pending-files-table-wrap :deep(.el-scrollbar__wrap),
.pending-files-table-wrap :deep(.el-table__body-wrapper) {
  overscroll-behavior: contain;
}
.search-bar {
  margin-bottom: 24px;
}
.search-list {
  border-top: 1px solid var(--amm-border, #e5e7eb);
  padding-top: 16px;
}
.search-items {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.search-item {
  display: flex;
  gap: 12px;
  padding: 12px;
  border-radius: 12px;
  cursor: pointer;
  border: 1px solid transparent;
  transition: background-color 0.2s ease, border-color 0.2s ease;
}
.search-item:hover {
  background: rgba(148, 163, 184, 0.08);
  border-color: var(--amm-border, #e5e7eb);
}
.poster-wrap {
  width: 56px;
  min-width: 56px;
  height: 84px;
  border-radius: 8px;
  overflow: hidden;
  background: rgba(100, 116, 139, 0.12);
}
.poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}
.poster-placeholder {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #94a3b8;
  font-size: 10px;
}
.meta {
  flex: 1;
  min-width: 0;
}
.title-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}
.title {
  font-size: 16px;
  font-weight: 500;
  line-height: 1.25;
  color: #111827;
}
.year {
  color: #6b7280;
  font-size: 14px;
}
.overview {
  color: #4b5563;
  font-size: 14px;
  line-height: 1.5;
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
.id-line {
  margin-top: 6px;
  color: #94a3b8;
  font-size: 12px;
}
.tmdb-link {
  color: #3b82f6;
  text-decoration: none;
}
.tmdb-link:hover {
  text-decoration: underline;
}
.choose {
  width: 60px;
  min-width: 60px;
  display: flex;
  align-items: center;
  justify-content: flex-end;
}
.pagination-container {
  margin-top: 24px;
  display: flex;
  justify-content: flex-end;
}

:deep(.tmdb-search-dialog .el-dialog) {
  max-width: 92vw;
}

/* organize-dialog 对话框级别的样式已移至文件末尾非 scoped <style> 块
   原因：el-dialog 使用 append-to-body teleport，:deep() 生成的后代选择器
   [data-v-xxxx] .organize-dialog 无法匹配（没有 data-v 祖先节点）。 */

@media (max-width: 900px) {
  .pending-files-header {
    flex-direction: column;
    align-items: stretch;
  }
}
</style>

<!-- 对话框全局样式：el-dialog 被 teleport 到 body，scoped :deep() 无效，必须用全局 CSS -->
<style>
.el-overlay.organize-dialog-overlay {
  overflow: hidden;
}

.el-overlay.organize-dialog-overlay .el-overlay-dialog {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
  box-sizing: border-box;
  overflow: hidden;
}

.el-dialog.organize-dialog {
  box-sizing: border-box;
  height: min(760px, calc(100dvh - 32px));
  max-height: calc(100dvh - 32px);
  width: min(980px, calc(100vw - 32px));
  max-width: calc(100vw - 32px);
  margin: 0 !important;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.el-dialog.organize-dialog .el-dialog__header,
.el-dialog.organize-dialog .el-dialog__footer {
  flex: 0 0 auto;
}

.el-dialog.organize-dialog .el-form-item {
  margin-bottom: 10px;
}

.el-dialog.organize-dialog .el-dialog__body {
  flex: 1 1 0;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  padding-top: 12px;
}
</style>
