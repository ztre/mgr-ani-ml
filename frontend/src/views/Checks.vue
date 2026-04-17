<template>
  <div class="checks-view">
    <div class="header">
      <h1 class="page-title">检查中心</h1>
      <div class="header-actions">
        <el-button type="info" :loading="runningFull" @click="runFullCheck">
          <el-icon><Search /></el-icon>
          全量检查
        </el-button>
      </div>
    </div>

    <!-- Issue filter bar -->
    <el-card shadow="never" class="filter-card">
      <el-row :gutter="12" align="middle">
        <el-col :span="4">
          <el-select v-model="filterStatus" placeholder="状态" clearable @change="loadIssues">
            <el-option label="待处理 (open)" value="open" />
            <el-option label="已忽略 (ignored)" value="ignored" />
            <el-option label="已解决 (resolved)" value="resolved" />
          </el-select>
        </el-col>
        <el-col :span="4">
          <el-select v-model="filterChecker" placeholder="检查器" clearable @change="loadIssues">
            <el-option label="源文件未使用" value="source_unrecorded" />
            <el-option label="源目录未使用" value="source_dir_unrecorded" />
            <el-option label="孤立硬链接" value="links_orphans" />
            <el-option label="目标路径异常" value="media_path_sanity" />
            <el-option label="目标文件无源" value="target_no_source" />
            <el-option label="目标目录无源" value="target_dir_no_source" />
          </el-select>
        </el-col>
        <el-col :span="3">
          <el-button @click="resetFilters">重置过滤</el-button>
          <el-button :loading="issuesLoading" :icon="Refresh" @click="loadIssues" style="margin-left:8px" />
        </el-col>
      </el-row>
    </el-card>

    <!-- Issues table -->
    <el-card shadow="never" class="table-card">
      <el-table
        v-loading="issuesLoading"
        :data="groupedIssues"
        row-key="_key"
        :tree-props="{ children: 'children', hasChildren: '_isGroup' }"
        stripe
        size="small"
      >
        <!-- Directory / file column -->
        <el-table-column label="目录 / 文件" min-width="320">
          <template #default="{ row }">
            <template v-if="row._isGroup">
              <span class="dir-path" :title="row._dir">{{ row._dir }}</span>
              <el-tag size="small" effect="plain" style="margin-left:8px;vertical-align:middle">{{ row.children.length }} 个问题</el-tag>
            </template>
            <template v-else-if="row._isDirRow">
              <span class="dir-path" :title="row.resource_dir">{{ row.resource_dir }}</span>
              <el-tag v-if="row.payload?.file_count" size="small" effect="plain" style="margin-left:8px;vertical-align:middle">{{ row.payload.file_count }} 个文件</el-tag>
            </template>
            <template v-else>
              <div class="path-cell">
                <div v-if="row.source_path" class="path-row">
                  <span class="path-label">源</span>
                  <span class="path-text" :title="row.source_path">{{ basename(row.source_path) }}</span>
                </div>
                <div v-if="row.target_path" class="path-row">
                  <span class="path-label target">目标</span>
                  <span class="path-text" :title="row.target_path">{{ basename(row.target_path) }}</span>
                </div>
                <div v-if="!row.source_path && !row.target_path" class="path-text">-</div>
              </div>
            </template>
          </template>
        </el-table-column>
        <!-- Checker column -->
        <el-table-column label="检查器" width="130">
          <template #default="{ row }">
            <template v-if="row._isGroup">
              <div class="tag-stack">
                <el-tag v-for="c in row.checker_codes" :key="c" type="info" effect="plain" size="small">{{ checkerLabel(c) }}</el-tag>
              </div>
            </template>
            <template v-else>
              <el-tag type="info" effect="plain" size="small">{{ checkerLabel(row.checker_code) }}</el-tag>
            </template>
          </template>
        </el-table-column>
        <!-- Status column -->
        <el-table-column label="状态" width="140">
          <template #default="{ row }">
            <template v-if="row._isGroup">
              <span class="group-status-summary">{{ groupStatusSummary(row) }}</span>
            </template>
            <template v-else>
              <el-tag :type="statusTagType(row.status)" size="small" effect="plain">{{ statusLabel(row.status) }}</el-tag>
            </template>
          </template>
        </el-table-column>
        <!-- Time column -->
        <el-table-column label="更新时间" width="130">
          <template #default="{ row }">
            <span class="muted">{{ formatTime(row.updated_at) }}</span>
          </template>
        </el-table-column>
        <!-- Operations column -->
        <el-table-column label="操作" width="230" fixed="right">
          <template #default="{ row }">
            <el-button-group v-if="row._isGroup" size="small">
              <el-button type="warning" plain
                :disabled="(row.children || []).every(c => c.status === 'ignored' || c.status === 'resolved')"
                @click="batchActionGroup(row, 'ignore')">批量忽略</el-button>
              <el-button type="success" plain
                :disabled="(row.children || []).every(c => c.status === 'resolved' || c.status === 'ignored')"
                @click="batchActionGroup(row, 'resolve')">批量解决</el-button>
              <el-button plain
                v-if="filterStatus !== 'open'"
                @click="batchActionGroup(row, 'reopen')">批量转待处理</el-button>
            </el-button-group>
            <el-button-group v-else-if="!row._isDirRow" size="small">
              <el-button v-if="row.status !== 'ignored' && row.status !== 'resolved'" plain @click="ignoreIssue(row)">忽略</el-button>
              <el-button v-if="row.status !== 'resolved'" type="success" plain @click="resolveIssue(row)">解决</el-button>
              <el-button v-if="row.status === 'ignored' || row.status === 'resolved'" type="primary" plain @click="reopenIssue(row)">转为待处理</el-button>
            </el-button-group>
            <el-button-group v-else size="small">
              <el-button plain
                :disabled="row.status === 'ignored' || row.status === 'resolved'"
                @click="ignoreIssue(row)">忽略</el-button>
              <el-button type="success" plain
                :disabled="row.status === 'resolved'"
                @click="resolveIssue(row)">解决</el-button>
              <el-button v-if="row.status === 'ignored' || row.status === 'resolved'" type="primary" plain @click="reopenIssue(row)">转为待处理</el-button>
            </el-button-group>
          </template>
        </el-table-column>
      </el-table>

      <div class="pagination">
        <el-pagination
          v-model:current-page="page"
          v-model:page-size="pageSize"
          :total="total"
          layout="total, prev, pager, next"
          @current-change="loadIssues"
        />
      </div>
    </el-card>

    <!-- Recent check runs -->
    <el-card shadow="never" class="runs-card">
      <template #header>
        <span>近期检查记录</span>
        <el-button style="float: right" size="small" @click="loadRuns">刷新</el-button>
      </template>
      <el-table :data="runs" stripe size="small" style="width:100%">
        <el-table-column prop="id" label="ID" width="50" />
        <el-table-column label="范围" width="120">
          <template #default="{ row }">
            {{ row.sync_group_id ? groupName(row.sync_group_id) : '全量' }}
          </template>
        </el-table-column>
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag :type="runStatusType(row.status)" size="small">{{ runStatusLabel(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="开始时间" width="145">
          <template #default="{ row }">{{ formatTime(row.started_at) }}</template>
        </el-table-column>
        <el-table-column label="完成时间" width="145">
          <template #default="{ row }">{{ formatTime(row.finished_at) }}</template>
        </el-table-column>
        <el-table-column label="摘要" min-width="160">
          <template #default="{ row }">
            {{ parseSummary(row.summary_json) }}
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh, Search } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { checksApi, syncGroupsApi } from '../api/client'

const groups = ref([])
const issues = ref([])
const runs = ref([])
const issuesLoading = ref(false)
const runningFull = ref(false)
const total = ref(0)
const page = ref(1)
const pageSize = ref(200)

const DIR_LEVEL_CHECKER_CODES = new Set(['source_dir_unrecorded', 'target_dir_no_source'])

const groupedIssues = computed(() => {
  const map = new Map()
  const dirRows = []
  for (const issue of issues.value) {
    // Directory-level issues: show as flat row (no expand), not grouped into tree
    if (DIR_LEVEL_CHECKER_CODES.has(issue.checker_code)) {
      dirRows.push({ ...issue, _isDirRow: true, _key: `dir-issue:${issue.id}` })
      continue
    }
    const dir =
      issue.resource_dir ||
      (issue.source_path ? issue.source_path.substring(0, issue.source_path.lastIndexOf('/')) || issue.source_path : null) ||
      (issue.target_path ? issue.target_path.substring(0, issue.target_path.lastIndexOf('/')) || issue.target_path : null) ||
      '(未知目录)'
    if (!map.has(dir)) {
      map.set(dir, {
        _isGroup: true,
        _key: `dir:${dir}`,
        _dir: dir,
        checker_codes: [],
        severities: [],
        sync_group_ids: [],
        updated_at: null,
        children: [],
        _cs: new Set(),
        _ss: new Set(),
        _gs: new Set(),
      })
    }
    const g = map.get(dir)
    g.children.push({ ...issue, _key: `issue:${issue.id}` })
    if (!g._cs.has(issue.checker_code)) { g._cs.add(issue.checker_code); g.checker_codes.push(issue.checker_code) }
    if (!g._ss.has(issue.severity)) { g._ss.add(issue.severity); g.severities.push(issue.severity) }
    if (issue.sync_group_id != null && !g._gs.has(issue.sync_group_id)) { g._gs.add(issue.sync_group_id); g.sync_group_ids.push(issue.sync_group_id) }
    if (!g.updated_at || issue.updated_at > g.updated_at) g.updated_at = issue.updated_at
  }
  const treeRows = Array.from(map.values()).map(({ _cs, _ss, _gs, ...rest }) => rest)
  return [...dirRows, ...treeRows]
})
const filterStatus = ref('open')
const filterChecker = ref(null)

onMounted(async () => {
  await loadGroups()
  await Promise.all([loadIssues(), loadRuns()])
})

async function loadGroups() {
  try {
    const { data } = await syncGroupsApi.list()
    groups.value = data || []
  } catch {
    /* ignore */
  }
}

async function loadIssues() {
  issuesLoading.value = true
  try {
    const params = {
      page: page.value,
      page_size: pageSize.value,
    }
    if (filterStatus.value) params.status = filterStatus.value
    if (filterChecker.value) params.checker_code = filterChecker.value
    const { data } = await checksApi.listIssues(params)
    issues.value = data.items || []
    total.value = data.total || 0
  } catch {
    ElMessage.error('加载检查问题失败')
  } finally {
    issuesLoading.value = false
  }
}

async function loadRuns() {
  try {
    const { data } = await checksApi.listRuns({ page: 1, page_size: 10 })
    runs.value = data.items || []
  } catch {
    /* ignore */
  }
}

async function pollUntilCheckDone(triggerTime, onDone) {
  const MAX_WAIT_MS = 90_000
  const POLL_INTERVAL_MS = 2_000
  const deadline = Date.now() + MAX_WAIT_MS
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS))
    try {
      const { data } = await checksApi.listRuns({ page: 1, page_size: 1 })
      const run = data?.items?.[0]
      if (run && new Date(run.started_at + (run.started_at.endsWith('Z') ? '' : 'Z')) >= triggerTime) {
        if (run.status === 'completed' || run.status === 'failed') {
          onDone()
          return
        }
      }
    } catch {
      // network hiccup — keep polling
    }
  }
  // Timeout — refresh anyway
  onDone()
}

async function runFullCheck() {
  runningFull.value = true
  const triggerTime = new Date()
  try {
    await checksApi.runFull()
    ElMessage.success('全量检查任务已进入队列，完成后将自动刷新')
    pollUntilCheckDone(triggerTime, () => {
      loadRuns()
      loadIssues()
      runningFull.value = false
    })
  } catch {
    ElMessage.error('启动失败')
    runningFull.value = false
  }
}

function resetFilters() {
  filterStatus.value = 'open'
  filterChecker.value = null
  page.value = 1
  loadIssues()
}

async function ignoreIssue(row) {
  try {
    await checksApi.ignoreIssue(row.id)
    ElMessage.success('已忽略')
    row.status = 'ignored'
  } catch (e) {
    ElMessage.error(e?.response?.data?.detail || '操作失败')
  }
}

async function resolveIssue(row) {
  try {
    await checksApi.resolveIssue(row.id)
    ElMessage.success('已标记解决')
    row.status = 'resolved'
  } catch (e) {
    ElMessage.error(e?.response?.data?.detail || '操作失败')
  }
}

async function reopenIssue(row) {
  try {
    await checksApi.reopenIssue(row.id)
    ElMessage.success('已转为待处理')
    row.status = 'open'
  } catch (e) {
    ElMessage.error(e?.response?.data?.detail || '操作失败')
  }
}

async function batchActionGroup(groupRow, action) {
  let ids
  if (action === 'reopen') {
    ids = (groupRow.children || [])
      .filter((c) => c.status === 'ignored' || c.status === 'resolved')
      .map((c) => c.id)
  } else {
    ids = (groupRow.children || [])
      .filter((c) => c.status !== 'resolved' && (action !== 'ignore' || c.status !== 'ignored'))
      .map((c) => c.id)
  }
  if (!ids.length) {
    ElMessage.info('该目录下无可操作的问题')
    return
  }
  try {
    const { data } = await checksApi.batchAction(ids, action)
    const label = action === 'resolve' ? '解决' : action === 'ignore' ? '忽略' : '转待处理'
    ElMessage.success(`已批量${label} ${data.updated} 个问题`)
    const newStatus = action === 'resolve' ? 'resolved' : action === 'ignore' ? 'ignored' : 'open'
    for (const child of groupRow.children) {
      if (ids.includes(child.id)) child.status = newStatus
    }
  } catch (e) {
    ElMessage.error(e?.response?.data?.detail || '批量操作失败')
  }
}

function basename(path) {
  if (!path) return ''
  return path.split('/').pop() || path
}

function groupStatusSummary(group) {
  const stats = {}
  for (const child of group.children) {
    stats[child.status] = (stats[child.status] || 0) + 1
  }
  return Object.entries(stats).map(([k, v]) => `${statusLabel(k)} ${v}`).join(' / ')
}

function checkerLabel(code) {
  const map = {
    source_unrecorded: '源文件未使用',
    source_dir_unrecorded: '源目录未使用',
    links_orphans: '孤立硬链接',
    media_path_sanity: '目标路径异常',
    target_no_source: '目标文件无源',
    target_dir_no_source: '目标目录无源',
  }
  return map[code] || code
}

function groupName(id) {
  const g = groups.value.find((x) => x.id === id)
  return g ? g.name : `#${id}`
}

function statusLabel(status) {
  const map = {
    open: '待处理',
    ignored: '已忽略',
    resolved: '已解决',
  }
  return map[status] || status
}

function statusTagType(status) {
  const map = { open: 'danger', ignored: 'info', resolved: 'success' }
  return map[status] || 'info'
}

function runStatusType(status) {
  const map = { running: 'warning', completed: 'success', failed: 'danger' }
  return map[status] || 'info'
}

function runStatusLabel(status) {
  const map = { running: '运行中', completed: '已完成', failed: '失败' }
  return map[status] || status
}

function parseSummary(json) {
  if (!json) return '-'
  try {
    const obj = JSON.parse(json)
    let s = `发现 ${obj.found ?? 0} 个，新增 ${obj.opened ?? 0}，重开 ${obj.reopened ?? 0}`
    if (obj.resolved) s += `，自动关闭 ${obj.resolved}`
    return s
  } catch {
    return json
  }
}

function formatTime(t) {
  if (!t) return '-'
  // Backend stores UTC naive datetimes (no tz suffix). Append 'Z' so dayjs
  // parses as UTC and auto-converts to local time.
  const s = String(t)
  const normalized = /[Zz]$|[+-]\d{2}:?\d{2}$/.test(s) ? s : s + 'Z'
  return dayjs(normalized).format('MM-DD HH:mm:ss')
}
</script>

<style scoped>
.checks-view {
  padding: 0;
}
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}
.header-actions {
  display: flex;
  gap: 8px;
  align-items: center;
}
.page-title {
  font-size: 22px;
  font-weight: 600;
  margin: 0;
}
.filter-card {
  margin-bottom: 16px;
}
.table-card {
  margin-bottom: 16px;
}
.runs-card {
  margin-bottom: 16px;
}
.pagination {
  display: flex;
  justify-content: flex-end;
  margin-top: 12px;
}
.path-cell {
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.path-row {
  display: flex;
  align-items: baseline;
  gap: 4px;
  min-width: 0;
}
.path-label {
  flex-shrink: 0;
  font-size: 11px;
  font-weight: 600;
  color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
  border-radius: 3px;
  padding: 0 4px;
  line-height: 18px;
}
.path-label.target {
  color: var(--el-color-success);
  background: var(--el-color-success-light-9);
}
.path-text {
  font-size: 12px;
  word-break: break-all;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
}
.dir-path {
  font-size: 13px;
  font-weight: 600;
  color: var(--el-text-color-primary);
  word-break: break-all;
}
.group-status-summary {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.tag-stack {
  display: flex;
  flex-wrap: wrap;
  gap: 3px;
}
.muted {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
