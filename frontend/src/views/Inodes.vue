<template>
  <div class="inodes-page">
    <div class="header">
      <h1 class="page-title">Inode 管理</h1>
    </div>

    <div class="toolbar">
      <div class="left-tools">
        <el-input
          v-model="searchQuery"
          placeholder="搜索源/目标路径..."
          clearable
          style="width: 260px"
          @clear="onFilterChanged"
          @keyup.enter="onFilterChanged"
        >
          <template #append>
            <el-button @click="onFilterChanged">
              <el-icon><Search /></el-icon>
            </el-button>
          </template>
        </el-input>
        <el-select v-model="syncGroupId" clearable placeholder="同步组" style="width: 160px" @change="onFilterChanged">
          <el-option v-for="g in syncGroups" :key="g.id" :label="g.name" :value="g.id" />
        </el-select>
        <el-select v-model="hasTarget" clearable placeholder="目标路径" style="width: 140px" @change="onFilterChanged">
          <el-option label="有目标" :value="true" />
          <el-option label="无目标" :value="false" />
        </el-select>
      </div>

      <div class="right-actions">
        <el-button type="warning" :loading="cleaning" @click="cleanupInodes">
          <el-icon><Delete /></el-icon>
          清理无效记录
        </el-button>
        <el-button type="primary" @click="loadInodes">
          <el-icon><Refresh /></el-icon>
        </el-button>
      </div>
    </div>

    <el-card shadow="never" class="table-card">
      <el-table v-loading="loading" :data="pagedRows" stripe style="width: 100%">
        <el-table-column label="资源" min-width="520">
          <template #default="{ row }">
            <div class="title-cell" @click="openDrawer(row)">
              <img
                v-if="getPosterUrl(row)"
                class="poster-thumb"
                :src="getPosterUrl(row)"
                alt="poster"
                loading="lazy"
              />
              <div v-else class="poster-fallback">
                <el-icon class="media-icon" :size="20"><Monitor /></el-icon>
              </div>

              <div class="title-info">
                <div class="main-title">{{ getResourceName(row) }}</div>
                <div class="sub-info">
                  <el-tag size="small" effect="plain" class="type-tag">
                    {{ getMediaTypeLabel(row) }}
                  </el-tag>
                  <span class="season-link">{{ row.season_label }}</span>
                  <span v-if="getTmdbId(row)">TMDB: {{ getTmdbId(row) }}</span>
                  <span>记录 {{ row.record_count }}</span>
                  <span>同步组 {{ row.sync_group_id || '-' }}</span>
                </div>
              </div>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="最近更新时间" width="180" align="right">
          <template #default="{ row }">{{ formatDate(row.latest_updated_at) }}</template>
        </el-table-column>
      </el-table>

      <div class="pagination">
        <el-pagination
          v-model:current-page="currentPage"
          v-model:page-size="pageSize"
          :page-sizes="[20, 50, 100]"
          layout="total, sizes, prev, pager, next"
          :total="total"
          @size-change="refreshPagedRows"
          @current-change="refreshPagedRows"
        />
      </div>
    </el-card>

    <el-drawer
      v-model="drawerVisible"
      direction="rtl"
      size="58%"
      :title="drawerTitle"
      destroy-on-close
    >
      <div class="drawer-path">{{ drawerDir }}</div>
      <div class="drawer-toolbar">
        <el-input
          v-model="drawerSearch"
          placeholder="搜索该目录文件..."
          clearable
          style="width: 260px"
        />
        <el-button
          type="danger"
          plain
          :disabled="!selectedRows.length"
          @click="deleteSelected"
        >
          删除选中 ({{ selectedRows.length }})
        </el-button>
      </div>

      <el-table
        :data="filteredDrawerItems"
        v-loading="loading"
        stripe
        style="width: 100%"
        @selection-change="handleSelectionChange"
      >
        <el-table-column type="selection" width="44" />
        <el-table-column prop="id" label="ID" width="80" />
        <el-table-column label="源文件" min-width="220" show-overflow-tooltip>
          <template #default="{ row }">{{ extractFilename(row.source_path) }}</template>
        </el-table-column>
        <el-table-column label="目标文件" min-width="220" show-overflow-tooltip>
          <template #default="{ row }">{{ extractFilename(row.target_path) }}</template>
        </el-table-column>
        <el-table-column prop="sync_group_id" label="同步组ID" width="100" />
        <el-table-column label="更新时间" width="180">
          <template #default="{ row }">{{ formatDate(row.updated_at) }}</template>
        </el-table-column>
        <el-table-column label="操作" width="90" fixed="right">
          <template #default="{ row }">
            <el-button type="danger" size="small" @click="deleteInode(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, computed, reactive, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { inodesApi, syncGroupsApi, mediaApi } from '../api/client'
import dayjs from 'dayjs'

const rawInodes = ref([])
const groupedRows = ref([])
const pagedRows = ref([])
const syncGroups = ref([])
const loading = ref(false)
const cleaning = ref(false)
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const searchQuery = ref('')
const syncGroupId = ref(undefined)
const hasTarget = ref(undefined)
const selectedRows = ref([])

const drawerVisible = ref(false)
const drawerTitle = ref('')
const drawerDir = ref('')
const drawerGroupKey = ref('')
const drawerSearch = ref('')

const metaCache = reactive({})
const posterCache = reactive({})

function normalizePath(path) {
  return String(path || '').replace(/\\/g, '/').replace(/\/+$/, '')
}

function extractFilename(path) {
  if (!path) return ''
  return String(path).split(/[/\\]/).pop()
}

function extractTargetDir(path) {
  if (!path) return ''
  return String(path).replace(/[/\\][^/\\]+$/, '')
}

function extractSeasonDir(path) {
  if (!path) return ''
  const normalized = normalizePath(path)
  const m = normalized.match(/^(.*\/Season\s+\d{1,2})(?:\/|$)/i)
  if (m?.[1]) return m[1]
  return extractTargetDir(path)
}

function extractSeasonLabel(path) {
  const normalized = normalizePath(path)
  const m = normalized.match(/\/(Season\s+\d{1,2})(?:\/|$)/i)
  if (!m?.[1]) return 'Season --'
  const n = m[1].match(/\d+/)?.[0]
  return n ? `Season ${String(Number(n)).padStart(2, '0')}` : m[1]
}

function extractSeasonNumber(label) {
  const n = String(label || '').match(/\d+/)?.[0]
  return n ? Number(n) : null
}

function extractResourceName(path, type) {
  if (!path) return '未识别资源'
  if (type === 'movie') {
    const base = extractFilename(path).replace(/\.[^/.]+$/, '')
    return base.replace(/\s*-\s*[\w.\- ]{2,20}$/i, '').trim() || '未识别资源'
  }
  const seasonDir = extractSeasonDir(path)
  const showDir = extractTargetDir(seasonDir)
  return String(extractFilename(showDir) || '').replace(/\s*\[tmdbid=\d+\]/i, '').trim() || '未识别资源'
}

function buildPosterUrl(path) {
  if (!path) return ''
  return `https://image.tmdb.org/t/p/w185${path}`
}

function inferMediaTypeByPath(targetPath) {
  return /\/Season\s+\d+/i.test(normalizePath(targetPath)) ? 'tv' : 'movie'
}

function toGroupKey(inode) {
  const type = inferMediaTypeByPath(inode?.target_path || '')
  const dir = type === 'tv' ? extractSeasonDir(inode?.target_path || '') : extractTargetDir(inode?.target_path || '')
  return `${inode?.sync_group_id || 0}:${type}:${normalizePath(dir)}`
}

function splitPathParts(path) {
  return normalizePath(path).split('/').filter(Boolean)
}

function isAncestorPath(ancestor, child) {
  const a = normalizePath(ancestor).toLowerCase()
  const c = normalizePath(child).toLowerCase()
  if (!a || !c) return false
  return c === a || c.startsWith(`${a}/`)
}

function buildGroups(rows) {
  const collapseByBucket = new Map()
  const rowsWithDir = rows.map((inode) => {
    const targetPath = inode?.target_path || ''
    const fallbackType = inferMediaTypeByPath(targetPath)
    const dir = fallbackType === 'tv' ? extractSeasonDir(targetPath) : extractTargetDir(targetPath)
    const bucket = `${inode?.sync_group_id || 0}:${fallbackType}`
    if (!collapseByBucket.has(bucket)) collapseByBucket.set(bucket, [])
    if (dir) collapseByBucket.get(bucket).push(normalizePath(dir))
    return { inode, targetPath, fallbackType, dir, bucket }
  })

  const canonicalDirMap = new Map()
  for (const [bucket, dirs] of collapseByBucket.entries()) {
    const uniq = Array.from(new Set(dirs)).sort((a, b) => splitPathParts(a).length - splitPathParts(b).length)
    const selected = []
    for (const dir of uniq) {
      const parent = selected.find((x) => isAncestorPath(x, dir))
      if (parent) canonicalDirMap.set(`${bucket}|${dir}`, parent)
      else {
        selected.push(dir)
        canonicalDirMap.set(`${bucket}|${dir}`, dir)
      }
    }
  }

  const map = new Map()

  for (const x of rowsWithDir) {
    const { inode, targetPath, fallbackType, dir, bucket } = x
    if (!targetPath) continue

    const normalizedDir = normalizePath(dir)
    const canonicalDir = canonicalDirMap.get(`${bucket}|${normalizedDir}`) || normalizedDir
    const key = `${inode?.sync_group_id || 0}:${fallbackType}:${canonicalDir}`
    const latest = new Date(inode.updated_at || inode.created_at || 0).getTime()

    if (!map.has(key)) {
      map.set(key, {
        key,
        sync_group_id: inode.sync_group_id,
        target_dir: canonicalDir,
        season_label: extractSeasonLabel(targetPath),
        resource_name: extractResourceName(targetPath, fallbackType),
        fallback_type: fallbackType,
        record_count: 1,
        latest_updated_at: inode.updated_at || inode.created_at,
        latest_ts: latest,
      })
      continue
    }

    const item = map.get(key)
    item.record_count += 1
    if (latest > item.latest_ts) {
      item.latest_ts = latest
      item.latest_updated_at = inode.updated_at || inode.created_at
    }
  }

  groupedRows.value = Array.from(map.values()).sort((a, b) => b.latest_ts - a.latest_ts)
  refreshPagedRows()
  for (const row of groupedRows.value) loadMeta(row)
}

function refreshPagedRows() {
  total.value = groupedRows.value.length
  const start = (currentPage.value - 1) * pageSize.value
  pagedRows.value = groupedRows.value.slice(start, start + pageSize.value)
}

function onFilterChanged() {
  currentPage.value = 1
  loadInodes()
}

function getMetaKey(row) {
  return row?.key || ''
}

function getPosterUrl(row) {
  const key = getMetaKey(row)
  if (!key) return ''
  if (!posterCache[key]) {
    loadMeta(row)
    return ''
  }
  if (posterCache[key] === '__loading__') return ''
  return posterCache[key]
}

function getTmdbId(row) {
  const key = getMetaKey(row)
  return metaCache[key]?.tmdb_id || null
}

function getMediaTypeLabel(row) {
  const key = getMetaKey(row)
  const t = metaCache[key]?.media_type || row?.fallback_type || 'tv'
  return t === 'movie' ? 'Movie' : 'TV'
}

function getResourceName(row) {
  const key = getMetaKey(row)
  const title = metaCache[key]?.title
  const year = metaCache[key]?.year
  if (title) return year ? `${title} (${year})` : title
  return row?.resource_name || '未识别资源'
}

async function loadMeta(row) {
  const key = getMetaKey(row)
  if (!key || metaCache[key]?.__loaded || posterCache[key] === '__loading__') return
  posterCache[key] = '__loading__'

  try {
    const { data } = await mediaApi.byTargetDir({ target_dir: row.target_dir, limit: 1 })
    const item = (data?.items || [])[0]
    if (!item?.tmdb_id) {
      metaCache[key] = { __loaded: true, media_type: row.fallback_type }
      posterCache[key] = ''
      return
    }

    const mediaType = item.type || row.fallback_type || 'tv'
    const tmdbId = item.tmdb_id
    let poster = ''

    const seasonNum = mediaType === 'tv' ? extractSeasonNumber(row?.season_label) : null
    if (seasonNum !== null) {
      const { data: seasonData } = await mediaApi.seasonPoster({ tmdb_id: tmdbId, season: seasonNum })
      poster = buildPosterUrl(seasonData?.poster_path)
    }

    const { data: tmdb } = await mediaApi.searchTmdb({ q: String(tmdbId), media_type: mediaType, limit: 1 })
    const first = (tmdb?.items || [])[0]
    if (!poster) poster = buildPosterUrl(first?.poster_path)

    metaCache[key] = {
      __loaded: true,
      media_type: mediaType,
      tmdb_id: tmdbId,
      title: first?.title || '',
      year: first?.year || null,
    }
    posterCache[key] = poster || ''
  } catch {
    metaCache[key] = { __loaded: true, media_type: row.fallback_type }
    posterCache[key] = ''
  }
}

const filteredDrawerItems = computed(() => {
  const keyword = String(drawerSearch.value || '').trim().toLowerCase()
  const base = rawInodes.value.filter((x) => x.__group_key === drawerGroupKey.value)
  if (!keyword) return base
  return base.filter((row) => {
    const source = extractFilename(row.source_path).toLowerCase()
    const target = extractFilename(row.target_path).toLowerCase()
    return source.includes(keyword) || target.includes(keyword)
  })
})

function handleSelectionChange(val) {
  selectedRows.value = val || []
}

function openDrawer(row) {
  drawerVisible.value = true
  drawerTitle.value = `${getResourceName(row)} · ${row.season_label}`
  drawerDir.value = row.target_dir || '未生成目标目录'
  drawerGroupKey.value = row.key
  drawerSearch.value = ''
  selectedRows.value = []
}

async function deleteSelected() {
  if (!selectedRows.value.length) return

  try {
    await ElMessageBox.confirm(`确定要删除选中的 ${selectedRows.value.length} 条记录吗？`, '批量删除', { type: 'warning' })
    loading.value = true
    await Promise.all(selectedRows.value.map((row) => inodesApi.delete(row.id)))
    ElMessage.success('批量删除完成')
    selectedRows.value = []
    await loadInodes()
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error('部分删除失败或出错')
      await loadInodes()
    }
  } finally {
    loading.value = false
  }
}

async function deleteInode(row) {
  try {
    await ElMessageBox.confirm(
      '删除此 Inode 记录将导致下次扫描时重新处理该文件（如果文件仍存在）。确定删除吗？',
      '警告',
      { type: 'warning' },
    )
    await inodesApi.delete(row.id)
    ElMessage.success('已删除')
    await loadInodes()
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('删除失败')
  }
}

async function loadInodes() {
  loading.value = true
  try {
    const { data } = await inodesApi.list({
      skip: 0,
      limit: 2000,
      search: searchQuery.value || undefined,
      sync_group_id: syncGroupId.value ?? undefined,
      has_target: typeof hasTarget.value === 'boolean' ? hasTarget.value : undefined,
    })

    rawInodes.value = (data.items || [])
      .filter((x) => String(x?.target_path || '').trim())
      .map((x) => ({ ...x, __group_key: toGroupKey(x) }))

    buildGroups(rawInodes.value)
  } catch {
    ElMessage.error('加载失败')
    rawInodes.value = []
    groupedRows.value = []
    pagedRows.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

async function cleanupInodes() {
  try {
    await ElMessageBox.confirm(
      '将检查所有记录，删除源文件已不存在的 Inode 记录。这可能需要一点时间。',
      '确认清理',
      { type: 'info' },
    )
    cleaning.value = true
    const { data } = await inodesApi.cleanup()
    ElMessage.success(data.message)
    await loadInodes()
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('清理失败')
  } finally {
    cleaning.value = false
  }
}

async function loadSyncGroups() {
  try {
    const { data } = await syncGroupsApi.list()
    syncGroups.value = data || []
  } catch {
    syncGroups.value = []
  }
}

function formatDate(date) {
  return dayjs(date).format('YYYY-MM-DD HH:mm')
}

onMounted(() => {
  loadSyncGroups()
  loadInodes()
})
</script>

<style scoped>
.inodes-page {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 24px;
  flex-wrap: wrap;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 24px;
  flex-wrap: wrap;
}

.left-tools {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
}

.right-actions {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}

.table-card {
  border: none;
  background: transparent;
}

.title-cell {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  cursor: pointer;
}

.poster-thumb {
  width: 44px;
  height: 66px;
  object-fit: cover;
  border-radius: 6px;
  flex-shrink: 0;
  background: #e5e7eb;
}

.poster-fallback {
  width: 44px;
  height: 66px;
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  background: #f1f5f9;
}

.media-icon {
  color: #94a3b8;
}

.title-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.main-title {
  font-weight: 600;
  font-size: 14px;
  color: #111827;
}

.sub-info {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
  font-size: 12px;
  color: #64748b;
}

.season-link {
  color: #2563eb;
  font-weight: 500;
}

.type-tag {
  transform: scale(0.9);
  transform-origin: left;
}

.pagination {
  margin-top: 24px;
  display: flex;
  justify-content: flex-end;
}

.drawer-path {
  margin-bottom: 10px;
  color: #64748b;
  word-break: break-all;
}

.drawer-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 12px;
}
</style>
