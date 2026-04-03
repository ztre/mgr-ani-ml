<template>
  <div class="media">
    <div class="header">
      <h1 class="page-title">媒体记录</h1>
      <div class="header-actions">
        <el-input
          v-model="filters.search"
          placeholder="搜索文件名..."
          clearable
          style="width: 220px"
          @clear="onFilterChanged"
          @keyup.enter="onFilterChanged"
        />
        <el-select v-model="filters.type" placeholder="类型" clearable style="width: 100px" @change="onFilterChanged">
          <el-option label="TV" value="tv" />
          <el-option label="电影" value="movie" />
        </el-select>
        <el-select v-model="filters.category" placeholder="分类" style="width: 120px" @change="onFilterChanged">
          <el-option label="全部" value="all" />
          <el-option label="正片" value="main" />
          <el-option label="SPs/Extras" value="sps" />
        </el-select>
        <el-button @click="loadMedia" :loading="loading">
          <el-icon><Refresh /></el-icon>
        </el-button>
        <el-button type="warning" plain @click="deduplicateRecords">
          记录去重
        </el-button>
      </div>
    </div>

    <el-card shadow="never" class="table-card">
      <el-table :data="seasonRows" stripe v-loading="loading" style="width: 100%">
        <el-table-column label="资源" min-width="520">
          <template #default="{ row }">
            <div class="title-cell" @click="openSeasonDrawer(row)">
              <img
                v-if="getPosterUrl(row)"
                class="poster-thumb"
                :src="getPosterUrl(row)"
                alt="poster"
                loading="lazy"
              />
              <div v-else class="poster-fallback">
                <el-icon class="media-icon" :size="20">
                  <Monitor v-if="row.type === 'tv'" />
                  <Film v-else />
                </el-icon>
              </div>

              <div class="title-info">
                <div class="main-title">{{ getResourceName(row) }}</div>
                <div class="sub-info">
                  <el-tag size="small" effect="plain" class="type-tag">
                    {{ row.type === 'tv' ? 'TV' : 'Movie' }}
                  </el-tag>
                  <span class="season-link">{{ row.season_summary || row.season_label }}</span>
                  <span v-if="row.type === 'tv'">季数 {{ row.season_count || 0 }}</span>
                  <span v-if="row.tmdb_id">TMDB: {{ row.tmdb_id }}</span>
                  <span>记录 {{ row.record_count }}</span>
                </div>
              </div>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="最近更新时间" width="180" align="right">
          <template #default="{ row }">
            {{ formatTime(row.latest_updated_at) }}
          </template>
        </el-table-column>

        <el-table-column label="操作" width="130" align="center" fixed="right">
          <template #default="{ row }">
            <el-dropdown trigger="click" @command="(cmd) => onDeleteResourceCommand(row, cmd)">
              <el-button type="danger" link size="small">删除资源</el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="records">仅删除该资源记录</el-dropdown-item>
                  <el-dropdown-item command="records_and_links">删除该资源 + 硬链接</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
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
          @size-change="refreshPagedRows"
          @current-change="refreshPagedRows"
        />
      </div>
    </el-card>

    <el-dialog v-model="fixDialogVisible" title="手动修正识别" width="500px">
      <el-form :model="fixForm" label-width="100px">
        <el-form-item label="文件名">
          <div class="text-ellipsis" :title="currentFixRow?.original_path">
            {{ extractFilename(currentFixRow?.original_path) }}
          </div>
        </el-form-item>
        <el-form-item label="TMDB ID" required>
          <el-input v-model.number="fixForm.tmdb_id" placeholder="例如: 45782" />
        </el-form-item>
        <el-form-item label="标题" required>
          <el-input v-model="fixForm.title" placeholder="TMDB 标准标题 (如: 刀剑神域)" />
        </el-form-item>
        <el-form-item label="年份" required>
          <el-input v-model.number="fixForm.year" placeholder="首播年份 (如: 2012)" />
        </el-form-item>
        <el-form-item label="季号">
          <el-input-number v-model="fixForm.season" :min="0" />
        </el-form-item>
        <el-form-item label="集号">
          <el-input-number v-model="fixForm.episode" :min="0" />
        </el-form-item>
        <el-form-item label="集号偏移">
          <el-input-number v-model="fixForm.episode_offset" :min="0" />
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="fixDialogVisible = false">取消</el-button>
          <el-button type="primary" :loading="fixing" @click="submitFix">提交修正</el-button>
        </span>
      </template>
    </el-dialog>

    <el-drawer
      v-model="seasonDrawerVisible"
      direction="rtl"
      size="58%"
      :title="seasonDrawerTitle"
      destroy-on-close
    >
      <div class="drawer-path">{{ seasonDrawerDir }}</div>
      <div class="drawer-header-actions">
        <div class="drawer-filters">
          <el-input
            v-model="seasonDrawerSearch"
            placeholder="搜索文件名..."
            clearable
            style="width: 220px"
          />
          <el-select
            v-if="seasonDrawerType === 'tv'"
            v-model="seasonDrawerSeason"
            placeholder="Season"
            style="width: 140px"
          >
            <el-option label="全部 Season" value="all" />
            <el-option
              v-for="label in seasonDrawerSeasonOptions"
              :key="label"
              :label="label"
              :value="label"
            />
          </el-select>
          <el-select v-model="seasonDrawerCategory" placeholder="分类" style="width: 120px">
            <el-option label="全部" value="all" />
            <el-option label="正片" value="main" />
            <el-option label="SPs/Extras" value="sps" />
          </el-select>
          <el-button @click="refreshSeasonDrawerItems" :loading="seasonDrawerLoading">
            <el-icon><Refresh /></el-icon>
          </el-button>
          <el-button type="primary" plain @click="openDirFixDialog" :disabled="!seasonDrawerDir">
            整体修正
          </el-button>
        </div>
        <div class="drawer-danger-actions">
          <el-button
            type="danger"
            plain
            size="small"
            :disabled="seasonDrawerSelectedRows.length === 0"
            @click="deleteDrawerSelected(false)"
          >
            删除选中记录
          </el-button>
          <el-button
            type="danger"
            size="small"
            :disabled="seasonDrawerSelectedRows.length === 0"
            @click="deleteDrawerSelected(true)"
          >
            删除选中+硬链接
          </el-button>
          <el-dropdown trigger="click" @command="onDeleteDrawerResourceCommand">
            <el-button type="danger" plain size="small">
              删除当前资源
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="records">仅删除该资源记录</el-dropdown-item>
                <el-dropdown-item command="records_and_links">删除该资源 + 硬链接</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
      </div>
      <el-table
        :data="filteredSeasonDrawerItems"
        v-loading="seasonDrawerLoading"
        stripe
        style="width: 100%"
        @selection-change="onDrawerSelectionChange"
      >
        <el-table-column type="selection" width="50" />
        <el-table-column label="文件名" min-width="300">
          <template #default="{ row }">
            <div class="path-flow">
              <div class="path-row source" :title="row.original_path">
                <span class="label">源</span>
                <span class="path">{{ extractFilename(row.original_path) }}</span>
              </div>
              <div class="arrow-row" v-if="row.target_path">
                <el-icon><Bottom /></el-icon>
              </div>
              <div class="path-row target" v-if="row.target_path" :title="row.target_path">
                <span class="label">标</span>
                <span class="path">{{ extractFilename(row.target_path) }}</span>
              </div>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="方式" width="90" align="center">
          <template #default>
            <el-tag effect="light" round>硬链接</el-tag>
          </template>
        </el-table-column>

        <el-table-column label="大小" width="110" align="right">
          <template #default="{ row }">{{ formatSize(row.size) }}</template>
        </el-table-column>

        <el-table-column label="时间/状态" width="150" align="right">
          <template #default="{ row }">
            <div class="status-cell">
              <div class="time">{{ formatTime(row.created_at) }}</div>
              <el-tag :type="statusType(row.status)" size="small" effect="dark">
                {{ statusText(row.status) }}
              </el-tag>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="操作" width="130" align="center" fixed="right">
          <template #default="{ row }">
            <el-button
              type="primary"
              link
              size="small"
              :disabled="row.is_directory_pending"
              :title="row.is_directory_pending ? '目录级待办不支持直接修正，请先手动整理目录后重扫' : ''"
              @click="openFixDialog(row)"
            >
              修正
            </el-button>
            <el-dropdown trigger="click" @command="(cmd) => onDeleteDrawerRowCommand(row, cmd)">
              <el-button type="danger" link size="small">删除</el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="records">仅删记录</el-dropdown-item>
                  <el-dropdown-item command="records_and_links">记录+硬链接</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </template>
        </el-table-column>
      </el-table>
    </el-drawer>

    <el-dialog v-model="dirFixDialogVisible" title="整组资源修正" width="520px">
      <el-form :model="dirFixForm" label-width="110px">
        <el-form-item label="资源目录">
          <div class="text-ellipsis" :title="seasonDrawerDir">{{ seasonDrawerDir || '-' }}</div>
        </el-form-item>
        <el-form-item label="TMDB ID" required>
          <el-input v-model.number="dirFixForm.tmdb_id" placeholder="例如: 45782" />
        </el-form-item>
        <el-form-item label="标题" required>
          <el-input v-model="dirFixForm.title" placeholder="TMDB 标准标题" />
        </el-form-item>
        <el-form-item label="年份" required>
          <el-input v-model.number="dirFixForm.year" placeholder="首播年份 (如: 2012)" />
        </el-form-item>
        <el-form-item label="季号 (Season)">
          <el-input-number v-model="dirFixForm.season" :min="0" />
        </el-form-item>
        <el-form-item label="Episode Offset">
          <el-input-number v-model="dirFixForm.episode_offset" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dirFixDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="dirFixing" @click="submitDirFix">提交修正</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { mediaApi, syncGroupsApi } from '../api/client'
import { ElMessage, ElMessageBox } from 'element-plus'
import dayjs from 'dayjs'
import { getCachedMeta, getCachedPoster, setCachedMeta, setCachedPoster } from '../utils/tmdbUiCache'

const route = useRoute()
const rawItems = ref([])
const allSeasonRows = ref([])
const seasonRows = ref([])
const total = ref(0)
const loading = ref(false)
const page = ref(1)
const pageSize = ref(20)
const filters = reactive({ type: '', category: 'all', search: '' })

const fixDialogVisible = ref(false)
const fixing = ref(false)
const currentFixRow = ref(null)
const dirFixDialogVisible = ref(false)
const dirFixing = ref(false)
const seasonDrawerVisible = ref(false)
const seasonDrawerLoading = ref(false)
const seasonDrawerTitle = ref('')
const seasonDrawerDir = ref('')
const seasonDrawerType = ref('')
const seasonDrawerItems = ref([])
const seasonDrawerSelectedRows = ref([])
const seasonDrawerSearch = ref('')
const seasonDrawerCategory = ref('all')
const seasonDrawerSeason = ref('all')
const posterCache = reactive({})
const tmdbMetaCache = reactive({})
const syncGroupRootNames = reactive({})
const globalRootNames = ref(new Set())
const fixForm = reactive({ tmdb_id: '', title: '', year: '', season: 1, episode: 1, episode_offset: undefined })
const dirFixForm = reactive({ tmdb_id: '', title: '', year: '', season: 1, episode_offset: undefined })
const AUX_RESOURCE_DIRS = new Set(['extras', 'specials', 'trailers', 'interviews'])

function extractFilename(path) {
  if (!path) return ''
  return path.split(/[/\\]/).pop()
}

function extractRootName(path) {
  if (!path) return ''
  const normalized = String(path).replace(/\\/g, '/').replace(/\/+$/, '')
  if (!normalized) return ''
  const seg = normalized.split('/').filter(Boolean)
  return (seg.pop() || '').trim()
}

function normalizePath(path) {
  return String(path || '').replace(/\\/g, '/').replace(/\/+$/, '')
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

function extractTargetDir(path) {
  if (!path) return ''
  const parts = path.split(/[/\\]/).filter(Boolean)
  if (parts.length <= 1) return path
  return path.replace(/[/\\][^/\\]+$/, '')
}

function extractSeasonDir(path) {
  if (!path) return ''
  const normalized = String(path).replace(/\\/g, '/')
  const m = normalized.match(/^(.*\/Season\s+\d{1,2})(?:\/|$)/i)
  if (m?.[1]) return m[1]
  return extractTargetDir(path)
}

function extractShowDir(path) {
  if (!path) return ''
  const seasonDir = extractSeasonDir(path)
  if (!seasonDir || seasonDir === path) return extractTargetDir(path)
  return extractTargetDir(seasonDir)
}

function extractMovieResourceDir(path) {
  if (!path) return ''
  const targetDir = extractTargetDir(path)
  const leaf = extractFilename(targetDir).toLowerCase()
  if (AUX_RESOURCE_DIRS.has(leaf)) {
    return extractTargetDir(targetDir)
  }
  return targetDir
}

function extractSeasonLabelFromPath(path) {
  if (!path) return 'Season --'
  const normalized = String(path).replace(/\\/g, '/')
  const m = normalized.match(/\/(Season\s+\d{1,2})(?:\/|$)/i)
  if (!m?.[1]) return 'Season --'
  const n = m[1].match(/\d+/)?.[0]
  return n ? `Season ${String(Number(n)).padStart(2, '0')}` : m[1]
}

function formatSeasonSummary(labels) {
  const uniq = Array.from(new Set((labels || []).filter(Boolean)))
  if (!uniq.length) return '-'
  if (uniq.length <= 2) return uniq.join(' / ')
  return `${uniq.length} seasons`
}

function extractDrawerSeasonBucket(path) {
  if (!path) return 'Other'
  const normalized = String(path).replace(/\\/g, '/')
  const seasonMatch = normalized.match(/\/(Season\s+\d{1,2})(?:\/|$)/i)
  if (seasonMatch?.[1]) return extractSeasonLabelFromPath(path)
  const auxMatch = normalized.match(/\/(specials|extras|trailers|interviews)(?:\/|$)/i)
  if (!auxMatch?.[1]) return 'Other'
  const labelMap = {
    specials: 'Specials',
    extras: 'Extras',
    trailers: 'Trailers',
    interviews: 'Interviews',
  }
  return labelMap[auxMatch[1].toLowerCase()] || 'Other'
}

function isDrawerExtraLike(row) {
  const normalized = String(row?.target_path || row?.original_path || '').replace(/\\/g, '/').toLowerCase()
  if (!normalized) return false
  return (
    normalized.includes('/season 00/') ||
    normalized.includes('/specials/') ||
    normalized.includes('/extras/') ||
    normalized.includes('/trailers/') ||
    normalized.includes('/interviews/')
  )
}

function drawerItemMatchesCategory(row, category) {
  if (!row) return false
  const isExtraLike = isDrawerExtraLike(row)
  if (category === 'main') return !isExtraLike
  if (category === 'sps') return isExtraLike
  return true
}

function extractResourceName(path, type) {
  if (!path) return '-'
  if (type === 'movie') {
    return String(extractFilename(extractMovieResourceDir(path)) || '-').replace(/\s*\[tmdbid=\d+\]/i, '').trim()
  }
  const seasonDir = extractSeasonDir(path)
  const showDir = extractTargetDir(seasonDir)
  return String(extractFilename(showDir) || '-').replace(/\s*\[tmdbid=\d+\]/i, '').trim()
}

function buildPosterUrl(path) {
  if (!path) return ''
  return `https://image.tmdb.org/t/p/w185${path}`
}

function extractSeasonNumber(label) {
  const n = String(label || '').match(/\d+/)?.[0]
  return n ? Number(n) : null
}

function metaKeyForRow(row) {
  return `${row?.type || 'tv'}:${row?.tmdb_id || ''}`
}

function posterKeyForRow(row) {
  const base = metaKeyForRow(row)
  const seasonNum = row?.type === 'tv' ? extractSeasonNumber(row?.season_label) : null
  return seasonNum !== null ? `${base}:s${seasonNum}` : base
}

function getPosterUrl(row) {
  const key = posterKeyForRow(row)
  if (!row?.tmdb_id || !key) return ''
  if (!posterCache[key]) {
    loadTmdbMeta(row)
    return ''
  }
  if (posterCache[key] === '__loading__') return ''
  return posterCache[key]
}

function getResourceName(row) {
  const key = metaKeyForRow(row)
  const meta = tmdbMetaCache[key]
  if (meta?.title) {
    return meta.year ? `${meta.title} (${meta.year})` : meta.title
  }
  if (row?.tmdb_id) {
    loadTmdbMeta(row.sample || row)
    return `TMDB: ${row.tmdb_id}`
  }
  const fallback = String(row?.resource_name || '').trim()
  return isSyncGroupRootFallbackName(row, fallback) ? '未识别资源' : (fallback || '未识别资源')
}

function isSyncGroupRootFallbackName(row, name) {
  const value = String(name || '').trim().toLowerCase()
  if (!value) return true
  const groupId = row?.sample?.sync_group_id
  if (groupId) {
    const roots = syncGroupRootNames[groupId]
    if (roots && roots.size) return roots.has(value)
  }
  return globalRootNames.value.has(value)
}

function saveSyncGroupRootNames(groups) {
  const allNames = new Set()
  for (const group of groups || []) {
    const gid = group?.id
    if (!gid) continue
    const names = new Set()
    const groupName = String(group?.name || '').trim()

    const srcParts = splitPathParts(group?.source)
    const dstParts = splitPathParts(group?.target)
    for (const seg of [...srcParts, ...dstParts]) {
      if (!seg) continue
      const lowered = seg.toLowerCase()
      names.add(lowered)
      allNames.add(lowered)
    }
    if (groupName) {
      const lowered = groupName.toLowerCase()
      names.add(lowered)
      allNames.add(lowered)
    }
    syncGroupRootNames[gid] = names
  }
  globalRootNames.value = allNames
}

async function loadSyncGroupsMeta() {
  try {
    const { data } = await syncGroupsApi.list()
    saveSyncGroupRootNames(data || [])
  } catch {
    // ignore: fallback name filtering becomes no-op when sync groups are unavailable
  }
}

async function loadTmdbMeta(row) {
  const tmdbId = row?.tmdb_id
  const mediaType = row?.type || 'tv'
  const metaKey = `${mediaType}:${tmdbId || ''}`
  const posterKey = posterKeyForRow(row)
  if (!tmdbId || !posterKey || posterCache[posterKey] === '__loading__') return
  const cachedMeta = getCachedMeta(metaKey)
  if (cachedMeta && !tmdbMetaCache[metaKey]) {
    tmdbMetaCache[metaKey] = cachedMeta
  }
  const cachedPoster = getCachedPoster(posterKey)
  if (cachedPoster !== null && cachedPoster !== undefined && !posterCache[posterKey]) {
    posterCache[posterKey] = cachedPoster
  }
  if (tmdbMetaCache[metaKey]?.title && typeof posterCache[posterKey] === 'string' && posterCache[posterKey] !== '__loading__') {
    return
  }
  posterCache[posterKey] = '__loading__'
  try {
    const seasonNum = mediaType === 'tv' ? extractSeasonNumber(row?.season_label) : null
    if (seasonNum !== null) {
      const { data: seasonData } = await mediaApi.seasonPoster({ tmdb_id: tmdbId, season: seasonNum })
      const seasonPoster = buildPosterUrl(seasonData?.poster_path)
      if (seasonPoster) {
        posterCache[posterKey] = seasonPoster
        setCachedPoster(posterKey, seasonPoster)
      }
    }
    const { data } = await mediaApi.searchTmdb({ q: String(tmdbId), media_type: mediaType, limit: 1 })
    const item = data?.items?.[0]
    if (!posterCache[posterKey] || posterCache[posterKey] === '__loading__') {
      posterCache[posterKey] = buildPosterUrl(item?.poster_path) || ''
      setCachedPoster(posterKey, posterCache[posterKey] || '')
    }
    tmdbMetaCache[metaKey] = {
      title: item?.title || '',
      year: item?.year || null,
    }
    setCachedMeta(metaKey, tmdbMetaCache[metaKey])
  } catch {
    posterCache[posterKey] = ''
    tmdbMetaCache[metaKey] = { title: '', year: null }
    setCachedPoster(posterKey, '')
    setCachedMeta(metaKey, tmdbMetaCache[metaKey])
  }
}

function groupRows(records) {
  const map = new Map()
  for (const row of records || []) {
    const basePath = row.target_path || row.original_path || ''
    const resourceDir = row.type === 'tv' ? extractShowDir(basePath) : extractMovieResourceDir(basePath)
    const seasonDir = row.type === 'tv' ? extractSeasonDir(basePath) : resourceDir
    const seasonLabel = row.type === 'tv' ? extractSeasonLabelFromPath(basePath) : 'Movie'
    const key = `${row.type || ''}:${row.tmdb_id || 0}:${normalizePath(resourceDir)}`
    const latest = new Date(row.updated_at || row.created_at || 0).getTime()
    if (!map.has(key)) {
      map.set(key, {
        key,
        type: row.type,
        tmdb_id: row.tmdb_id,
        resource_dir: resourceDir,
        season_dir: seasonDir,
        season_dirs: seasonDir ? [seasonDir] : [],
        season_labels: seasonLabel ? [seasonLabel] : [],
        season_label: seasonLabel,
        season_summary: seasonLabel,
        season_count: row.type === 'tv' ? 1 : 0,
        resource_name: extractResourceName(basePath, row.type),
        record_count: 1,
        latest_updated_at: row.updated_at || row.created_at,
        latest_ts: latest,
        sample: row,
        item_ids: row.id ? [row.id] : [],
      })
      continue
    }
    const item = map.get(key)
    item.record_count += 1
    if (row.id) item.item_ids.push(row.id)
    if (seasonDir && !item.season_dirs.includes(seasonDir)) item.season_dirs.push(seasonDir)
    if (seasonLabel && !item.season_labels.includes(seasonLabel)) item.season_labels.push(seasonLabel)
    item.season_count = item.type === 'tv' ? item.season_dirs.length : 0
    item.season_summary = item.type === 'tv' ? formatSeasonSummary(item.season_labels) : 'Movie'
    if (latest > item.latest_ts) {
      item.latest_ts = latest
      item.latest_updated_at = row.updated_at || row.created_at
      item.sample = row
      item.season_label = seasonLabel
      item.season_dir = seasonDir
    }
  }
  return Array.from(map.values()).sort((a, b) => b.latest_ts - a.latest_ts)
}

const seasonDrawerSeasonOptions = computed(() => {
  if (seasonDrawerType.value !== 'tv') return []
  const labels = Array.from(new Set(
    (seasonDrawerItems.value || [])
      .map((item) => extractDrawerSeasonBucket(item?.target_path || item?.original_path || ''))
      .filter(Boolean),
  ))
  return labels.sort((a, b) => {
    const aNum = extractSeasonNumber(a)
    const bNum = extractSeasonNumber(b)
    if (aNum !== null && bNum !== null) return aNum - bNum
    if (aNum !== null) return -1
    if (bNum !== null) return 1
    return a.localeCompare(b, 'zh-Hans-CN')
  })
})

const filteredSeasonDrawerItems = computed(() => {
  const search = String(seasonDrawerSearch.value || '').trim().toLowerCase()
  const category = seasonDrawerCategory.value || 'all'
  const season = seasonDrawerSeason.value || 'all'
  return (seasonDrawerItems.value || []).filter((row) => {
    const source = String(row?.original_path || '').toLowerCase()
    const target = String(row?.target_path || '').toLowerCase()
    if (search && !source.includes(search) && !target.includes(search)) return false
    if (!drawerItemMatchesCategory(row, category)) return false
    if (seasonDrawerType.value === 'tv' && season !== 'all') {
      const bucket = extractDrawerSeasonBucket(row?.target_path || row?.original_path || '')
      if (bucket !== season) return false
    }
    return true
  })
})

function refreshPagedRows() {
  total.value = allSeasonRows.value.length
  const start = (page.value - 1) * pageSize.value
  seasonRows.value = allSeasonRows.value.slice(start, start + pageSize.value)
}

function onFilterChanged() {
  page.value = 1
  loadMedia()
}

function onDrawerSelectionChange(rows) {
  seasonDrawerSelectedRows.value = rows || []
}

function openFixDialog(row) {
  if (row.is_directory_pending) {
    ElMessage.warning('目录级待办不支持直接修正，请先手动整理目录后重扫')
    return
  }
  currentFixRow.value = row
  fixForm.tmdb_id = row.tmdb_id || ''
  fixForm.title = ''
  fixForm.year = ''
  fixForm.season = undefined
  fixForm.episode = undefined
  fixForm.episode_offset = undefined
  fixDialogVisible.value = true
}

function openDirFixDialog() {
  if (!seasonDrawerDir.value) {
    ElMessage.warning('缺少资源目录信息')
    return
  }
  dirFixForm.tmdb_id = ''
  dirFixForm.title = ''
  dirFixForm.year = ''
  dirFixForm.season = undefined
  dirFixForm.episode_offset = undefined
  dirFixDialogVisible.value = true
}

async function submitFix() {
  if (!fixForm.tmdb_id || !fixForm.title || !fixForm.year) {
    ElMessage.warning('请填写完整的 TMDB 信息 (ID, 标题, 年份)')
    return
  }

  fixing.value = true
  try {
    await mediaApi.reidentify(currentFixRow.value.id, {
      tmdb_id: fixForm.tmdb_id,
      title: fixForm.title,
      year: fixForm.year,
      season: fixForm.season,
      episode: fixForm.episode,
      episode_offset: fixForm.episode_offset,
    })
    ElMessage.success('修正成功')
    fixDialogVisible.value = false
    await loadMedia()
    if (seasonDrawerVisible.value && seasonDrawerDir.value) {
      seasonDrawerItems.value = await fetchAllMediaByTargetDirItems(seasonDrawerDir.value)
    }
  } catch (e) {
    ElMessage.error(`修正失败: ${e.response?.data?.detail || e.message}`)
  } finally {
    fixing.value = false
  }
}

async function submitDirFix() {
  if (!dirFixForm.tmdb_id || !dirFixForm.title || !dirFixForm.year) {
    ElMessage.warning('请填写完整的 TMDB 信息 (ID, 标题, 年份)')
    return
  }
  if (!seasonDrawerDir.value) {
    ElMessage.warning('缺少资源目录信息')
    return
  }
  dirFixing.value = true
  try {
    const payload = {
      target_dir: seasonDrawerDir.value,
      tmdb_id: dirFixForm.tmdb_id,
      title: dirFixForm.title,
      year: dirFixForm.year,
      season: dirFixForm.season,
      episode_offset: dirFixForm.episode_offset,
    }
    const { data } = await mediaApi.reidentifyByTargetDir(payload)
    ElMessage.success(data?.message || '修正成功')
    dirFixDialogVisible.value = false
    await loadMedia()
    await refreshSeasonDrawerItems()
  } catch (e) {
    ElMessage.error(`修正失败: ${e.response?.data?.detail || e.message}`)
  } finally {
    dirFixing.value = false
  }
}

function statusType(s) {
  const map = { pending: 'info', pending_manual: 'warning', linked: 'warning', scraped: 'success', error: 'danger' }
  return map[s] || 'info'
}

function statusText(s) {
  const map = { pending: '待处理', pending_manual: '待办(需确认)', linked: '已链接', scraped: '成功', error: '失败' }
  return map[s] || s
}

function formatSize(bytes) {
  if (!bytes) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let n = Number(bytes)
  let i = 0
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024
    i += 1
  }
  return `${n.toFixed(2)} ${units[i]}`
}

function formatTime(t) {
  if (!t) return '-'
  return dayjs(t).format('YYYY-MM-DD HH:mm')
}

async function openSeasonDrawer(row) {
  const resourceDir = row?.resource_dir
  if (!resourceDir) return
  seasonDrawerVisible.value = true
  seasonDrawerDir.value = resourceDir
  seasonDrawerType.value = row?.type || ''
  seasonDrawerTitle.value = `${getResourceName(row)} · ${row.season_summary || row.season_label}`
  seasonDrawerSearch.value = ''
  seasonDrawerCategory.value = 'all'
  seasonDrawerSeason.value = 'all'
  seasonDrawerLoading.value = true
  try {
    seasonDrawerItems.value = await fetchAllMediaByTargetDirItems(resourceDir)
    seasonDrawerSelectedRows.value = []
  } catch (e) {
    seasonDrawerItems.value = []
    ElMessage.error(e?.response?.data?.detail || '加载资源目录记录失败')
  } finally {
    seasonDrawerLoading.value = false
  }
}

async function refreshSeasonDrawerItems() {
  if (!seasonDrawerDir.value) return
  seasonDrawerLoading.value = true
  try {
    seasonDrawerItems.value = await fetchAllMediaByTargetDirItems(seasonDrawerDir.value)
    seasonDrawerSelectedRows.value = []
  } catch (e) {
    seasonDrawerItems.value = []
    ElMessage.error(e?.response?.data?.detail || '刷新资源目录记录失败')
  } finally {
    seasonDrawerLoading.value = false
  }
}

async function deleteByIds(ids, deleteFiles, sceneName, deleteResourceScope = false) {
  const validIds = (ids || []).filter((x) => Number.isFinite(Number(x))).map((x) => Number(x))
  if (!validIds.length) {
    ElMessage.warning('没有可删除的记录')
    return false
  }
  const hint = deleteFiles ? '并删除硬链接文件' : '仅删除媒体记录'
  await ElMessageBox.confirm(
    `确认删除 ${validIds.length} 条记录？\n操作将${hint}。`,
    sceneName || '确认删除',
    {
      type: 'warning',
      confirmButtonText: '确认删除',
      cancelButtonText: '取消',
    },
  )
  const { data } = await mediaApi.batchDelete({
    ids: validIds,
    delete_files: !!deleteFiles,
    delete_resource_scope: !!deleteResourceScope,
  })
  const successParts = [
    `记录 ${data?.deleted_records ?? 0} 条`,
    `文件 ${data?.deleted_files ?? 0} 个`,
    `inode ${data?.deleted_inodes ?? 0} 条`,
    `目录状态 ${data?.deleted_directory_states ?? 0} 条`,
    `空目录 ${data?.pruned_dirs ?? 0} 个`,
  ]
  ElMessage.success(`删除完成：${successParts.join('，')}`)
  return true
}

async function fetchAllMediaByTargetDirItems(targetDir) {
  if (!targetDir) return []
  const limit = 2000
  let offset = 0
  let total = 0
  const items = []

  do {
    const { data } = await mediaApi.byTargetDir({
      target_dir: targetDir,
      offset,
      limit,
    })
    const pageItems = data?.items || []
    total = Number(data?.total || 0)
    items.push(...pageItems)
    offset += pageItems.length
    if (!pageItems.length) break
  } while (offset < total)

  return items
}

async function onDeleteDrawerRowCommand(row, command) {
  try {
    const ok = await deleteByIds([row?.id], command === 'records_and_links', '删除当前记录')
    if (!ok) return
    await loadMedia()
    await refreshSeasonDrawerItems()
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error(e?.response?.data?.detail || '删除失败')
    }
  }
}

async function deleteDrawerSelected(deleteFiles) {
  try {
    const ids = seasonDrawerSelectedRows.value.map((x) => x.id)
    const ok = await deleteByIds(ids, deleteFiles, '删除选中记录')
    if (!ok) return
    await loadMedia()
    await refreshSeasonDrawerItems()
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error(e?.response?.data?.detail || '删除失败')
    }
  }
}

async function deleteResourceByDir(targetDir, deleteFiles, fallbackIds = []) {
  if (!targetDir) {
    ElMessage.warning('缺少资源目录信息')
    return
  }
  const items = await fetchAllMediaByTargetDirItems(targetDir)
  const ids = items.length
    ? items.map((x) => x.id)
    : (fallbackIds || []).filter((x) => Number.isFinite(Number(x))).map((x) => Number(x))
  return deleteByIds(ids, deleteFiles, '删除整组资源', true)
}

async function onDeleteDrawerResourceCommand(command) {
  try {
    const deleteFiles = command === 'records_and_links'
    let ok = false
    if (seasonDrawerType.value === 'tv' && seasonDrawerSeason.value !== 'all') {
      const scopedIds = (seasonDrawerItems.value || [])
        .filter((item) => extractSeasonLabelFromPath(item?.target_path || item?.original_path || '') === seasonDrawerSeason.value)
        .map((item) => item.id)
      ok = await deleteByIds(scopedIds, deleteFiles, `删除 ${seasonDrawerSeason.value}`, true)
    } else {
      ok = await deleteResourceByDir(
        seasonDrawerDir.value,
        deleteFiles,
        (seasonDrawerItems.value || []).map((x) => x.id),
      )
    }
    if (!ok) return
    await loadMedia()
    if (seasonDrawerVisible.value) {
      await refreshSeasonDrawerItems()
    }
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error(e?.response?.data?.detail || '删除失败')
    }
  }
}

async function onDeleteResourceCommand(row, command) {
  try {
    const ok = await deleteResourceByDir(
      row?.resource_dir || row?.season_dir,
      command === 'records_and_links',
      row?.item_ids || [],
    )
    if (!ok) return
    await loadMedia()
    if (seasonDrawerVisible.value && seasonDrawerDir.value === (row?.resource_dir || row?.season_dir)) {
      await refreshSeasonDrawerItems()
    }
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error(e?.response?.data?.detail || '删除失败')
    }
  }
}

async function fetchAllMediaItems(params) {
  const limit = 2000
  let offset = 0
  let total = 0
  const items = []

  do {
    const { data } = await mediaApi.list({
      ...params,
      offset,
      limit,
    })
    const pageItems = data?.items || []
    total = Number(data?.total || 0)
    items.push(...pageItems)
    offset += pageItems.length
    if (!pageItems.length) break
  } while (offset < total)

  return items
}

async function loadMedia() {
  loading.value = true
  try {
    const items = await fetchAllMediaItems({
      type: filters.type || undefined,
      category: filters.category || 'all',
      search: filters.search || undefined,
    })
    rawItems.value = items.filter((x) => String(x?.target_path || '').trim())
    allSeasonRows.value = groupRows(rawItems.value)
    refreshPagedRows()
    for (const row of allSeasonRows.value) {
      getPosterUrl(row)
    }
  } catch {
    rawItems.value = []
    allSeasonRows.value = []
    seasonRows.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

async function deduplicateRecords() {
  try {
    await ElMessageBox.confirm('将按“同步组 + 源路径”只保留最新一条媒体记录。是否继续？', '确认去重', { type: 'warning' })
    const { data } = await mediaApi.deduplicate()
    ElMessage.success(data.message || '去重完成')
    loadMedia()
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('去重失败')
  }
}

onMounted(() => {
  if (route.query.search) filters.search = String(route.query.search)
  loadSyncGroupsMeta()
  loadMedia()
})
</script>

<style scoped>
.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
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

:deep(.el-table) {
  background-color: transparent;
  --el-table-tr-bg-color: transparent;
}

.title-cell {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  cursor: pointer;
}

.media-icon {
  margin-top: 2px;
  color: #94a3b8;
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

.title-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.main-title {
  font-weight: 600;
  font-size: 14px;
  color: #111827;
  line-height: 1.4;
  word-break: break-all;
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

.path-flow {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 12px;
  font-family: monospace;
}

.path-row {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #64748b;
}

.path-row.source .path {
  color: #64748b;
}

.path-row.target .path {
  color: #111827;
}

.label {
  flex-shrink: 0;
  font-size: 10px;
  opacity: 0.6;
  width: 14px;
}

.path {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  direction: rtl;
  text-align: left;
}

.arrow-row {
  padding-left: 22px;
  color: #94a3b8;
  line-height: 1;
}

.status-cell {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 6px;
}

.time {
  font-size: 12px;
  color: #94a3b8;
}

.pagination-container {
  margin-top: 24px;
  display: flex;
  justify-content: flex-end;
}

.drawer-path {
  margin-bottom: 10px;
  color: #64748b;
  word-break: break-all;
}

.drawer-header-actions {
  display: flex;
  gap: 10px 14px;
  align-items: flex-start;
  justify-content: space-between;
  flex-wrap: wrap;
  margin-bottom: 12px;
}

.drawer-filters {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
}

.drawer-danger-actions {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}
</style>
