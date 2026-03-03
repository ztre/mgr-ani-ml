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
        <el-button @click="loadPending" :loading="loading">
          <el-icon><Refresh /></el-icon>
          刷新
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
        <el-table-column label="操作" width="180" fixed="right" align="center">
          <template #default="{ row }">
            <el-button type="primary" link @click="openOrganizeDialog(row)">手动整理</el-button>
            <el-button
              type="danger"
              link
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

    <el-dialog v-model="dialogVisible" title="手动识别整理" width="520px">
      <el-form :model="form" label-width="110px">
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
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitOrganize">开始整理</el-button>
      </template>
    </el-dialog>

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
import { reactive, ref, onMounted, watch } from 'vue'
import { mediaApi } from '../api/client'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
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
const form = reactive({
  media_type: 'tv',
  tmdb_id: '',
  title: '',
  year: undefined,
  season: undefined,
})

function extractDirName(path) {
  if (!path) return '-'
  const parts = path.split(/[/\\]/).filter(Boolean)
  return parts[parts.length - 1] || '-'
}

function formatTime(t) {
  if (!t) return '-'
  return dayjs(t).format('YYYY-MM-DD HH:mm')
}

function openOrganizeDialog(row) {
  currentRow.value = row
  form.media_type = row?.type || 'tv'
  form.tmdb_id = ''
  form.title = ''
  form.year = undefined
  form.season = undefined
  dialogVisible.value = true
}

async function submitOrganize() {
  if (!currentRow.value) return
  if (!form.tmdb_id) {
    ElMessage.warning('请填写 TMDB ID，或先用搜索选择条目')
    return
  }
  submitting.value = true
  try {
    const payload = {
      tmdb_id: Number(form.tmdb_id),
      title: (form.title || '').trim() || null,
      year: form.year ?? null,
      media_type: form.media_type,
      season: form.media_type === 'tv' ? (form.season ?? null) : null,
    }
    const { data } = await mediaApi.manualOrganize(currentRow.value.id, payload)
    ElMessage.success(data.message || '整理完成')
    dialogVisible.value = false
    await loadPending()
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '整理失败')
  } finally {
    submitting.value = false
  }
}

async function deletePending(row) {
  if (!row?.id) return
  try {
    await ElMessageBox.confirm(
      `确认删除该待办记录？\n${extractDirName(row.original_path) || '-'}`,
      '删除待办',
      {
        type: 'warning',
        confirmButtonText: '删除',
        cancelButtonText: '取消',
      },
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
</style>
