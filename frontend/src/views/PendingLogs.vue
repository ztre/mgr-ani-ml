<template>
  <div class="pending-logs-page">
    <div class="page-header">
      <div>
        <h1 class="page-title">人工修正日志</h1>
        <p class="page-subtitle">查看 `pending / 未处理项 / 人工修正` 日志，并登记人工处理结果。</p>
      </div>
      <div class="page-actions">
        <el-input
          v-model="search"
          placeholder="按路径、原因、备注、TMDB ID 搜索"
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
        <el-button :loading="loading" @click="loadLogs">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>
    </div>

    <el-card shadow="never" class="toolbar-card">
      <div class="kind-switch">
        <el-radio-group v-model="kind" @change="handleKindChange">
          <el-radio-button label="pending">Pending</el-radio-button>
          <el-radio-button label="unprocessed">Unprocessed</el-radio-button>
          <el-radio-button label="review">Review</el-radio-button>
        </el-radio-group>
        <el-tag effect="dark" type="info">{{ currentPath || '未配置路径' }}</el-tag>
      </div>
    </el-card>

    <div class="content-grid">
      <el-card shadow="never" class="table-card">
        <el-table
          :data="items"
          stripe
          v-loading="loading"
          highlight-current-row
          style="width: 100%"
          @current-change="handleCurrentChange"
        >
          <el-table-column label="时间" width="190">
            <template #default="{ row }">
              {{ formatTime(row.timestamp) }}
            </template>
          </el-table-column>
          <el-table-column label="类型" width="130">
            <template #default="{ row }">
              <el-tag size="small" :type="fileTypeTag(row.file_type)">
                {{ row.file_type || row.entry_type || '-' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="原因 / 状态" min-width="220" show-overflow-tooltip>
            <template #default="{ row }">
              {{ row.reason || row.resolution_status || '-' }}
            </template>
          </el-table-column>
          <el-table-column label="源路径" min-width="320" show-overflow-tooltip>
            <template #default="{ row }">
              {{ row.original_path || row.source_original_path || row.source_dir || '-' }}
            </template>
          </el-table-column>
          <el-table-column label="TMDB" width="100" align="center">
            <template #default="{ row }">
              {{ row.tmdb_id || row.tmdbid || '-' }}
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
            @size-change="loadLogs"
            @current-change="loadLogs"
          />
        </div>
      </el-card>

      <div class="side-panel">
        <el-card shadow="never" class="detail-card">
          <template #header>
            <div class="panel-title">日志详情</div>
          </template>
          <div v-if="currentItem" class="detail-grid">
            <div><strong>路径：</strong>{{ currentItem.original_path || currentItem.source_original_path || '-' }}</div>
            <div><strong>原因：</strong>{{ currentItem.reason || '-' }}</div>
            <div><strong>TMDB：</strong>{{ currentItem.tmdb_id || currentItem.tmdbid || '-' }}</div>
            <div><strong>季 / 集：</strong>{{ formatSeasonEpisode(currentItem) }}</div>
            <div><strong>Extra：</strong>{{ currentItem.extra_category || '-' }}</div>
            <div><strong>建议目标：</strong>{{ currentItem.suggested_target || '-' }}</div>
            <div><strong>备注：</strong>{{ currentItem.note || '-' }}</div>
          </div>
          <el-empty v-else description="选择一条日志查看详情" />
        </el-card>

        <el-card shadow="never" class="review-card">
          <template #header>
            <div class="panel-title">人工处理登记</div>
          </template>
          <el-form :model="reviewForm" label-width="96px">
            <el-form-item label="处理状态">
              <el-select v-model="reviewForm.resolution_status">
                <el-option label="已解决" value="resolved" />
                <el-option label="确认跳过" value="skipped" />
                <el-option label="误报" value="false_positive" />
                <el-option label="继续跟进" value="needs_followup" />
              </el-select>
            </el-form-item>
            <el-form-item label="处理人">
              <el-input v-model="reviewForm.reviewer" placeholder="可选" />
            </el-form-item>
            <el-form-item label="目标路径">
              <el-input v-model="reviewForm.suggested_target" placeholder="可选" />
            </el-form-item>
            <el-form-item label="备注">
              <el-input
                v-model="reviewForm.note"
                type="textarea"
                :rows="4"
                placeholder="记录人工判断、修正方式或后续动作"
              />
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="submitting" :disabled="!currentItem" @click="submitReview">
                写入 Review 日志
              </el-button>
            </el-form-item>
          </el-form>
        </el-card>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import dayjs from 'dayjs'
import { mediaApi } from '../api/client'

const kind = ref('pending')
const search = ref('')
const loading = ref(false)
const submitting = ref(false)
const items = ref([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const currentItem = ref(null)
const currentPath = ref('')

const reviewForm = reactive({
  resolution_status: 'resolved',
  reviewer: '',
  note: '',
  suggested_target: '',
})

const currentKindLabel = computed(() => {
  if (kind.value === 'pending') return 'pending'
  if (kind.value === 'review') return 'review'
  return 'unprocessed'
})

function formatTime(value) {
  if (!value) return '-'
  return dayjs(value).format('YYYY-MM-DD HH:mm:ss')
}

function formatSeasonEpisode(row) {
  const season = row?.season ?? '-'
  const episode = row?.episode ?? '-'
  return `S${season} / E${episode}`
}

function fileTypeTag(type) {
  if (type === 'video') return 'primary'
  if (type === 'attachment') return 'warning'
  if (type === 'special') return 'success'
  if (type === 'extra') return 'info'
  return 'info'
}

function resetReviewForm() {
  reviewForm.resolution_status = 'resolved'
  reviewForm.reviewer = ''
  reviewForm.note = ''
  reviewForm.suggested_target = currentItem.value?.suggested_target || ''
}

function handleCurrentChange(row) {
  currentItem.value = row || null
  resetReviewForm()
}

function handleSearch() {
  page.value = 1
  loadLogs()
}

function handleKindChange() {
  page.value = 1
  currentItem.value = null
  loadLogs()
}

async function loadLogs() {
  loading.value = true
  try {
    const { data } = await mediaApi.pendingLogs({
      kind: kind.value,
      offset: (page.value - 1) * pageSize.value,
      limit: pageSize.value,
      search: (search.value || '').trim() || undefined,
    })
    items.value = data.items || []
    total.value = data.total || 0
    currentPath.value = data.path || ''
    currentItem.value = items.value[0] || null
    resetReviewForm()
  } catch (e) {
    items.value = []
    total.value = 0
    currentItem.value = null
    currentPath.value = ''
    ElMessage.error(e.response?.data?.detail || `加载 ${currentKindLabel.value} 日志失败`)
  } finally {
    loading.value = false
  }
}

async function submitReview() {
  if (!currentItem.value) {
    ElMessage.warning('请先选择一条日志')
    return
  }
  if (!reviewForm.note.trim()) {
    ElMessage.warning('请填写处理备注')
    return
  }
  submitting.value = true
  try {
    const payload = {
      source_kind: kind.value,
      source_original_path: currentItem.value.original_path || currentItem.value.source_original_path || null,
      source_reason: currentItem.value.reason || null,
      source_timestamp: currentItem.value.timestamp || null,
      resolution_status: reviewForm.resolution_status,
      reviewer: reviewForm.reviewer || null,
      note: reviewForm.note.trim(),
      tmdb_id: currentItem.value.tmdb_id || currentItem.value.tmdbid || null,
      season: currentItem.value.season ?? null,
      episode: currentItem.value.episode ?? null,
      extra_category: currentItem.value.extra_category || null,
      suggested_target: reviewForm.suggested_target || currentItem.value.suggested_target || null,
    }
    const { data } = await mediaApi.createPendingReview(payload)
    ElMessage.success(data.message || '登记成功')
    if (kind.value === 'review') {
      await loadLogs()
    } else {
      resetReviewForm()
    }
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '登记失败')
  } finally {
    submitting.value = false
  }
}

onMounted(loadLogs)
</script>

<style scoped>
.pending-logs-page {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
  flex-wrap: wrap;
}

.page-title {
  font-size: 28px;
  font-weight: 700;
  margin-bottom: 6px;
}

.page-subtitle {
  color: #64748b;
}

.page-actions {
  display: flex;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
}

.toolbar-card,
.table-card,
.detail-card,
.review-card {
  border: none;
}

.kind-switch {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.content-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.6fr) minmax(320px, 0.9fr);
  gap: 16px;
}

.side-panel {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.panel-title {
  font-weight: 700;
}

.detail-grid {
  display: flex;
  flex-direction: column;
  gap: 10px;
  font-size: 13px;
  word-break: break-all;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
}

@media (max-width: 1080px) {
  .content-grid {
    grid-template-columns: 1fr;
  }
}
</style>
