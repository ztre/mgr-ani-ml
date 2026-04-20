<template>
  <div class="inodes-page">
    <div class="header">
      <div class="header-main">
        <h1 class="page-title">Inode 管理</h1>
      </div>
      <div class="header-actions">
        <el-input
          class="toolbar-search"
          v-model="filters.search"
          placeholder="搜索源路径或目标路径..."
          clearable
          @clear="onFilterChanged"
          @keyup.enter="onFilterChanged"
        />
        <el-select v-model="filters.syncGroupId" clearable placeholder="同步组" style="width: 160px" @change="onFilterChanged">
          <el-option v-for="group in syncGroups" :key="group.id" :label="group.name" :value="group.id" />
        </el-select>
        <el-button type="primary" @click="onFilterChanged">
          <el-icon><Search /></el-icon>
          搜索
        </el-button>
        <el-button plain @click="resetMainFilters">重置</el-button>
        <el-button type="warning" :loading="cleaning" @click="cleanupInodes">清理无效记录</el-button>
        <el-button :loading="loading" @click="loadResources">
          <el-icon><Refresh /></el-icon>
        </el-button>
      </div>
    </div>

    <el-card shadow="never" class="table-card">
      <el-table :data="resources" v-loading="loading" stripe style="width: 100%">
        <el-table-column label="资源" min-width="420">
          <template #default="{ row }">
            <div class="resource-cell" @click="openDrawer(row)">
              <div class="resource-icon" :class="{ 'has-poster': !!resourcePosterUrl(row) }" :ref="(el) => setResourceIconRef(row, el)">
                <img
                  v-if="resourcePosterUrl(row)"
                  class="resource-poster"
                  :src="resourcePosterUrl(row)"
                  :alt="resourcePosterAlt(row)"
                  loading="lazy"
                />
                <el-icon v-else :size="20">
                  <Monitor v-if="row.type === 'tv'" />
                  <Film v-else />
                </el-icon>
              </div>
              <div class="resource-body">
                <div class="resource-title">{{ row.resource_name }}</div>
                <div class="resource-meta">
                  <el-tag size="small" effect="plain">{{ row.type === 'tv' ? 'TV' : 'Movie' }}</el-tag>
                  <span>{{ row.season_summary || '-' }}</span>
                  <span>记录 {{ row.record_count }}</span>
                  <span>同步组 {{ row.sync_group_id || '-' }}</span>
                  <span v-if="row.tmdb_id">TMDB {{ row.tmdb_id }}</span>
                </div>
                <div class="resource-dir">{{ row.resource_dir }}</div>
              </div>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="最近更新时间" width="180" align="right">
          <template #default="{ row }">{{ formatTime(row.latest_updated_at) }}</template>
        </el-table-column>

        <el-table-column label="操作" width="120" align="center">
          <template #default="{ row }">
            <div class="table-actions">
              <el-button type="danger" plain size="small" @click.stop="deleteResourceRow(row)">删除资源</el-button>
            </div>
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
          @size-change="loadResources"
          @current-change="loadResources"
        />
      </div>
    </el-card>

    <el-drawer v-model="drawerVisible" direction="rtl" size="64%" :title="drawerTitle" destroy-on-close>
      <div class="drawer-header-sticky">
        <div class="drawer-path">{{ drawerResource?.resource_dir || '-' }}</div>
        <div class="drawer-toolbar">
          <div class="drawer-toolbar-left">
            <el-input v-model="drawerSearch" placeholder="搜索当前资源文件..." clearable style="width: 240px" />
            <el-select v-model="drawerCategory" placeholder="分类" style="width: 120px">
              <el-option label="全部" value="all" />
              <el-option label="正片" value="main" />
              <el-option label="SPs/Extras" value="sps" />
            </el-select>
            <el-button plain @click="resetDrawerFilters">重置</el-button>
            <el-button :loading="drawerLoading" @click="reloadDrawer">
              <el-icon><Refresh /></el-icon>
            </el-button>
          </div>
          <div class="drawer-toolbar-right">
            <el-button type="danger" plain size="small" @click="deleteResourceScope(drawerResource?.scope)">删除当前资源</el-button>
          </div>
        </div>
        <div v-if="displayNodes.length > 0" class="node-nav sticky-node-nav">
          <el-radio-group v-model="selectedNodeKey" size="small">
            <el-radio-button v-for="node in displayNodes" :key="node.key" :label="node.key">
              {{ node.label }} ({{ node.visibleCount }})
            </el-radio-button>
          </el-radio-group>
        </div>
      </div>

      <el-empty v-if="!drawerLoading && displayNodes.length === 0" description="当前资源没有 inode 记录" />

      <template v-else>
        <el-card v-if="currentNode" shadow="never" class="node-card">
          <div class="node-card-header">
            <div>
              <div class="node-title">{{ currentNode.label }}</div>
              <div class="node-summary">
                实际 {{ currentNode.record_count }} 条 / 当前可见 {{ currentNode.visibleCount }} 条 / 正片 {{ currentNode.main_count || 0 }} / SP {{ currentNode.aux_count || 0 }}
              </div>
            </div>
            <div class="node-header-actions">
              <el-button type="danger" plain size="small" @click="deleteNode(currentNode)">删除该级</el-button>
            </div>
          </div>

          <div class="group-grid">
            <el-card v-for="group in currentNode.groups" :key="group.key" shadow="hover" class="group-card">
              <template #header>
                <div class="group-header">
                  <div>
                    <div class="group-title">{{ group.label }}</div>
                    <div class="group-summary">实际 {{ group.item_count }} 条 / 当前可见 {{ group.visibleCount }} 条</div>
                  </div>
                  <div class="group-actions">
                    <el-button plain size="small" @click="toggleGroupCollapse(group)">
                      {{ isGroupCollapsed(group) ? '展开分组' : '收起分组' }}
                    </el-button>
                    <el-button type="danger" plain size="small" @click="deleteGroup(group)">删除分组</el-button>
                  </div>
                </div>
              </template>

              <div v-if="isGroupCollapsed(group)" class="collapsed-group-hint">
                该分组默认折叠，点击“展开分组”查看明细。
              </div>

              <el-table v-else :data="group.items" stripe style="width: 100%">
                <el-table-column label="源文件" min-width="240">
                  <template #default="{ row }">
                    <div class="path-block" :title="row.source_path">{{ extractFilename(row.source_path) }}</div>
                  </template>
                </el-table-column>

                <el-table-column label="目标文件" min-width="260">
                  <template #default="{ row }">
                    <div class="path-block" :title="row.target_path">{{ extractFilename(row.target_path) }}</div>
                  </template>
                </el-table-column>

                <el-table-column label="归属" width="120" align="center">
                  <template #default="{ row }">
                    <el-tag :type="bucketTagType(row.tree_bucket)" effect="light">{{ ownershipLabel(row) }}</el-tag>
                  </template>
                </el-table-column>

                <el-table-column label="更新时间" width="170" align="right">
                  <template #default="{ row }">{{ formatTime(row.updated_at || row.created_at) }}</template>
                </el-table-column>

                <el-table-column label="操作" width="90" align="center">
                  <template #default="{ row }">
                    <div class="row-actions">
                      <el-button class="row-action-button" type="danger" plain size="small" @click="deleteItem(row)">删除</el-button>
                    </div>
                  </template>
                </el-table-column>
              </el-table>
            </el-card>
          </div>
        </el-card>
      </template>
    </el-drawer>

    <teleport to="body">
      <transition
        :css="false"
        @enter="onFloatingPosterEnter"
        @leave="onFloatingPosterLeave"
      >
        <div v-if="floatingPosterUrl" class="drawer-floating-poster-shell" aria-hidden="true">
          <div class="drawer-floating-poster-card">
            <img
              class="drawer-floating-poster-image"
              :src="floatingPosterUrl"
              :alt="floatingPosterAlt"
              loading="lazy"
            />
          </div>
        </div>
      </transition>
    </teleport>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Film, Monitor, Refresh, Search } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { inodesApi, mediaApi, syncGroupsApi } from '../api/client'
import { buildConfirmDialogOptions, buildConfirmMessage } from '../utils/confirmMessage'
import { animateFloatingPosterEnter, animateFloatingPosterLeave, resourceIdentityKey, setResourceIconElement } from '../utils/floatingPosterMotion'
import { useResourcePoster } from '../utils/resourcePosterStore'

const VIDEO_FILE_EXTENSIONS = new Set([
  '.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts', '.m2ts',
])

const loading = ref(false)
const cleaning = ref(false)
const syncGroups = ref([])
const resources = ref([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const filters = reactive({ search: '', syncGroupId: undefined })

const drawerVisible = ref(false)
const drawerLoading = ref(false)
const drawerTitle = ref('')
const drawerResource = ref(null)
const drawerTree = ref([])
const selectedNodeKey = ref('')
const drawerSearch = ref('')
const drawerCategory = ref('all')
const collapsedGroups = ref({})
const resourceIconElements = new Map()

const { ensurePosterForResource, hydrateCachedPosterMeta, resourcePosterUrl, resourcePosterAlt } = useResourcePoster(
  (params) => mediaApi.poster(params).then(({ data }) => data),
)

function extractFilename(path) {
  return String(path || '').split(/[/\\]/).pop() || ''
}

function setResourceIconRef(resource, el) {
  setResourceIconElement(resourceIconElements, resource, el)
}

function onFloatingPosterEnter(el, done) {
  const targetEl = resourceIconElements.get(resourceIdentityKey(drawerResource.value))
  animateFloatingPosterEnter(el, targetEl, done)
}

function onFloatingPosterLeave(el, done) {
  const targetEl = resourceIconElements.get(resourceIdentityKey(drawerResource.value))
  animateFloatingPosterLeave(el, targetEl, done)
}

function formatTime(value) {
  if (!value) return '-'
  return dayjs(value).format('YYYY-MM-DD HH:mm')
}

function bucketTagType(bucket) {
  if (bucket === 'main') return 'primary'
  if (bucket === 'aux') return 'warning'
  return 'info'
}

function extractItemExtension(item) {
  const path = String(item?.target_path || item?.source_path || '').split('?')[0]
  const filename = extractFilename(path)
  const dotIndex = filename.lastIndexOf('.')
  if (dotIndex < 0) return ''
  return filename.slice(dotIndex).toLowerCase()
}

function isVideoItem(item) {
  return VIDEO_FILE_EXTENSIONS.has(extractItemExtension(item))
}

function ownershipLabel(item) {
  if (!isVideoItem(item)) {
    if (item?.tree_bucket === 'main') return '正片附件'
    if (item?.tree_bucket === 'aux') return 'SP附件'
    return '附件'
  }
  if (item?.tree_bucket === 'main') return '正片'
  if (item?.tree_bucket === 'aux') return 'SP/Extras'
  return 'Misc'
}

function itemMatchesSearch(item, keyword) {
  if (!keyword) return true
  const text = [item.source_path, item.target_path, extractFilename(item.source_path), extractFilename(item.target_path)]
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
  return text.includes(keyword)
}

const displayNodes = computed(() => {
  const keyword = String(drawerSearch.value || '').trim().toLowerCase()
  const category = drawerCategory.value || 'all'
  return (drawerTree.value || [])
    .map((node) => {
      const groups = (node.groups || [])
        .map((group) => {
          const items = (group.items || []).filter((item) => {
            if (category === 'main' && item.tree_bucket !== 'main') return false
            if (category === 'sps' && item.tree_bucket === 'main') return false
            return itemMatchesSearch(item, keyword)
          })
          return { ...group, items, visibleCount: items.length }
        })
        .filter((group) => group.items.length > 0)
      const visibleCount = groups.reduce((sum, group) => sum + group.items.length, 0)
      return { ...node, groups, visibleCount }
    })
    .filter((node) => node.groups.length > 0)
})

const currentNode = computed(() => displayNodes.value.find((node) => node.key === selectedNodeKey.value) || displayNodes.value[0] || null)
const floatingPosterUrl = computed(() => {
  if (!drawerVisible.value || !drawerResource.value) return ''
  return resourcePosterUrl(drawerResource.value, 'w500')
})
const floatingPosterAlt = computed(() => resourcePosterAlt(drawerResource.value))

watch(displayNodes, (nodes) => {
  if (!nodes.length) {
    selectedNodeKey.value = ''
    return
  }
  if (!nodes.some((node) => node.key === selectedNodeKey.value)) {
    selectedNodeKey.value = nodes[0].key
  }
})

async function loadSyncGroups() {
  try {
    const { data } = await syncGroupsApi.list()
    syncGroups.value = data || []
  } catch {
    syncGroups.value = []
  }
}

async function loadResources() {
  loading.value = true
  try {
    const { data } = await inodesApi.resources({
      offset: (page.value - 1) * pageSize.value,
      limit: pageSize.value,
      search: filters.search || undefined,
      sync_group_id: filters.syncGroupId || undefined,
    })
    resources.value = data.items || []
    hydrateCachedPosterMeta(resources.value)
    total.value = Number(data.total || 0)
  } catch (error) {
    ElMessage.error(error?.response?.data?.detail || error?.message || '加载 inode 资源失败')
  } finally {
    loading.value = false
  }
}

function onFilterChanged() {
  page.value = 1
  loadResources()
}

function resetMainFilters() {
  filters.search = ''
  filters.syncGroupId = undefined
  onFilterChanged()
}

async function loadDrawerTree(resource) {
  if (!resource?.resource_dir) return
  drawerLoading.value = true
  try {
    const { data } = await inodesApi.resourceTree({
      resource_dir: resource.resource_dir,
      sync_group_id: resource.sync_group_id || undefined,
    })
    drawerResource.value = data.resource || resource
    ensurePosterForResource(drawerResource.value)
    drawerTree.value = data.nodes || []
    collapsedGroups.value = Object.fromEntries(
      (data.nodes || [])
        .flatMap((node) => node.groups || [])
        .map((group) => [group.key, ['main', 'aux', 'extras'].includes(group.kind)])
    )
  } catch (error) {
    ElMessage.error(error?.response?.data?.detail || error?.message || '加载 inode 资源树失败')
  } finally {
    drawerLoading.value = false
  }
}

async function openDrawer(resource) {
  drawerTitle.value = resource?.resource_name || '资源详情'
  drawerVisible.value = true
  drawerSearch.value = ''
  drawerCategory.value = 'all'
  drawerResource.value = resource
  ensurePosterForResource(resource)
  await loadDrawerTree(resource)
}

async function reloadDrawer() {
  if (!drawerResource.value) return
  await loadDrawerTree(drawerResource.value)
}

function resetDrawerFilters() {
  drawerSearch.value = ''
  drawerCategory.value = 'all'
}

function isGroupCollapsed(group) {
  return Boolean(collapsedGroups.value?.[group.key])
}

function toggleGroupCollapse(group) {
  collapsedGroups.value = {
    ...collapsedGroups.value,
    [group.key]: !isGroupCollapsed(group),
  }
}

async function confirmDelete(label, scope, visibleCount) {
  const actualCount = Number(scope?.item_ids?.length || 0)
  const visibleText = visibleCount !== actualCount ? `当前筛选可见 ${visibleCount} 条，实际会处理 ${actualCount} 条。` : `将处理 ${actualCount} 条记录。`
  await ElMessageBox.confirm(
    buildConfirmMessage([
      label,
      visibleText,
    ]),
    '确认删除',
    buildConfirmDialogOptions(),
  )
}

async function deleteScope(scope, visibleCount) {
  if (!scope) {
    ElMessage.warning('当前作用域不可用')
    return
  }
  await inodesApi.deleteScope(scope)
  ElMessage.success('删除完成')
  await Promise.all([loadResources(), reloadDrawer()])
}

async function deleteResourceRow(row) {
  if (!row?.sample_id) {
    ElMessage.warning('当前资源缺少可删除记录')
    return
  }
  try {
    await ElMessageBox.confirm(
      buildConfirmMessage(['将删除该资源关联的 inode 记录，是否继续？']),
      '确认删除',
      buildConfirmDialogOptions(),
    )
    await inodesApi.batchDelete({ ids: [row.sample_id] })
    ElMessage.success('删除完成')
    await loadResources()
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除资源失败')
  }
}

async function deleteResourceScope(scope) {
  try {
    await confirmDelete(`删除资源 ${drawerResource.value?.resource_name || ''}`, scope, displayNodes.value.reduce((sum, node) => sum + node.visibleCount, 0))
    await deleteScope(scope, displayNodes.value.reduce((sum, node) => sum + node.visibleCount, 0))
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除资源失败')
  }
}

async function deleteNode(node) {
  try {
    await confirmDelete(`删除 ${node.label}`, node.scope, node.visibleCount)
    await deleteScope(node.scope, node.visibleCount)
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除节点失败')
  }
}

async function deleteGroup(group) {
  try {
    await confirmDelete(`删除分组 ${group.label}`, group.scope, group.visibleCount)
    await deleteScope(group.scope, group.visibleCount)
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除分组失败')
  }
}

async function deleteItem(row) {
  try {
    await confirmDelete(`删除文件 ${extractFilename(row.source_path)}`, row.scope, 1)
    await deleteScope(row.scope, 1)
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除失败')
  }
}

async function cleanupInodes() {
  try {
    await ElMessageBox.confirm('将清理已经失效的 inode 记录，是否继续？', '清理无效记录', { type: 'warning' })
    cleaning.value = true
    const { data } = await inodesApi.cleanup()
    ElMessage.success(data?.message || '清理完成')
    await Promise.all([loadResources(), reloadDrawer()])
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '清理失败')
  } finally {
    cleaning.value = false
  }
}

onMounted(async () => {
  await Promise.all([loadSyncGroups(), loadResources()])
})
</script>

<style scoped>
.inodes-page {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.header,
.drawer-toolbar,
.drawer-toolbar-left,
.drawer-toolbar-right,
.node-card-header,
.node-header-actions,
.group-header,
.group-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.header {
  align-items: flex-start;
  flex-direction: column;
}

.header-main {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.header-actions {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  gap: 12px;
  flex-wrap: wrap;
  width: 100%;
}

.page-title {
  margin: 0;
  font-size: 28px;
  font-weight: 600;
}

.resource-cell {
  display: flex;
  align-items: flex-start;
  gap: 14px;
  cursor: pointer;
}

.resource-icon {
  width: 56px;
  height: 78px;
  border-radius: 16px;
  background: linear-gradient(135deg, #eff8f5, #f6f1e7);
  display: flex;
  align-items: center;
  justify-content: center;
  color: #2f6b5f;
  flex: 0 0 auto;
  overflow: hidden;
  box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
}

.resource-icon.has-poster {
  background: #dbe4f0;
}

.resource-poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.resource-body {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 0;
}

.resource-title,
.node-title,
.group-title {
  font-size: 16px;
  font-weight: 600;
  color: #1f2937;
}

.resource-meta,
.resource-dir,
.drawer-path,
.node-summary,
.group-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  color: #64748b;
  font-size: 13px;
}

.resource-dir,
.drawer-path,
.path-block {
  word-break: break-all;
}

.drawer-header-sticky {
  position: sticky;
  top: -20px;
  z-index: 10;
  padding: 20px 20px 12px;
  margin: -20px -20px 12px;
  background: linear-gradient(180deg, #ffffff 0%, #ffffff 80%, rgba(255, 255, 255, 0.92) 100%);
  border-bottom: 1px solid #e5e7eb;
  box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
}

.drawer-path {
  padding: 4px 0 10px;
}

.drawer-toolbar {
  padding: 0;
  margin: 0;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
}

.node-nav {
  margin: 12px 0 16px;
  overflow-x: auto;
}

.sticky-node-nav {
  margin: 12px 0 0;
  padding-top: 12px;
  border-top: 1px solid rgba(148, 163, 184, 0.18);
}

:deep(.sticky-node-nav .el-radio-group) {
  display: inline-flex;
  gap: 8px;
  flex-wrap: nowrap;
  min-width: max-content;
}

:deep(.sticky-node-nav .el-radio-button__inner) {
  border-radius: 999px !important;
  border: 1px solid #dbe3ef !important;
  box-shadow: none !important;
  background: #f8fafc;
  color: #334155;
}

:deep(.sticky-node-nav .el-radio-button:first-child .el-radio-button__inner),
:deep(.sticky-node-nav .el-radio-button:last-child .el-radio-button__inner) {
  border-radius: 999px !important;
}

:deep(.sticky-node-nav .el-radio-button__original-radio:checked + .el-radio-button__inner) {
  background: #1d4ed8;
  border-color: #1d4ed8 !important;
  color: #ffffff;
}

.node-card,
.group-card {
  border-radius: 16px;
}

.group-grid {
  display: grid;
  gap: 16px;
  margin-top: 8px;
}

.collapsed-group-hint {
  padding: 8px 4px 0;
  color: #64748b;
  font-size: 13px;
}

.row-actions {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.row-action-button {
  min-width: 60px;
}

.drawer-floating-poster-shell {
  position: fixed;
  top: clamp(92px, 11vh, 148px);
  right: calc(64% + 24px);
  transform: none;
  z-index: 2100;
  width: min(340px, calc(36vw - 32px));
  aspect-ratio: 2 / 3;
  pointer-events: none;
  transform-origin: center center;
  --poster-collapse-x: 24px;
  --poster-collapse-y: 14px;
  --poster-collapse-scale-x: 0.95;
  --poster-collapse-scale-y: 0.95;
}

.drawer-floating-poster-card {
  width: 100%;
  height: 100%;
  border-radius: 30px;
  overflow: hidden;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.95), rgba(241, 245, 249, 0.82));
  border: 1px solid rgba(255, 255, 255, 0.7);
  box-shadow: 0 32px 96px rgba(15, 23, 42, 0.28);
  backdrop-filter: blur(10px);
}

.drawer-floating-poster-image {
  display: block;
  width: 100%;
  height: 100%;
  object-fit: cover;
}

@media (max-width: 1180px) {
  .drawer-floating-poster-shell {
    display: none;
  }
}

@media (max-width: 960px) {
  .header,
  .header-actions,
  .drawer-toolbar,
  .node-card-header,
  .group-header {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
