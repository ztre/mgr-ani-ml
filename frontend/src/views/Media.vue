<template>
  <div class="media-page">
    <div class="header">
      <h1 class="page-title">媒体记录</h1>
      <div class="header-actions">
        <el-input
          v-model="filters.search"
          placeholder="搜索资源或文件名..."
          clearable
          style="width: 240px"
          @clear="onFilterChanged"
          @keyup.enter="onFilterChanged"
        />
        <el-select v-model="filters.type" placeholder="类型" clearable style="width: 110px" @change="onFilterChanged">
          <el-option label="TV" value="tv" />
          <el-option label="电影" value="movie" />
        </el-select>
        <el-select v-model="filters.category" placeholder="分类" style="width: 120px" @change="onFilterChanged">
          <el-option label="全部" value="all" />
          <el-option label="正片" value="main" />
          <el-option label="SPs/Extras" value="sps" />
        </el-select>
        <el-button plain @click="resetMainFilters">重置</el-button>
        <el-button :loading="loading" @click="loadResources">
          <el-icon><Refresh /></el-icon>
        </el-button>
        <el-button type="warning" plain @click="deduplicateRecords">记录去重</el-button>
      </div>
    </div>

    <el-card shadow="never" class="table-card">
      <el-table :data="resources" v-loading="loading" stripe style="width: 100%">
        <el-table-column label="资源" min-width="420">
          <template #default="{ row }">
            <div class="resource-cell" @click="openResourceDrawer(row)">
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
                  <span>正片 {{ row.main_count || 0 }}</span>
                  <span>SP/Extras {{ row.aux_count || 0 }}</span>
                  <span v-if="row.misc_count">Misc {{ row.misc_count }}</span>
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

        <el-table-column label="操作" width="130" align="center" fixed="right">
          <template #default="{ row }">
            <el-dropdown trigger="click" @command="(cmd) => onDeleteResourceCommand(row, cmd)">
              <el-button type="danger" plain size="small">删除资源</el-button>
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
          @size-change="loadResources"
          @current-change="loadResources"
        />
      </div>
    </el-card>

    <el-drawer
      v-model="drawerVisible"
      direction="rtl"
      size="64%"
      :title="drawerTitle"
      destroy-on-close
      :before-close="handleOverlayBeforeClose"
    >
      <div class="drawer-header-sticky">
        <div class="drawer-path">{{ drawerResource?.resource_dir || '-' }}</div>
        <div class="drawer-toolbar">
          <div class="drawer-toolbar-left">
            <el-input v-model="drawerSearch" placeholder="搜索当前资源内文件..." clearable style="width: 240px" />
            <el-select v-model="drawerCategory" placeholder="分类" style="width: 120px">
              <el-option label="全部" value="all" />
              <el-option label="正片" value="main" />
              <el-option label="SPs/Extras" value="sps" />
            </el-select>
            <el-button plain @click="resetDrawerFilters">重置</el-button>
            <el-button :loading="drawerLoading" @click="reloadDrawer">
              <el-icon><Refresh /></el-icon>
            </el-button>
            <el-button v-if="drawerResource?.type === 'movie'" type="primary" plain :disabled="!drawerResource?.resource_dir" @click="openDirFixDialog">
              修正整个资源
            </el-button>
          </div>
          <div class="drawer-toolbar-right">
            <span class="selection-summary">已选 {{ selectedDrawerItems.length }} 条</span>
            <el-button plain size="small" :disabled="!selectedDrawerItems.length" @click="clearDrawerSelection">清空选择</el-button>
            <el-dropdown trigger="click" :disabled="!selectedDrawerItems.length" @command="deleteSelectedItems">
              <el-button type="warning" plain size="small">删除选中</el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="records">仅删除选中记录</el-dropdown-item>
                  <el-dropdown-item command="records_and_links">删除选中 + 硬链接</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
            <el-dropdown trigger="click" @command="onDeleteDrawerResourceCommand">
              <el-button type="danger" plain size="small">删除当前资源</el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="records">仅删除该资源记录</el-dropdown-item>
                  <el-dropdown-item command="records_and_links">删除该资源 + 硬链接</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
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

      <el-empty v-if="!drawerLoading && displayNodes.length === 0" description="当前资源没有匹配内容" />

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
              <el-button
                v-if="currentNode.scope?.scope_level === 'season'"
                type="primary"
                plain
                size="small"
                @click="openSeasonFixDialog(currentNode)"
              >
                修正该 Season
              </el-button>
              <el-dropdown trigger="click" @command="(cmd) => onDeleteNodeCommand(currentNode, cmd)">
                <el-button type="danger" plain size="small">删除 {{ currentNode.season != null ? 'Season' : '节点' }}</el-button>
                <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item command="records">仅删除该级记录</el-dropdown-item>
                    <el-dropdown-item command="records_and_links">删除该级 + 硬链接</el-dropdown-item>
                  </el-dropdown-menu>
                </template>
              </el-dropdown>
            </div>
          </div>

          <div class="group-grid">
            <el-card v-for="group in currentNode.groups" :key="group.key" shadow="hover" class="group-card">
              <template #header>
                <div class="group-header">
                  <div>
                    <div class="group-title">{{ group.label }}</div>
                    <div class="group-summary">
                      实际 {{ group.item_count }} 条 / 当前可见 {{ group.visibleCount }} 条
                    </div>
                  </div>
                  <div class="group-actions">
                    <el-select
                      v-if="supportsGroupFilter(group)"
                      :model-value="drawerGroupFilters[group.key] || 'all'"
                      size="small"
                      class="group-filter-select"
                      @change="(value) => updateGroupFilter(group, value)"
                    >
                      <el-option label="全部" value="all" />
                      <el-option label="正片(视频)" value="video" />
                      <el-option label="附件" value="attachment" />
                      <el-option label="已选中项" value="selected" />
                    </el-select>
                    <el-button plain size="small" @click="toggleGroupCollapse(group)">
                      {{ isGroupCollapsed(group) ? '展开分组' : '收起分组' }}
                    </el-button>
                    <el-dropdown trigger="click" @command="(cmd) => onDeleteGroupCommand(group, cmd)">
                      <el-button type="danger" plain size="small">删除分组</el-button>
                      <template #dropdown>
                        <el-dropdown-menu>
                          <el-dropdown-item command="records">仅删除该分组记录</el-dropdown-item>
                          <el-dropdown-item command="records_and_links">删除该分组 + 硬链接</el-dropdown-item>
                        </el-dropdown-menu>
                      </template>
                    </el-dropdown>
                  </div>
                </div>
              </template>

              <div v-if="isGroupCollapsed(group)" class="collapsed-group-hint">
                该分组默认折叠，点击“展开分组”查看明细。
              </div>

              <div v-else-if="group.visibleCount === 0" class="group-empty-hint">
                当前筛选下没有项目。
              </div>

              <el-table v-else :data="group.items" stripe style="width: 100%">
                <el-table-column label="选中" width="72" align="center">
                  <template #default="{ row }">
                    <el-checkbox :model-value="isRowSelected(row)" @change="(checked) => toggleRowSelection(row, checked)" />
                  </template>
                </el-table-column>

                <el-table-column label="文件" min-width="320" >
                  <template #default="{ row }">
                    <div class="path-flow">
                      <div class="path-row source" :title="row.original_path">
                        <span class="path-label">源</span>
                        <span class="path-name">{{ extractFilename(row.original_path) }}</span>
                      </div>
                      <div class="path-arrow" v-if="row.target_path">→</div>
                      <div class="path-row target" v-if="row.target_path" :title="row.target_path">
                        <span class="path-label">标</span>
                        <span class="path-name">{{ extractFilename(row.target_path) }}</span>
                      </div>
                    </div>
                  </template>
                </el-table-column>

                <el-table-column label="归属" width="120" align="center">
                  <template #default="{ row }">
                    <el-tag :type="bucketTagType(row.tree_bucket)" effect="light">{{ bucketLabel(row.tree_bucket) }}</el-tag>
                  </template>
                </el-table-column>

                <el-table-column label="大小" width="110" align="center">
                  <template #default="{ row }">{{ formatSize(row.size) }}</template>
                </el-table-column>

                <el-table-column label="时间/状态" width="150" align="center">
                  <template #default="{ row }">
                    <div class="status-cell">
                      <div>{{ formatTime(row.updated_at || row.created_at) }}</div>
                      <el-tag v-if="row.status" :type="statusType(row.status)" size="small" effect="dark">{{ statusText(row.status) }}</el-tag>
                    </div>
                  </template>
                </el-table-column>

                <el-table-column label="操作" width="130" align="center" fixed="right">
                  <template #default="{ row }">
                    <div class="row-actions">
                      <el-button class="row-action-button" type="primary" plain size="small" @click="openFixDialog(row)">修正</el-button>
                      <el-dropdown trigger="click" @command="(cmd) => onDeleteItemCommand(row, cmd)">
                        <el-button class="row-action-button" type="danger" plain size="small">删除</el-button>
                        <template #dropdown>
                          <el-dropdown-menu>
                            <el-dropdown-item command="records">仅删记录</el-dropdown-item>
                            <el-dropdown-item command="records_and_links">记录 + 硬链接</el-dropdown-item>
                          </el-dropdown-menu>
                        </template>
                      </el-dropdown>
                    </div>
                  </template>
                </el-table-column>
              </el-table>
            </el-card>
          </div>
        </el-card>
      </template>
    </el-drawer>

    <TaskLogMonitorDrawer
      v-model="fixMonitorVisible"
      title="修正任务日志"
      direction="rtl"
      size="46%"
      append-to-body
      destroy-on-close
      drawer-class="fix-monitor-drawer"
      :before-close="handleOverlayBeforeClose"
      :task-id="fixMonitorTaskId"
      :task-status="fixMonitorTaskStatus"
      :target-label="fixMonitorTargetLabel"
      :last-refreshed-at="fixMonitorLastRefreshedAt"
      :auto-refresh="fixMonitorAutoRefresh"
      :logs="fixMonitorLogs"
      :logs-loading="fixMonitorLogsLoading"
      :waiting-for-task="fixMonitorWaitingForTask"
      :manual-refresh-disabled="!fixMonitorTaskId"
      waiting-text="正在等待修正任务写入日志..."
      running-text="修正进行中，可实时查看日志进度。"
      finished-text="修正已结束，日志面板保持打开，关闭时会一并收起下层抽屉。"
      @update:autoRefresh="fixMonitorAutoRefresh = $event"
      @refresh="manualRefreshFixMonitor"
    />

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

    <el-dialog v-model="fixDialogVisible" title="手动修正识别" width="540px" append-to-body destroy-on-close>
      <el-form ref="fixFormRef" :model="fixForm" label-width="110px">
        <el-form-item label="待处理文件">
          <div class="path-preview" :title="currentFixRow?.original_path">{{ currentFixRow?.original_path || '-' }}</div>
        </el-form-item>
        <el-form-item v-if="fixCompanionRows.length" label="随行附件">
          <div class="companion-hint">将同时对该分组中 {{ fixCompanionRows.length }} 个附件做附带处理</div>
        </el-form-item>
        <el-form-item label="媒体类型" required>
          <el-select v-model="fixForm.media_type" style="width: 180px">
            <el-option label="TV" value="tv" />
            <el-option label="电影" value="movie" />
          </el-select>
        </el-form-item>
        <el-form-item label="TMDB ID" required prop="tmdb_id" :rules="[{ required: true, message: '请填写 TMDB ID', trigger: 'blur' }]">
          <el-input v-model="fixForm.tmdb_id" placeholder="可直接填写 TMDB ID">
            <template #append>
              <el-button @click="openTmdbSearchDialog('fix')">
                <el-icon><Search /></el-icon>
              </el-button>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item label="标题" required prop="title" :rules="[{ required: true, message: '请填写标题', trigger: 'blur' }]">
          <el-input v-model="fixForm.title" placeholder="例如：刀剑神域：序列之争" />
        </el-form-item>
        <el-form-item label="年份">
          <el-input-number v-model="fixForm.year" :min="1900" :max="2100" />
        </el-form-item>
        <el-form-item v-if="fixForm.media_type === 'tv'" label="强制季号">
          <el-input-number v-model="fixForm.season" :min="0" />
        </el-form-item>
        <el-form-item v-if="fixForm.media_type === 'tv'" label="强制集数">
          <el-input-number v-model="fixForm.episode" :min="1" />
        </el-form-item>
        <el-form-item v-if="fixForm.media_type === 'tv'" label="集号偏移">
          <el-input-number v-model="fixForm.episode_offset" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="fixDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="fixing" @click="submitFix">提交修正</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="dirFixDialogVisible"
      :title="dirFixMode === 'season' ? 'Season 修正重整' : '整组资源修正'"
      width="560px"
      append-to-body
      destroy-on-close
    >
      <el-form ref="dirFixFormRef" :model="dirFixForm" label-width="110px">
        <el-form-item label="待办目录">
          <div class="path-preview" :title="dirFixMode === 'season' ? currentNode?.label : drawerResource?.resource_dir">
            {{ dirFixMode === 'season' ? `${drawerResource?.resource_dir || '-'} / ${currentNode?.label || '-'}` : (drawerResource?.resource_dir || '-') }}
          </div>
        </el-form-item>
        <el-form-item label="媒体类型" required>
          <el-select v-model="dirFixForm.media_type" style="width: 180px">
            <el-option label="TV" value="tv" />
            <el-option label="电影" value="movie" />
          </el-select>
        </el-form-item>
        <el-form-item label="TMDB ID" required prop="tmdb_id" :rules="[{ required: true, message: '请填写 TMDB ID', trigger: 'blur' }]">
          <el-input v-model="dirFixForm.tmdb_id" placeholder="可直接填写 TMDB ID">
            <template #append>
              <el-button @click="openTmdbSearchDialog('dir')">
                <el-icon><Search /></el-icon>
              </el-button>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item label="标题" required prop="title" :rules="[{ required: true, message: '请填写标题', trigger: 'blur' }]">
          <el-input v-model="dirFixForm.title" placeholder="例如：刀剑神域：序列之争" />
        </el-form-item>
        <el-form-item label="年份">
          <el-input-number v-model="dirFixForm.year" :min="1900" :max="2100" />
        </el-form-item>
        <el-form-item v-if="dirFixForm.media_type === 'tv'" label="强制季号">
          <el-input-number v-model="dirFixForm.season" :min="0" />
        </el-form-item>
        <el-form-item v-if="dirFixForm.media_type === 'tv' && dirFixForm.season === 0" label="强制集数" required prop="episode_override" :rules="[{ required: true, type: 'number', message: '强制季号为 0 时必须填写强制集数', trigger: 'change' }]">
          <el-input-number v-model="dirFixForm.episode_override" :min="1" />
        </el-form-item>
        <el-form-item v-if="dirFixForm.media_type === 'tv'" label="集号偏移">
          <el-input-number v-model="dirFixForm.episode_offset" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dirFixDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="dirFixing" @click="submitDirFix">提交修正</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="tmdbSearchDialogVisible"
      title="搜索 TMDB 条目"
      width="860px"
      class="tmdb-search-dialog"
      append-to-body
      destroy-on-close
    >
      <div class="search-bar">
        <el-input
          v-model="tmdbSearchKeyword"
          placeholder="电影或电视剧名称 / 直接输入 TMDB ID"
          clearable
          @keyup.enter="runTmdbSearch"
        >
          <template #prepend>
            <el-icon><Search /></el-icon>
          </template>
          <template #append>
            <el-button :loading="tmdbSearchLoading" @click="runTmdbSearch">搜索</el-button>
          </template>
        </el-input>
      </div>

      <div class="search-list" v-loading="tmdbSearchLoading">
        <el-scrollbar max-height="560px">
          <div v-if="tmdbSearchResults.length" class="search-items">
            <div
              v-for="item in tmdbSearchResults"
              :key="`${item.media_type}:${item.tmdb_id}`"
              class="search-item"
              @click="selectTmdbSearchItem(item)"
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
                  <span v-if="item.year" class="year">({{ item.year }})</span>
                  <el-tag size="small" effect="plain" :type="item.media_type === 'tv' ? 'primary' : 'success'">
                    {{ item.media_type === 'tv' ? '电视剧' : '电影' }}
                  </el-tag>
                </div>
                <div class="overview">{{ item.overview || '暂无简介' }}</div>
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
import { computed, defineAsyncComponent, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Film, Monitor, Refresh, Search } from '@element-plus/icons-vue'
import dayjs from 'dayjs'
import { mediaApi, tasksApi } from '../api/client'
import { buildConfirmDialogOptions, buildConfirmMessage } from '../utils/confirmMessage'
import { animateFloatingPosterEnter, animateFloatingPosterLeave, resourceIdentityKey, setResourceIconElement } from '../utils/floatingPosterMotion'
import { useResourcePoster } from '../utils/resourcePosterStore'

const TaskLogMonitorDrawer = defineAsyncComponent(() => import('../components/TaskLogMonitorDrawer.vue'))

const loading = ref(false)
const resources = ref([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)
const filters = reactive({ search: '', type: '', category: 'all' })

const drawerVisible = ref(false)
const drawerLoading = ref(false)
const drawerTitle = ref('')
const drawerResource = ref(null)
const drawerTree = ref([])
const selectedNodeKey = ref('')
const drawerSearch = ref('')
const drawerCategory = ref('all')
const drawerGroupFilters = ref({})
const drawerSelectedIds = ref([])
const collapsedGroups = ref({})
const resourceIconElements = new Map()

const { ensurePosterForResource, hydrateCachedPosterMeta, resourcePosterUrl, resourcePosterAlt } = useResourcePoster(
  (params) => mediaApi.poster(params).then(({ data }) => data),
)

const fixDialogVisible = ref(false)
const fixing = ref(false)
const currentFixRow = ref(null)
const fixCompanionRows = ref([])
const fixFormRef = ref(null)
const fixForm = reactive({ media_type: 'tv', tmdb_id: '', title: '', year: undefined, season: 1, episode: 1, episode_offset: undefined })

const dirFixDialogVisible = ref(false)
const dirFixing = ref(false)
const dirFixMode = ref('resource')
const dirFixScope = ref(null)
const dirFixFormRef = ref(null)
const dirFixForm = reactive({ media_type: 'tv', tmdb_id: '', title: '', year: undefined, season: undefined, episode_override: undefined, episode_offset: undefined })
const tmdbSearchDialogVisible = ref(false)
const tmdbSearchLoading = ref(false)
const tmdbSearchKeyword = ref('')
const tmdbSearchResults = ref([])
const tmdbSearchTarget = ref('fix')
const fixMonitorVisible = ref(false)
const fixMonitorLogsLoading = ref(false)
const fixMonitorLogs = ref([])
const fixMonitorTaskId = ref(null)
const fixMonitorTaskStatus = ref('pending')
const fixMonitorTargetLabel = ref('')
const fixMonitorLastRefreshedAt = ref('')
const fixMonitorWaitingForTask = ref(false)
const fixMonitorAutoRefresh = ref(true)
const fixMonitorRequestPending = ref(false)
const fixMonitorMatchSpec = ref(null)
const fixMonitorHandledTerminalTaskId = ref(null)
let fixMonitorTimer = null

const VIDEO_FILE_EXTENSIONS = new Set([
  '.3g2',
  '.3gp',
  '.asf',
  '.avi',
  '.flv',
  '.m2ts',
  '.m4v',
  '.mkv',
  '.mov',
  '.mp4',
  '.mpeg',
  '.mpg',
  '.mts',
  '.ogm',
  '.rm',
  '.rmvb',
  '.ts',
  '.vob',
  '.webm',
  '.wmv',
])

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

function formatSize(size) {
  const value = Number(size || 0)
  if (!value) return '-'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let next = value
  let index = 0
  while (next >= 1024 && index < units.length - 1) {
    next /= 1024
    index += 1
  }
  return `${next.toFixed(index === 0 ? 0 : 1)} ${units[index]}`
}

function clearFixMonitorTimer() {
  if (fixMonitorTimer) {
    clearInterval(fixMonitorTimer)
    fixMonitorTimer = null
  }
}

function resetFixMonitorState() {
  clearFixMonitorTimer()
  fixMonitorLogsLoading.value = false
  fixMonitorLogs.value = []
  fixMonitorTaskId.value = null
  fixMonitorTaskStatus.value = 'pending'
  fixMonitorTargetLabel.value = ''
  fixMonitorLastRefreshedAt.value = ''
  fixMonitorWaitingForTask.value = false
  fixMonitorAutoRefresh.value = true
  fixMonitorRequestPending.value = false
  fixMonitorMatchSpec.value = null
  fixMonitorHandledTerminalTaskId.value = null
}

function closeAllMediaOverlays() {
  fixDialogVisible.value = false
  dirFixDialogVisible.value = false
  tmdbSearchDialogVisible.value = false
  fixMonitorVisible.value = false
  drawerVisible.value = false
  resetFixMonitorState()
}

function handleOverlayBeforeClose(done) {
  closeAllMediaOverlays()
  done()
}

function isTerminalTaskStatus(status) {
  return ['completed', 'failed', 'cancelled'].includes(String(status || ''))
}

function buildFixMonitorMatchSpec({ typePrefix, targetName, targetLabel }) {
  return {
    typePrefix,
    targetName,
    targetLabel,
    startedAtMs: Date.now(),
  }
}

function findMatchingFixMonitorTask(tasks) {
  const spec = fixMonitorMatchSpec.value
  if (!spec) return null
  const expectedTargetName = String(spec.targetName || '')
  return (tasks || []).find((task) => {
    const taskType = String(task?.type || '')
    const taskTargetName = String(task?.target_name || '')
    const createdAt = dayjs(task?.created_at).valueOf()
    return taskType.startsWith(spec.typePrefix)
      && (!expectedTargetName
        || taskTargetName === expectedTargetName
        || taskTargetName.endsWith(` / ${expectedTargetName}`))
      && Number.isFinite(createdAt)
      && createdAt >= spec.startedAtMs - 5000
  }) || null
}

function buildScopedFixMonitorTargetName(resourceDir, groupLabel) {
  const resourceName = extractFilename(resourceDir)
  const label = String(groupLabel || '').trim()
  if (resourceName && label && label !== resourceName) {
    return `${resourceName} / ${label}`
  }
  return resourceName || label || ''
}

async function fetchFixMonitorLogs(taskId, { silent = false } = {}) {
  if (!taskId) return
  if (!silent) {
    fixMonitorLogsLoading.value = true
  }
  try {
    const { data } = await tasksApi.getLogs(taskId)
    fixMonitorLogs.value = data?.logs || []
    fixMonitorLastRefreshedAt.value = dayjs().format('HH:mm:ss')
  } catch {
    if (!silent) {
      fixMonitorLogs.value = ['加载日志失败']
    }
  } finally {
    if (!silent) {
      fixMonitorLogsLoading.value = false
    }
  }
}

async function refreshFixMonitorTask({ silent = false } = {}) {
  if (!fixMonitorVisible.value) return
  try {
    const { data } = await tasksApi.list({ limit: 30, offset: 0 })
    const tasks = data?.items || []
    if (!fixMonitorTaskId.value) {
      const matched = findMatchingFixMonitorTask(tasks)
      if (matched) {
        fixMonitorTaskId.value = matched.id
        fixMonitorTaskStatus.value = String(matched.status || 'running')
        fixMonitorWaitingForTask.value = false
        await fetchFixMonitorLogs(matched.id, { silent })
        return
      }
      fixMonitorWaitingForTask.value = true
      return
    }

    const matched = tasks.find((task) => Number(task?.id) === Number(fixMonitorTaskId.value))
    if (matched) {
      fixMonitorTaskStatus.value = String(matched.status || fixMonitorTaskStatus.value || 'running')
    }
  } catch {
    if (!fixMonitorTaskId.value) {
      fixMonitorWaitingForTask.value = true
    }
  }
}

function restartFixMonitorAutoRefresh() {
  clearFixMonitorTimer()
  if (!fixMonitorVisible.value || !fixMonitorAutoRefresh.value) return
  fixMonitorTimer = setInterval(async () => {
    await refreshFixMonitorTask({ silent: true })
    if (fixMonitorTaskId.value) {
      await fetchFixMonitorLogs(fixMonitorTaskId.value, { silent: true })
    }
    if (!fixMonitorRequestPending.value && isTerminalTaskStatus(fixMonitorTaskStatus.value)) {
      clearFixMonitorTimer()
    }
  }, 2000)
}

function onFixMonitorAutoRefreshChange() {
  restartFixMonitorAutoRefresh()
}

async function manualRefreshFixMonitor() {
  await refreshFixMonitorTask()
  if (fixMonitorTaskId.value) {
    await fetchFixMonitorLogs(fixMonitorTaskId.value)
  }
}

function openFixMonitor(spec) {
  resetFixMonitorState()
  fixMonitorMatchSpec.value = spec
  fixMonitorTargetLabel.value = spec.targetLabel || spec.targetName || ''
  fixMonitorVisible.value = true
  fixMonitorWaitingForTask.value = true
  fixMonitorRequestPending.value = true
  restartFixMonitorAutoRefresh()
}

async function attachFixMonitorTask(taskId) {
  if (!taskId) return
  fixMonitorTaskId.value = Number(taskId)
  fixMonitorWaitingForTask.value = false
  await refreshFixMonitorTask({ silent: true })
  await fetchFixMonitorLogs(fixMonitorTaskId.value)
  restartFixMonitorAutoRefresh()
}

function posterUrl(posterPath) {
  if (!posterPath) return ''
  if (String(posterPath).startsWith('http')) return posterPath
  return `https://image.tmdb.org/t/p/w500${posterPath}`
}

function extractResourceTitleAndYear(resource) {
  const rawName = String(resource?.resource_name || '').trim()
  if (!rawName) {
    return { title: '', year: undefined }
  }
  const match = rawName.match(/^(.*?)(?:\s*\((\d{4})\))?$/)
  const title = String(match?.[1] || rawName).trim() || rawName
  const year = match?.[2] ? Number(match[2]) : undefined
  return { title, year }
}

function activeSearchMediaType() {
  if (tmdbSearchTarget.value === 'dir') {
    return dirFixForm.media_type === 'movie' ? 'movie' : 'tv'
  }
  return fixForm.media_type === 'movie' ? 'movie' : 'tv'
}

function openTmdbSearchDialog(target) {
  tmdbSearchTarget.value = target === 'dir' ? 'dir' : 'fix'
  tmdbSearchKeyword.value = String(tmdbSearchTarget.value === 'dir' ? (dirFixForm.title || '') : (fixForm.title || '')).trim()
  tmdbSearchResults.value = []
  tmdbSearchDialogVisible.value = true
}

async function runTmdbSearch() {
  const keyword = String(tmdbSearchKeyword.value || '').trim()
  if (!keyword) {
    ElMessage.warning('请输入关键词或 TMDB ID')
    return
  }
  tmdbSearchLoading.value = true
  try {
    const { data } = await mediaApi.searchTmdb({
      q: keyword,
      media_type: activeSearchMediaType(),
      limit: 20,
    })
    tmdbSearchResults.value = data?.items || []
    if (!tmdbSearchResults.value.length) {
      ElMessage.info('未找到匹配结果')
    }
  } catch (error) {
    ElMessage.error(error?.response?.data?.detail || error?.message || '搜索失败')
  } finally {
    tmdbSearchLoading.value = false
  }
}

function selectTmdbSearchItem(item) {
  const targetForm = tmdbSearchTarget.value === 'dir' ? dirFixForm : fixForm
  targetForm.tmdb_id = item?.tmdb_id ? String(item.tmdb_id) : ''
  targetForm.title = item?.title || ''
  targetForm.year = item?.year ?? undefined
  tmdbSearchDialogVisible.value = false
}

function tmdbLink(item) {
  if (!item?.tmdb_id) return '#'
  const type = item.media_type === 'tv' ? 'tv' : 'movie'
  return `https://www.themoviedb.org/${type}/${item.tmdb_id}`
}

function bucketLabel(bucket) {
  if (bucket === 'main') return '正片'
  if (bucket === 'aux') return 'SP/Extras'
  return 'Misc'
}

function bucketTagType(bucket) {
  if (bucket === 'main') return 'primary'
  if (bucket === 'aux') return 'warning'
  return 'info'
}

function statusType(status) {
  if (status === 'scraped' || status === 'manual_fixed') return 'success'
  if (status === 'pending_manual') return 'warning'
  if (status === 'failed') return 'danger'
  return 'info'
}

function statusText(status) {
  if (status === 'scraped') return '已整理'
  if (status === 'manual_fixed') return '已修正'
  if (status === 'pending_manual') return '待处理'
  if (status === 'failed') return '失败'
  return status || '-'
}

function itemMatchesSearch(item, keyword) {
  if (!keyword) return true
  const text = [item.original_path, item.target_path, extractFilename(item.original_path), extractFilename(item.target_path)]
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
  return text.includes(keyword)
}

function supportsGroupFilter(group) {
  return ['main', 'aux'].includes(String(group?.kind || ''))
}

function extractItemExtension(item) {
  const path = String(item?.target_path || item?.original_path || '').split('?')[0]
  const filename = extractFilename(path)
  const dotIndex = filename.lastIndexOf('.')
  if (dotIndex < 0) return ''
  return filename.slice(dotIndex).toLowerCase()
}

function isVideoItem(item) {
  return VIDEO_FILE_EXTENSIONS.has(extractItemExtension(item))
}

function itemMatchesGroupFilter(item, mode, selectedIds) {
  if (mode === 'video') return isVideoItem(item)
  if (mode === 'attachment') return !isVideoItem(item)
  if (mode === 'selected') return selectedIds.has(item.id)
  return true
}

function buildDrawerGroupFilters(nodes) {
  return Object.fromEntries(
    (nodes || [])
      .flatMap((node) => node.groups || [])
      .filter((group) => supportsGroupFilter(group))
      .map((group) => [group.key, 'video']),
  )
}

function updateGroupFilter(group, value) {
  drawerGroupFilters.value = {
    ...drawerGroupFilters.value,
    [group.key]: value || 'all',
  }
}

const displayNodes = computed(() => {
  const keyword = String(drawerSearch.value || '').trim().toLowerCase()
  const category = drawerCategory.value || 'all'
  const selectedIds = new Set(drawerSelectedIds.value || [])
  return (drawerTree.value || [])
    .map((node) => {
      const groups = (node.groups || [])
        .map((group) => {
          const baseItems = (group.items || []).filter((item) => {
            if (category === 'main' && item.tree_bucket !== 'main') return false
            if (category === 'sps' && item.tree_bucket === 'main') return false
            return itemMatchesSearch(item, keyword)
          })
          const groupMode = supportsGroupFilter(group) ? (drawerGroupFilters.value[group.key] || 'all') : 'all'
          const items = baseItems.filter((item) => itemMatchesGroupFilter(item, groupMode, selectedIds))
          return {
            ...group,
            items,
            visibleCount: items.length,
            baseVisibleCount: baseItems.length,
          }
        })
        .filter((group) => group.baseVisibleCount > 0)
      const visibleCount = groups.reduce((sum, group) => sum + group.items.length, 0)
      return { ...node, groups, visibleCount }
    })
    .filter((node) => node.groups.length > 0)
})

const currentNode = computed(() => displayNodes.value.find((node) => node.key === selectedNodeKey.value) || displayNodes.value[0] || null)
const selectedDrawerItems = computed(() => {
  const selectedIds = new Set(drawerSelectedIds.value || [])
  const items = []
  for (const node of drawerTree.value || []) {
    for (const group of node.groups || []) {
      for (const item of group.items || []) {
        if (selectedIds.has(item.id)) {
          items.push(item)
        }
      }
    }
  }
  return items
})
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

async function loadResources() {
  loading.value = true
  try {
    const { data } = await mediaApi.resources({
      offset: (page.value - 1) * pageSize.value,
      limit: pageSize.value,
      search: filters.search || undefined,
      type: filters.type || undefined,
      category: filters.category || 'all',
    })
    resources.value = data.items || []
    hydrateCachedPosterMeta(resources.value)
    total.value = Number(data.total || 0)
  } catch (error) {
    ElMessage.error(error?.response?.data?.detail || error?.message || '加载媒体资源失败')
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
  filters.type = ''
  filters.category = 'all'
  onFilterChanged()
}

async function loadDrawerTree(resource) {
  if (!resource?.resource_dir) return
  drawerLoading.value = true
  try {
    const { data } = await mediaApi.resourceTree({
      resource_dir: resource.resource_dir,
      type: resource.type || undefined,
      sync_group_id: resource.sync_group_id || undefined,
      tmdb_id: resource.tmdb_id || undefined,
    })
    drawerResource.value = data.resource || resource
    ensurePosterForResource(drawerResource.value)
    drawerTree.value = data.nodes || []
    drawerGroupFilters.value = buildDrawerGroupFilters(data.nodes || [])
    collapsedGroups.value = Object.fromEntries(
      (data.nodes || [])
        .flatMap((node) => node.groups || [])
        .map((group) => [group.key, ['main', 'aux', 'extras'].includes(group.kind)])
    )
    drawerSelectedIds.value = []
  } catch (error) {
    ElMessage.error(error?.response?.data?.detail || error?.message || '加载资源树失败')
  } finally {
    drawerLoading.value = false
  }
}

async function openResourceDrawer(resource) {
  drawerTitle.value = resource?.resource_name || '资源详情'
  drawerVisible.value = true
  drawerSearch.value = ''
  drawerCategory.value = 'all'
  drawerGroupFilters.value = {}
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
  drawerGroupFilters.value = buildDrawerGroupFilters(drawerTree.value || [])
  reloadDrawer()
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

function isRowSelected(row) {
  return drawerSelectedIds.value.includes(row?.id)
}

function toggleRowSelection(row, checked) {
  const rowId = Number(row?.id || 0)
  if (!rowId) return
  const selected = new Set(drawerSelectedIds.value || [])
  if (checked) {
    selected.add(rowId)
  } else {
    selected.delete(rowId)
  }
  drawerSelectedIds.value = Array.from(selected)
}

function clearDrawerSelection() {
  drawerSelectedIds.value = []
}

function scopeVisibleCount(scope, fallbackCount = 0) {
  if (!scope) return fallbackCount
  return fallbackCount
}

async function confirmScopeDelete(label, scope, visibleCount, deleteFiles) {
  const actualCount = Number(scope?.item_ids?.length || 0)
  const visibleText = visibleCount !== actualCount ? `当前筛选可见 ${visibleCount} 条，实际会处理 ${actualCount} 条。` : `将处理 ${actualCount} 条记录。`
  const actionText = deleteFiles ? '并删除硬链接文件' : '仅删除记录'
  await ElMessageBox.confirm(
    buildConfirmMessage([
      label,
      visibleText,
      `操作：${actionText}`,
    ]),
    '确认删除',
    buildConfirmDialogOptions(),
  )
}

async function deleteMediaScope(scope, deleteFiles) {
  const payload = { ...scope, delete_files: !!deleteFiles }
  const { data } = await mediaApi.deleteScope(payload)
  return data
}

async function onDeleteResourceCommand(row, command) {
  if (!row?.sample_id) {
    ElMessage.warning('当前资源缺少可删除记录')
    return
  }
  const deleteFiles = command === 'records_and_links'
  try {
    await ElMessageBox.confirm(
      buildConfirmMessage([
        deleteFiles ? '将删除该资源记录及硬链接文件，是否继续？' : '将删除该资源记录，是否继续？',
      ]),
      '确认删除',
      buildConfirmDialogOptions(),
    )
    await mediaApi.batchDelete({ ids: [row.sample_id], delete_files: deleteFiles, delete_resource_scope: true })
    ElMessage.success('资源删除完成')
    if (drawerVisible.value && drawerResource.value?.key === row.key) {
      closeAllMediaOverlays()
    }
    await loadResources()
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除资源失败')
  }
}

async function onDeleteDrawerResourceCommand(command) {
  const scope = drawerResource.value?.scope
  if (!scope) {
    ElMessage.warning('当前资源作用域不可用')
    return
  }
  const deleteFiles = command === 'records_and_links'
  try {
    await confirmScopeDelete(`删除资源 ${drawerResource.value.resource_name}`, scope, displayNodes.value.reduce((sum, node) => sum + node.visibleCount, 0), deleteFiles)
    await deleteMediaScope(scope, deleteFiles)
    ElMessage.success('资源删除完成')
    closeAllMediaOverlays()
    await loadResources()
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除资源失败')
  }
}

async function onDeleteNodeCommand(node, command) {
  const deleteFiles = command === 'records_and_links'
  try {
    await confirmScopeDelete(`删除 ${node.label}`, node.scope, scopeVisibleCount(node.scope, node.visibleCount), deleteFiles)
    await deleteMediaScope(node.scope, deleteFiles)
    ElMessage.success('节点删除完成')
    await Promise.all([loadResources(), reloadDrawer()])
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除节点失败')
  }
}

async function onDeleteGroupCommand(group, command) {
  const deleteFiles = command === 'records_and_links'
  try {
    await confirmScopeDelete(`删除分组 ${group.label}`, group.scope, scopeVisibleCount(group.scope, group.visibleCount), deleteFiles)
    await deleteMediaScope(group.scope, deleteFiles)
    ElMessage.success('分组删除完成')
    await Promise.all([loadResources(), reloadDrawer()])
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除分组失败')
  }
}

async function onDeleteItemCommand(row, command) {
  const deleteFiles = command === 'records_and_links'
  try {
    await confirmScopeDelete(`删除文件 ${extractFilename(row.original_path)}`, row.scope, 1, deleteFiles)
    await deleteMediaScope(row.scope, deleteFiles)
    ElMessage.success('删除成功')
    await Promise.all([loadResources(), reloadDrawer()])
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除失败')
  }
}

async function deleteSelectedItems(command) {
  const deleteFiles = command === 'records_and_links'
  const ids = selectedDrawerItems.value.map((item) => Number(item.id)).filter((value) => value > 0)
  if (!ids.length) {
    ElMessage.warning('请先选择具体记录')
    return
  }
  try {
    await ElMessageBox.confirm(
      buildConfirmMessage([
        deleteFiles ? `将删除选中的 ${ids.length} 条记录及硬链接，是否继续？` : `将删除选中的 ${ids.length} 条记录，是否继续？`,
      ]),
      '删除选中记录',
      buildConfirmDialogOptions(),
    )
    await mediaApi.batchDelete({ ids, delete_files: deleteFiles })
    ElMessage.success('选中记录删除完成')
    await Promise.all([loadResources(), reloadDrawer()])
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '删除选中记录失败')
  }
}

function findGroupCompanionAttachments(row) {
  // 在未筛选的原始树中查找 row 所属分组，收集同组非视频附件
  for (const node of drawerTree.value || []) {
    for (const group of node.groups || []) {
      const inGroup = (group.items || []).some((item) => item.id === row.id)
      if (inGroup) {
        return (group.items || []).filter((item) => item.id !== row.id && !isVideoItem(item))
      }
    }
  }
  return []
}

function openFixDialog(row) {
  currentFixRow.value = row
  fixForm.media_type = row?.type === 'movie' ? 'movie' : 'tv'
  fixForm.tmdb_id = row.tmdb_id || drawerResource.value?.tmdb_id || ''
  const { title: defaultTitle, year: defaultYear } = extractResourceTitleAndYear(drawerResource.value)
  fixForm.title = defaultTitle
  fixForm.year = defaultYear
  fixForm.season = row.tree_season || currentNode.value?.season || 1
  fixForm.episode = 1
  fixForm.episode_offset = undefined
  fixCompanionRows.value = isVideoItem(row) ? findGroupCompanionAttachments(row) : []
  tmdbSearchResults.value = []
  fixDialogVisible.value = true
}

async function submitFix() {
  if (!currentFixRow.value?.id) {
    ElMessage.warning('请先选择待修正文件')
    return
  }
  try {
    await fixFormRef.value?.validate()
  } catch {
    return
  }
  if (fixForm.media_type === 'tv' && fixForm.season === 0 && (fixForm.episode === undefined || fixForm.episode === null)) {
    ElMessage.warning('强制季号为 0 时必须填写强制集数')
    return
  }
  fixing.value = true
  openFixMonitor(
    buildFixMonitorMatchSpec({
      typePrefix: 'reidentify:item:',
      targetName: extractFilename(currentFixRow.value?.original_path),
      targetLabel: `单文件修正 · ${extractFilename(currentFixRow.value?.original_path)}`,
    }),
  )
  try {
    const { data } = await mediaApi.reidentify(currentFixRow.value.id, {
      media_type: fixForm.media_type,
      tmdb_id: Number(fixForm.tmdb_id),
      title: fixForm.title,
      year: fixForm.year ? Number(fixForm.year) : undefined,
      season: fixForm.media_type === 'tv' ? (fixForm.season ?? undefined) : undefined,
      episode: fixForm.media_type === 'tv' ? (fixForm.episode ?? undefined) : undefined,
      episode_offset: fixForm.media_type === 'tv' ? (fixForm.episode_offset ?? undefined) : undefined,
      companion_ids: fixCompanionRows.value.length ? fixCompanionRows.value.map((r) => r.id) : undefined,
    })
    await attachFixMonitorTask(data?.task_id)
    ElMessage.success(data?.message || '修正任务已进入队列')
    fixDialogVisible.value = false
  } catch (error) {
    await refreshFixMonitorTask()
    if (fixMonitorTaskId.value) {
      await fetchFixMonitorLogs(fixMonitorTaskId.value)
    }
    if (!fixMonitorTaskId.value) {
      resetFixMonitorState()
      fixMonitorVisible.value = false
    }
    ElMessage.error(error?.response?.data?.detail || error?.message || '修正失败')
  } finally {
    fixing.value = false
    fixMonitorRequestPending.value = false
    restartFixMonitorAutoRefresh()
  }
}

function openDirFixDialog() {
  dirFixMode.value = 'resource'
  dirFixScope.value = drawerResource.value?.scope || null
  dirFixForm.media_type = drawerResource.value?.type === 'movie' ? 'movie' : 'tv'
  dirFixForm.tmdb_id = drawerResource.value?.tmdb_id || ''
  const { title, year } = extractResourceTitleAndYear(drawerResource.value)
  dirFixForm.title = title
  dirFixForm.year = year
  dirFixForm.season = undefined
  dirFixForm.episode_override = undefined
  dirFixForm.episode_offset = undefined
  tmdbSearchResults.value = []
  dirFixDialogVisible.value = true
}

function openSeasonFixDialog(node) {
  dirFixMode.value = 'season'
  dirFixScope.value = node?.scope || null
  dirFixForm.media_type = drawerResource.value?.type === 'movie' ? 'movie' : 'tv'
  dirFixForm.tmdb_id = drawerResource.value?.tmdb_id || ''
  const { title, year } = extractResourceTitleAndYear(drawerResource.value)
  dirFixForm.title = title
  dirFixForm.year = year
  dirFixForm.season = node?.season ?? currentNode.value?.season ?? 1
  dirFixForm.episode_override = undefined
  dirFixForm.episode_offset = undefined
  tmdbSearchResults.value = []
  dirFixDialogVisible.value = true
}

async function submitDirFix() {
  if (!drawerResource.value?.resource_dir) {
    ElMessage.warning('当前资源目录不可用')
    return
  }
  try {
    await dirFixFormRef.value?.validate()
  } catch {
    return
  }
  if (dirFixMode.value === 'resource' && drawerResource.value?.type === 'tv') {
    ElMessage.warning('TV 资源仅保留 Season 级修正，请在具体 Season 节点执行')
    return
  }
  dirFixing.value = true
  openFixMonitor(
    buildFixMonitorMatchSpec({
      typePrefix: dirFixMode.value === 'season' ? 'reidentify:season:' : 'reidentify:resource:',
      targetName: dirFixMode.value === 'season'
        ? buildScopedFixMonitorTargetName(drawerResource.value?.resource_dir, dirFixScope.value?.group_label || currentNode.value?.label || '')
        : extractFilename(drawerResource.value?.resource_dir),
      targetLabel: dirFixMode.value === 'season'
        ? `季度修正 · ${buildScopedFixMonitorTargetName(drawerResource.value?.resource_dir, dirFixScope.value?.group_label || currentNode.value?.label || '')}`
        : `整组修正 · ${extractFilename(drawerResource.value?.resource_dir)}`,
    }),
  )
  try {
    const payload = {
      media_type: dirFixForm.media_type,
      tmdb_id: Number(dirFixForm.tmdb_id),
      title: dirFixForm.title,
      year: dirFixForm.year ? Number(dirFixForm.year) : undefined,
      season_override: dirFixForm.media_type === 'tv' ? (dirFixForm.season ?? undefined) : undefined,
      episode_override: (dirFixForm.media_type === 'tv' && dirFixForm.season === 0) ? (dirFixForm.episode_override ?? undefined) : undefined,
      episode_offset: dirFixForm.media_type === 'tv' ? (dirFixForm.episode_offset ?? undefined) : undefined,
    }
    const { data } = dirFixMode.value === 'season'
      ? await mediaApi.reidentifyScope({
          ...(dirFixScope.value || {}),
          scope_tmdb_id: dirFixScope.value?.tmdb_id ?? drawerResource.value?.tmdb_id ?? undefined,
          ...payload,
        })
      : await mediaApi.reidentifyByTargetDir({
          target_dir: drawerResource.value.resource_dir,
          ...payload,
        })
    await attachFixMonitorTask(data?.task_id)
    ElMessage.success(data?.message || (dirFixMode.value === 'season' ? 'Season 修正任务已进入队列' : '整组修正任务已进入队列'))
    dirFixDialogVisible.value = false
  } catch (error) {
    await refreshFixMonitorTask()
    if (fixMonitorTaskId.value) {
      await fetchFixMonitorLogs(fixMonitorTaskId.value)
    }
    if (!fixMonitorTaskId.value) {
      resetFixMonitorState()
      fixMonitorVisible.value = false
    }
    ElMessage.error(error?.response?.data?.detail || error?.message || (dirFixMode.value === 'season' ? 'Season 修正失败' : '整组修正失败'))
  } finally {
    dirFixing.value = false
    fixMonitorRequestPending.value = false
    restartFixMonitorAutoRefresh()
  }
}

async function deduplicateRecords() {
  try {
    await ElMessageBox.confirm('将按原始路径保留最新记录并删除重复项，是否继续？', '记录去重', { type: 'warning' })
    const { data } = await mediaApi.deduplicate()
    ElMessage.success(data?.message || '去重完成')
    await Promise.all([loadResources(), reloadDrawer()])
  } catch (error) {
    if (error === 'cancel') return
    ElMessage.error(error?.response?.data?.detail || error?.message || '去重失败')
  }
}

onMounted(() => {
  loadResources()
})

watch(fixMonitorVisible, (visible) => {
  if (!visible) {
    resetFixMonitorState()
    return
  }
  restartFixMonitorAutoRefresh()
})

watch(fixMonitorTaskStatus, async (status) => {
  if (!fixMonitorTaskId.value || !isTerminalTaskStatus(status)) return
  if (fixMonitorHandledTerminalTaskId.value === fixMonitorTaskId.value) return
  fixMonitorHandledTerminalTaskId.value = fixMonitorTaskId.value
  await Promise.all([loadResources(), reloadDrawer()])
})

watch(drawerVisible, (visible) => {
  if (!visible && fixMonitorVisible.value) {
    fixMonitorVisible.value = false
    resetFixMonitorState()
  }
})

onBeforeUnmount(() => {
  clearFixMonitorTimer()
})
</script>

<style scoped>
.media-page {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.header,
.header-actions,
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
  background: linear-gradient(135deg, #eef6ff, #f7f3e8);
  display: flex;
  align-items: center;
  justify-content: center;
  color: #315c8a;
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

.resource-body,
.node-title,
.group-title,
.status-cell,
.path-flow {
  display: flex;
  flex-direction: column;
  gap: 4px;
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
.drawer-path {
  word-break: break-all;
}

.path-preview {
  font-size: 12px;
  color: #64748b;
  word-break: break-all;
}

.companion-hint {
  font-size: 13px;
  color: #059669;
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

.selection-summary {
  color: #475569;
  font-size: 13px;
  font-weight: 600;
}

.logs-drawer-body {
  display: flex;
  flex-direction: column;
  gap: 14px;
  height: 100%;
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

.pagination-container {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
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

.group-empty-hint {
  padding: 12px 4px 4px;
  color: #64748b;
  font-size: 13px;
}

.group-filter-select {
  width: 144px;
}

.path-row {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.path-label {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border-radius: 6px;
  background: #eff6ff;
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 600;
}

.path-name,
.text-ellipsis {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.path-arrow {
  color: #94a3b8;
  margin-left: 28px;
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
  z-index: 1900;
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

:deep(.fix-monitor-drawer .el-drawer__body) {
  padding-top: 8px;
}

:deep(.row-actions .el-dropdown) {
  display: inline-flex;
}

@media (max-width: 1180px) {
  .drawer-floating-poster-shell {
    display: none;
  }
}

@media (max-width: 960px) {
  .header,
  .drawer-toolbar,
  .node-card-header,
  .group-header {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>