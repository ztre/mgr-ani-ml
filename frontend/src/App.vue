<template>
  <el-config-provider :locale="zhCn">
    <router-view v-if="isLoginPage" />
    <template v-else>
      <el-container class="app-container">
        <el-aside width="240px" class="sidebar">
          <div class="logo">Anime Media Manager</div>
          <el-menu
            :default-active="$route.path"
            router
            class="sidebar-menu"
          >
            <el-menu-item index="/">
              <el-icon><Monitor /></el-icon>
              <span>仪表盘</span>
            </el-menu-item>
            <el-menu-item index="/sync-groups">
              <el-icon><FolderOpened /></el-icon>
              <span>同步组</span>
            </el-menu-item>
            <el-menu-item index="/media">
              <el-icon><VideoPlay /></el-icon>
              <span>媒体记录</span>
            </el-menu-item>
            <el-menu-item index="/checks">
              <el-icon><Search /></el-icon>
              <span>检查中心</span>
            </el-menu-item>
            <el-menu-item index="/pending">
              <el-icon><WarningFilled /></el-icon>
              <span>待办清单</span>
            </el-menu-item>
            <el-menu-item index="/pending-logs">
              <el-icon><Document /></el-icon>
              <span>人工修正日志</span>
            </el-menu-item>
            <el-menu-item index="/inodes">
              <el-icon><Files /></el-icon>
              <span>Inode 管理</span>
            </el-menu-item>
            <el-menu-item index="/config">
              <el-icon><Setting /></el-icon>
              <span>配置</span>
            </el-menu-item>
          </el-menu>
          <div class="sidebar-footer">
            <el-button class="sidebar-tool-btn" @click="openAppLogDrawer">
              <span class="sidebar-footer-btn__content">
                <el-icon class="sidebar-footer-btn__icon"><Document /></el-icon>
                <span class="sidebar-footer-btn__label">后端日志</span>
              </span>
            </el-button>
            <el-button class="logout-btn" @click="logout">
              <span class="sidebar-footer-btn__content">
                <el-icon class="sidebar-footer-btn__icon"><SwitchButton /></el-icon>
                <span class="sidebar-footer-btn__label">退出登录</span>
              </span>
            </el-button>
          </div>
        </el-aside>
        <el-main class="main-content">
          <div class="page-shell">
            <router-view />
          </div>
        </el-main>
      </el-container>

      <TaskLogMonitorDrawer
        v-model="appLogDrawerVisible"
        title="后端应用日志"
        size="46%"
        drawer-class="logs-drawer"
        :append-to-body="true"
        :target-label="appLogTargetLabel"
        :last-refreshed-at="appLogLastRefreshedAt"
        :auto-refresh="appLogAutoRefresh"
        :logs="appLogLines"
        :logs-loading="appLogLoading"
        :manual-refresh-disabled="false"
        :show-status-tag="false"
        :show-summary="false"
        :reverse-logs="false"
        :stick-to-bottom="true"
        empty-text="暂无应用日志"
        @update:autoRefresh="appLogAutoRefresh = $event"
        @refresh="manualRefreshAppLogs"
      >
        <template #toolbar-left-extra>
          <el-select
            v-model="appLogFileName"
            size="small"
            style="width: 150px"
            @change="onAppLogFileChange"
          >
            <el-option
              v-for="option in appLogFiles"
              :key="option.name"
              :label="buildAppLogFileLabel(option)"
              :value="option.name"
            />
          </el-select>
        </template>
      </TaskLogMonitorDrawer>
    </template>
  </el-config-provider>
</template>

<script setup>
import { computed, defineAsyncComponent, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import dayjs from 'dayjs'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'
import { Document, Files, FolderOpened, Monitor, Search, Setting, SwitchButton, VideoPlay, WarningFilled } from '@element-plus/icons-vue'
import { logsApi } from './api/client'

const TaskLogMonitorDrawer = defineAsyncComponent(() => import('./components/TaskLogMonitorDrawer.vue'))

const route = useRoute()
const router = useRouter()
const isLoginPage = computed(() => route.path === '/login')

const appLogDrawerVisible = ref(false)
const appLogLoading = ref(false)
const appLogLines = ref([])
const appLogFileName = ref('app.log')
const appLogFiles = ref(buildDefaultAppLogFiles())
const appLogTargetLabel = ref('logs/app.log')
const appLogLastRefreshedAt = ref('')
const appLogAutoRefresh = ref(true)
let appLogTimer = null

function buildDefaultAppLogFiles() {
  return ['app.log', 'app.log.1', 'app.log.2', 'app.log.3', 'app.log.4', 'app.log.5'].map((name, index) => ({
    name,
    exists: index === 0,
  }))
}

function buildAppLogFileLabel(option) {
  const name = String(option?.name || 'app.log')
  return option?.exists ? name : `${name}（暂无）`
}

function logout() {
  clearAppLogTimer()
  appLogDrawerVisible.value = false
  localStorage.removeItem('amm_token')
  router.push('/login')
}

function clearAppLogTimer() {
  if (appLogTimer) {
    clearInterval(appLogTimer)
    appLogTimer = null
  }
}

async function fetchAppLogs({ silent = false } = {}) {
  if (!silent) appLogLoading.value = true
  try {
    const { data } = await logsApi.getAppLogs({ limit: 400, file_name: appLogFileName.value })
    appLogLines.value = Array.isArray(data?.logs) ? [...data.logs] : []
    appLogFiles.value = Array.isArray(data?.files) && data.files.length ? data.files : buildDefaultAppLogFiles()
    appLogFileName.value = data?.name || appLogFileName.value
    appLogTargetLabel.value = data?.name ? `logs/${data.name}` : 'logs/app.log'
    appLogLastRefreshedAt.value = dayjs().format('HH:mm:ss')
  } catch {
    if (!silent) {
      appLogLines.value = ['加载应用日志失败']
    }
  } finally {
    if (!silent) appLogLoading.value = false
  }
}

function restartAppLogAutoRefresh() {
  clearAppLogTimer()
  if (isLoginPage.value || !appLogDrawerVisible.value || !appLogAutoRefresh.value) return
  appLogTimer = setInterval(async () => {
    await fetchAppLogs({ silent: true })
  }, 2000)
}

async function openAppLogDrawer() {
  appLogDrawerVisible.value = true
  await fetchAppLogs()
  restartAppLogAutoRefresh()
}

async function onAppLogFileChange() {
  await fetchAppLogs()
  restartAppLogAutoRefresh()
}

async function manualRefreshAppLogs() {
  await fetchAppLogs()
}

watch([appLogDrawerVisible, appLogAutoRefresh, isLoginPage], () => {
  if (isLoginPage.value) {
    appLogDrawerVisible.value = false
    clearAppLogTimer()
    return
  }
  restartAppLogAutoRefresh()
})

onBeforeUnmount(() => {
  clearAppLogTimer()
})
</script>

<style>
:root {
  --app-bg: #f5f7fb;
  --el-input-border-radius: 10px;
  --page-backdrop: linear-gradient(180deg, #f5f7fb 0%, #eef3fb 100%);
  --page-overlay: radial-gradient(circle at top right, rgba(37, 99, 235, 0.08), transparent 28%);
  --panel-bg: #ffffff;
  --panel-raised-bg: #ffffff;
  --panel-header-bg: transparent;
  --text-main: #111827;
  --text-secondary: #6b7280;
  --line-soft: #e5e7eb;
  --brand: #2563eb;
  --sidebar-bg: #ffffff;
  --sidebar-item: #4b5563;
  --sidebar-active-bg: #eef4ff;
  --table-header-bg: #fafafa;
  --table-row-hover-bg: #f9fafb;
  --table-cell-line: #f1f5f9;
  --panel-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 8px 16px rgba(15, 23, 42, 0.04);
  --scrollbar-thumb: rgba(100, 116, 139, 0.18);
  --scrollbar-thumb-hover: rgba(100, 116, 139, 0.28);
  --stat-card-bg: linear-gradient(160deg, rgba(37, 99, 235, 0.08), rgba(37, 99, 235, 0.02));
  --stat-card-border: rgba(37, 99, 235, 0.12);
  --radius: 12px;
  --space: 24px;
}

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

html, body, #app {
  height: 100%;
}

body {
  font-family: "IBM Plex Sans", "Noto Sans SC", "PingFang SC", "Microsoft YaHei", sans-serif;
  color: var(--text-main);
  background: var(--app-bg);
  background-image: var(--page-overlay);
  background-attachment: fixed;
}

.page-shell :is(
  .actions,
  .header-actions,
  .page-actions,
  .filter-toolbar,
  .card-footer,
  .card-header-flex,
  .drawer-toolbar,
  .drawer-toolbar-left,
  .drawer-toolbar-right,
  .node-header-actions,
  .group-actions,
  .row-actions,
  .pending-files-actions,
  .pending-filter-group,
  .pending-select-group,
  .kind-switch,
  .danger-zone
) {
  flex-wrap: wrap;
}

.page-shell :is(
  .actions,
  .header-actions,
  .page-actions,
  .filter-toolbar,
  .card-footer,
  .card-header-flex,
  .drawer-toolbar,
  .drawer-toolbar-left,
  .drawer-toolbar-right,
  .node-header-actions,
  .group-actions,
  .row-actions,
  .pending-files-actions,
  .pending-filter-group,
  .pending-select-group,
  .kind-switch,
  .danger-zone
) > * {
  min-width: 0;
}

.page-shell :is(
  .actions,
  .header-actions,
  .page-actions,
  .filter-toolbar,
  .card-footer,
  .card-header-flex,
  .drawer-toolbar,
  .drawer-toolbar-left,
  .drawer-toolbar-right,
  .node-header-actions,
  .group-actions,
  .row-actions,
  .pending-files-actions,
  .pending-filter-group,
  .pending-select-group,
  .kind-switch,
  .danger-zone
) :is(.el-button, .el-dropdown) {
  max-width: 100%;
  flex-shrink: 1;
}

.page-shell :is(.header-actions, .page-actions, .filter-toolbar) .toolbar-search {
  flex: 1 1 320px;
  min-width: 220px;
  max-width: 420px;
}

.page-shell :is(.header-actions, .page-actions, .filter-toolbar) .toolbar-search--wide {
  max-width: 520px;
}

.page-shell .toolbar-actions-inline {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.page-shell .table-actions {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  flex-wrap: wrap;
  width: 100%;
}

.page-shell .table-actions .el-button + .el-button,
.page-shell .table-actions .el-dropdown + .el-button,
.page-shell .table-actions .el-button + .el-dropdown,
.page-shell .table-actions .el-dropdown + .el-dropdown {
  margin-left: 0;
}

.page-shell .table-actions .el-button-group {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.page-shell .table-actions .el-button-group > .el-button {
  margin-left: 0 !important;
  border-radius: 10px !important;
}

.app-container {
  height: 100%;
  background: var(--page-backdrop);
}

.sidebar {
  background: var(--sidebar-bg);
  border-right: 1px solid var(--line-soft);
  display: flex;
  flex-direction: column;
}

.logo {
  padding: var(--space);
  font-size: 18px;
  font-weight: 600;
  color: var(--text-main);
  border-bottom: 1px solid var(--line-soft);
}

.sidebar-menu {
  border-right: 0 !important;
  padding: 12px 10px;
}

.sidebar-menu .el-menu-item {
  height: 42px;
  line-height: 42px;
  margin: 4px 8px;
  border-radius: 10px;
  color: var(--sidebar-item);
  position: relative;
  overflow: hidden;
  transition: background-color 0.2s ease, color 0.2s ease, transform 0.2s ease;
}

.sidebar-menu .el-menu-item:hover {
  transform: translateX(1px);
}

.sidebar-menu .el-menu-item .el-icon {
  width: 18px;
  min-width: 18px;
  font-size: 18px;
  margin-right: 10px;
}

.sidebar-menu .el-menu-item .el-icon svg {
  stroke-width: 1.7;
}

.sidebar-menu .el-menu-item::before {
  content: "";
  position: absolute;
  left: 0;
  top: 8px;
  bottom: 8px;
  width: 3px;
  border-radius: 999px;
  background: transparent;
  transition: background-color 0.2s ease;
}

.sidebar-menu .el-menu-item.is-active {
  color: var(--brand);
  background: var(--sidebar-active-bg);
  font-weight: 600;
}

.sidebar-menu .el-menu-item.is-active::before {
  background: var(--brand);
}

.sidebar-footer {
  margin-top: auto;
  padding: 12px 16px 16px;
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 10px;
}

.sidebar-tool-btn,
.logout-btn {
  width: 100%;
  min-width: 0;
  justify-content: flex-start;
  text-align: left;
}

.sidebar-footer-btn__content {
  display: grid;
  grid-template-columns: 18px minmax(0, 1fr);
  align-items: center;
  width: 100%;
  column-gap: 10px;
}

.sidebar-footer-btn__icon {
  width: 18px;
  min-width: 18px;
  margin: 0;
  display: flex;
  align-items: center;
  justify-content: flex-start;
}

.sidebar-footer-btn__icon svg {
  width: 18px;
  height: 18px;
  display: block;
}

.sidebar-footer-btn__label {
  min-width: 0;
  justify-self: start;
}

.main-content {
  background: transparent;
  color: var(--text-main);
  padding: var(--space);
  overflow-y: auto;
}

.page-shell {
  width: 100%;
  max-width: 1320px;
  margin: 0 auto;
}

.page-title {
  font-size: 26px;
  line-height: 1.2;
  margin: 0;
  color: var(--text-main);
  letter-spacing: 0.2px;
}

.el-card {
  background: var(--panel-bg) !important;
  color: var(--text-main) !important;
  border: 1px solid transparent !important;
  border-radius: var(--radius) !important;
  box-shadow: var(--panel-shadow) !important;
}

.el-card__header {
  background: var(--panel-header-bg);
  border-bottom: 1px solid var(--line-soft);
}

.el-table {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-border-color: var(--line-soft);
  --el-table-header-bg-color: var(--table-header-bg);
  --el-table-row-hover-bg-color: var(--table-row-hover-bg);
}

.el-table th.el-table__cell {
  background: var(--table-header-bg);
  border-bottom: 1px solid var(--line-soft);
  color: var(--text-secondary);
  font-weight: 600;
}

.el-table td.el-table__cell {
  border-bottom: 1px solid var(--table-cell-line);
}

.el-table :is(.el-table-fixed-column--right, .el-table-fixed-column--left) {
  background: var(--panel-bg) !important;
}

.el-table .el-table__fixed-right-patch {
  background: var(--table-header-bg) !important;
}

.el-divider__text {
  color: var(--text-secondary);
  font-weight: 500;
}

.el-menu {
  --el-menu-bg-color: var(--sidebar-bg);
  --el-menu-text-color: var(--sidebar-item);
  --el-menu-active-color: var(--brand);
}

.el-input__wrapper,
.el-select__wrapper,
.el-button {
  border-radius: 10px;
}

.el-input-group__append,
.el-input-group__prepend {
  overflow: hidden;
}

.el-button {
  transition: transform 0.18s ease, box-shadow 0.18s ease, background-color 0.18s ease, border-color 0.18s ease, color 0.18s ease;
}

*::-webkit-scrollbar {
  width: 10px;
  height: 10px;
}

*::-webkit-scrollbar-track {
  background: transparent;
}

*::-webkit-scrollbar-thumb {
  background: var(--scrollbar-thumb);
  border-radius: 999px;
}

*::-webkit-scrollbar-thumb:hover {
  background: var(--scrollbar-thumb-hover);
}

.amm-confirm-box {
  width: min(520px, calc(100vw - 32px)) !important;
  max-width: calc(100vw - 32px);
  direction: ltr;
}

.amm-confirm-box .el-message-box__header {
  padding-bottom: 8px;
}

.amm-confirm-box .el-message-box__container {
  align-items: flex-start;
}

.amm-confirm-box .el-message-box__content {
  padding-top: 4px;
}

.amm-confirm-box .el-message-box__message {
  overflow-wrap: anywhere;
  word-break: break-word;
}

.amm-confirm-box .el-message-box__status {
  position: relative;
  top: 2px;
  flex: 0 0 auto;
}

.amm-confirm-box .el-message-box__btns {
  gap: 12px;
}

@media (max-width: 900px) {
  .page-shell :is(.header-actions, .page-actions, .filter-toolbar) .toolbar-search,
  .page-shell :is(.header-actions, .page-actions, .filter-toolbar) .toolbar-search--wide {
    flex-basis: 100%;
    min-width: 100%;
    max-width: none;
  }
}
</style>
