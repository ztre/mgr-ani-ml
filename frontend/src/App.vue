<template>
  <el-config-provider :locale="zhCn">
    <router-view v-if="isLoginPage" />
    <el-container v-else class="app-container">
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
          <el-menu-item index="/pending">
            <el-icon><WarningFilled /></el-icon>
            <span>待办清单</span>
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
          <el-button class="logout-btn" @click="logout">
            <el-icon><SwitchButton /></el-icon>
            退出登录
          </el-button>
        </div>
      </el-aside>
      <el-main class="main-content">
        <div class="page-shell">
          <router-view />
        </div>
      </el-main>
    </el-container>
  </el-config-provider>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import zhCn from 'element-plus/dist/locale/zh-cn.mjs'

const route = useRoute()
const router = useRouter()
const isLoginPage = computed(() => route.path === '/login')

function logout() {
  localStorage.removeItem('amm_token')
  router.push('/login')
}
</script>

<style>
:root {
  --app-bg: #f5f7fb;
  --panel-bg: #ffffff;
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
  --radius: 12px;
  --space: 24px;
}

body.theme-dark {
  /* Background tiers (GitHub Dark inspired) */
  --bg-base: #0d1117;
  --bg-surface-1: #161b22;
  --bg-surface-2: #1c2128;
  --bg-surface-active: #262c36;

  /* Text tiers */
  --text-primary: #e6edf3;
  --text-regular: #c9d1d9;
  --text-secondary: #8b949e;
  --text-disabled: #6e7681;

  /* Accent only for interactive states */
  --accent: #2f81f7;
  --accent-hover: #388bfd;
  --accent-active: #1f6feb;

  --border-default: #30363d;
  --border-muted: #21262d;
  --shadow-dark: 0 0 0 1px rgba(48, 54, 61, 0.25), 0 10px 24px rgba(1, 4, 9, 0.35);

  /* Legacy mapping to existing app vars */
  --app-bg: var(--bg-base);
  --panel-bg: var(--bg-surface-1);
  --text-main: var(--text-primary);
  --line-soft: var(--border-default);
  --brand: var(--accent);
  --sidebar-bg: var(--bg-base);
  --sidebar-item: var(--text-regular);
  --sidebar-active-bg: rgba(47, 129, 247, 0.14);
  --table-header-bg: var(--bg-surface-1);
  --table-row-hover-bg: var(--bg-surface-2);
  --table-cell-line: var(--border-muted);

  /* Element Plus dark semantic vars */
  --el-bg-color: var(--bg-surface-1);
  --el-bg-color-page: var(--bg-base);
  --el-bg-color-overlay: var(--bg-surface-2);
  --el-fill-color-blank: var(--bg-surface-1);
  --el-fill-color-light: var(--bg-surface-2);
  --el-fill-color-lighter: var(--bg-surface-active);
  --el-fill-color-dark: var(--bg-surface-active);
  --el-text-color-primary: var(--text-primary);
  --el-text-color-regular: var(--text-regular);
  --el-text-color-secondary: var(--text-secondary);
  --el-text-color-placeholder: var(--text-secondary);
  --el-text-color-disabled: var(--text-disabled);
  --el-border-color: var(--border-default);
  --el-border-color-light: var(--border-muted);
  --el-border-color-lighter: var(--border-muted);
  --el-border-color-extra-light: var(--border-muted);
  --el-mask-color: rgba(1, 4, 9, 0.7);
  --el-color-primary: var(--accent);
  --el-color-primary-light-3: #4d9bff;
  --el-color-primary-light-5: #69abff;
  --el-color-primary-light-7: #91c2ff;
  --el-color-primary-light-8: #a8d0ff;
  --el-color-primary-light-9: #c0ddff;
  --el-color-primary-dark-2: var(--accent-active);
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
}

.app-container {
  height: 100%;
  background: var(--app-bg);
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
}

.sidebar-menu .el-menu-item.is-active {
  color: var(--brand);
  background: var(--sidebar-active-bg);
}

.sidebar-footer {
  margin-top: auto;
  padding: 12px 16px 16px;
}

.logout-btn {
  width: 100%;
}

.main-content {
  background: var(--app-bg);
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
  border: 1px solid var(--line-soft) !important;
  border-radius: var(--radius) !important;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 8px 16px rgba(15, 23, 42, 0.04) !important;
}

.el-card__header {
  border-bottom: 1px solid var(--line-soft);
}

.el-table {
  --el-table-border-color: var(--line-soft);
  --el-table-header-bg-color: var(--table-header-bg);
  --el-table-row-hover-bg-color: var(--table-row-hover-bg);
}

.el-table th.el-table__cell {
  border-bottom: 1px solid var(--line-soft);
  color: var(--text-secondary);
  font-weight: 600;
}

.el-table td.el-table__cell {
  border-bottom: 1px solid var(--table-cell-line);
}

.el-divider__text {
  color: var(--text-secondary);
  font-weight: 500;
}

body.theme-dark .el-menu {
  --el-menu-bg-color: var(--sidebar-bg);
  --el-menu-text-color: var(--sidebar-item);
  --el-menu-active-color: var(--brand);
}

body.theme-dark .el-input__wrapper,
body.theme-dark .el-select__wrapper {
  background: var(--bg-surface-2);
  box-shadow: 0 0 0 1px var(--border-default) inset;
  border-radius: 10px;
}

body.theme-dark .el-input__inner,
body.theme-dark .el-select__placeholder,
body.theme-dark .el-select__selected-item {
  color: var(--text-main);
}

body.theme-dark .el-button {
  border-radius: 10px;
  box-shadow: none !important;
}

body.theme-dark .el-button:not(.el-button--primary):not(.el-button--success):not(.el-button--warning):not(.el-button--danger) {
  background: var(--bg-surface-2);
  color: var(--text-regular);
  border-color: var(--border-default);
}

body.theme-dark .el-button.el-button--primary {
  background: var(--accent);
  border-color: var(--accent-active);
  color: #ffffff;
}

body.theme-dark .el-button.el-button--primary:hover {
  background: var(--accent-hover);
  border-color: var(--accent);
}

body.theme-dark .el-tag {
  border-color: var(--border-default);
  box-shadow: none !important;
}

body.theme-dark .el-table,
body.theme-dark .el-table__inner-wrapper,
body.theme-dark .el-table tr,
body.theme-dark .el-table th.el-table__cell,
body.theme-dark .el-table td.el-table__cell {
  background: transparent !important;
  color: var(--text-main);
}

body.theme-dark .el-card {
  box-shadow: var(--shadow-dark) !important;
}

body.theme-dark .el-card__header,
body.theme-dark .el-divider__text {
  color: var(--text-regular) !important;
}

body.theme-dark .sidebar {
  border-right-color: var(--border-default);
}

body.theme-dark .sidebar-menu .el-menu-item:hover {
  background: var(--bg-surface-2);
}

body.theme-dark .sidebar-menu .el-menu-item.is-active {
  color: var(--accent);
  background: var(--sidebar-active-bg);
}

body.theme-dark .el-table {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
}

body.theme-dark .el-table th.el-table__cell {
  color: var(--text-regular);
}

body.theme-dark .el-table td.el-table__cell,
body.theme-dark .el-table th.el-table__cell {
  border-bottom-color: var(--border-muted);
}

body.theme-dark .el-pagination,
body.theme-dark .el-form-item__label {
  color: var(--text-secondary);
}

body.theme-dark .el-drawer {
  background: var(--bg-surface-1);
}

body.theme-dark .el-dialog {
  background: var(--bg-surface-1);
  border: 1px solid var(--border-default);
}

body.theme-dark .el-overlay-dialog,
body.theme-dark .el-overlay {
  backdrop-filter: none;
}
</style>
