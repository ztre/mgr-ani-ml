<template>
  <div class="sync-groups">
    <div class="header">
      <h1 class="page-title">同步组管理</h1>
      <el-button type="primary" @click="openDialog()">
        <el-icon><Plus /></el-icon>
        新建同步组
      </el-button>
    </div>

    <el-card shadow="never" class="settings-card">
      <template #header>
        <div class="section-head">
          <div>
            <div class="section-title">同步设置</div>
            <div class="section-desc">字幕备份根目录已迁移到同步组管理页面配置。</div>
          </div>
        </div>
      </template>

      <div class="settings-layout">
        <div class="settings-copy">
          <div class="settings-label">字幕备份根目录</div>
          <div class="settings-help">手动导入字幕的备份存储目录，需与资源目标目录在同一文件系统。</div>
        </div>
        <div class="settings-controls">
          <el-input v-model="subtitleBackupRoot" placeholder="/app/subtitle_backup" />
          <el-button type="primary" :loading="savingSettings" @click="saveSettings">保存目录</el-button>
        </div>
      </div>
    </el-card>

    <div v-if="groups.length" class="groups-grid">
      <el-card v-for="row in groups" :key="row.id" shadow="hover" class="group-card">
        <div class="group-card-head">
          <div class="group-card-title-block">
            <div class="group-card-title-line">
              <h2 class="group-card-title">{{ row.name }}</h2>
              <el-tag size="small" :type="row.source_type === 'tv' ? 'primary' : 'success'">
                {{ row.source_type === 'tv' ? 'TV' : '电影' }}
              </el-tag>
              <el-tag size="small" effect="plain" :type="row.enabled ? 'success' : 'info'">
                {{ row.enabled ? '已启用' : '已停用' }}
              </el-tag>
            </div>
            <div class="group-card-meta">同步组 ID {{ row.id }}</div>
          </div>
          <el-switch v-model="row.enabled" @change="updateGroup(row)" />
        </div>

        <div class="detail-grid">
          <div class="detail-block">
            <div class="detail-label">源路径</div>
            <div class="path-value" :title="row.source">{{ row.source }}</div>
          </div>
          <div class="detail-block">
            <div class="detail-label">目标路径</div>
            <div class="path-value" :title="row.target">{{ row.target }}</div>
          </div>
        </div>

        <div class="detail-grid detail-grid--rules">
          <div class="detail-block detail-block--soft">
            <div class="detail-label">Include</div>
            <div class="rule-value">{{ formatRuleText(row.include, '未设置，默认包含全部文件') }}</div>
          </div>
          <div class="detail-block detail-block--soft">
            <div class="detail-label">Exclude</div>
            <div class="rule-value">{{ formatRuleText(row.exclude, '未设置，默认不排除') }}</div>
          </div>
        </div>

        <div class="detail-block detail-block--soft">
          <div class="detail-label">检查器</div>
          <div class="checks-list">
            <el-tag v-for="item in resolveEnabledChecks(row)" :key="item.key" size="small" effect="plain">
              {{ item.label }}
            </el-tag>
          </div>
        </div>

        <div class="group-actions">
          <el-button type="primary" plain @click="openDialog(row)">编辑</el-button>
          <el-button type="info" plain @click="runGroupCheck(row)">检查</el-button>
          <el-button type="danger" plain @click="deleteGroup(row)">删除</el-button>
        </div>
      </el-card>
    </div>

    <el-card v-else shadow="never" class="empty-card">
      <el-empty description="暂无同步组，先创建一个同步组" />
    </el-card>

    <el-dialog
      v-model="dialogVisible"
      :title="editing ? '编辑同步组' : '新建同步组'"
      width="680px"
      destroy-on-close
    >
      <el-form :model="form" label-width="100px">
        <el-form-item label="名称" required>
          <el-input v-model="form.name" placeholder="同步组名称" />
        </el-form-item>
        <el-form-item label="源路径" required>
          <el-input v-model="form.source" placeholder="/media/source_tv" />
        </el-form-item>
        <el-form-item label="类型" required>
          <el-select v-model="form.source_type" placeholder="选择类型">
            <el-option label="TV 番剧" value="tv" />
            <el-option label="电影" value="movie" />
          </el-select>
        </el-form-item>
        <el-form-item label="目标路径" required>
          <el-input v-model="form.target" placeholder="/media/target_tv" />
        </el-form-item>
        <el-form-item label="Include">
          <el-input
            v-model="form.include"
            type="textarea"
            :autosize="{ minRows: 2, maxRows: 5 }"
            placeholder="*.mkv,*.mp4 留空表示全部"
          />
        </el-form-item>
        <el-form-item label="Exclude">
          <el-input
            v-model="form.exclude"
            type="textarea"
            :autosize="{ minRows: 2, maxRows: 5 }"
            placeholder="*.nfo,*.srt 留空表示无"
          />
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="form.enabled" />
        </el-form-item>
        <el-form-item label="检查器">
          <el-checkbox-group v-model="form.enabled_checks_arr">
            <el-checkbox label="source_unrecorded">源文件未使用</el-checkbox>
            <el-checkbox label="links_orphans">孤立链接</el-checkbox>
            <el-checkbox label="media_path_sanity">目标路径健康</el-checkbox>
            <el-checkbox label="target_no_source">目标文件无源</el-checkbox>
          </el-checkbox-group>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus } from '@element-plus/icons-vue'
import { syncGroupsApi, checksApi } from '../api/client'
import { buildConfirmDialogOptions, buildConfirmMessage } from '../utils/confirmMessage'

const CHECK_LABELS = {
  source_unrecorded: '源文件未使用',
  links_orphans: '孤立链接',
  media_path_sanity: '目标路径健康',
  target_no_source: '目标文件无源',
}

const groups = ref([])
const subtitleBackupRoot = ref('/app/subtitle_backup')
const dialogVisible = ref(false)
const editing = ref(false)
const saving = ref(false)
const savingSettings = ref(false)
const form = ref({
  name: '',
  source: '',
  source_type: 'tv',
  target: '',
  include: '',
  exclude: '',
  enabled: true,
  enabled_checks_arr: ['source_unrecorded', 'links_orphans', 'media_path_sanity', 'target_no_source'],
})

const ALL_CHECKS = ['source_unrecorded', 'links_orphans', 'media_path_sanity', 'target_no_source']

function checksArrToList(arr) {
  if (!arr || arr.length === ALL_CHECKS.length) return null
  return arr
}

function checksToArr(enabled_checks) {
  if (!enabled_checks) return [...ALL_CHECKS]
  try {
    const parsed = typeof enabled_checks === 'string' ? JSON.parse(enabled_checks) : enabled_checks
    return Array.isArray(parsed) ? parsed : [...ALL_CHECKS]
  } catch {
    return [...ALL_CHECKS]
  }
}

function formatRuleText(value, fallback) {
  const text = String(value || '').trim()
  return text || fallback
}

function resolveEnabledChecks(row) {
  return checksToArr(row?.enabled_checks).map((key) => ({
    key,
    label: CHECK_LABELS[key] || key,
  }))
}

async function loadGroups() {
  const { data } = await syncGroupsApi.list()
  groups.value = data
}

async function loadSettings() {
  try {
    const { data } = await syncGroupsApi.getSettings()
    subtitleBackupRoot.value = data?.subtitle_backup_root || '/app/subtitle_backup'
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '加载同步设置失败')
  }
}

async function saveSettings() {
  const root = String(subtitleBackupRoot.value || '').trim()
  if (!root) {
    ElMessage.warning('请填写字幕备份根目录')
    return
  }
  savingSettings.value = true
  try {
    const { data } = await syncGroupsApi.updateSettings({ subtitle_backup_root: root })
    subtitleBackupRoot.value = data?.subtitle_backup_root || root
    ElMessage.success('字幕备份根目录已保存')
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '保存失败')
  } finally {
    savingSettings.value = false
  }
}

function openDialog(row) {
  editing.value = !!row
  form.value = row
    ? {
        ...row,
        include: row.include || '',
        exclude: row.exclude || '',
        enabled_checks_arr: checksToArr(row.enabled_checks),
      }
    : {
        name: '',
        source: '',
        source_type: 'tv',
        target: '',
        include: '',
        exclude: '',
        enabled: true,
        enabled_checks_arr: [...ALL_CHECKS],
      }
  dialogVisible.value = true
}

async function save() {
  saving.value = true
  try {
    const payload = {
      ...form.value,
      name: String(form.value.name || '').trim(),
      source: String(form.value.source || '').trim(),
      target: String(form.value.target || '').trim(),
      include: String(form.value.include || '').trim(),
      exclude: String(form.value.exclude || '').trim(),
      enabled_checks: checksArrToList(form.value.enabled_checks_arr),
    }
    delete payload.enabled_checks_arr
    if (editing.value) {
      await syncGroupsApi.update(form.value.id, payload)
      ElMessage.success('更新成功')
    } else {
      await syncGroupsApi.create(payload)
      ElMessage.success('创建成功')
    }
    dialogVisible.value = false
    await loadGroups()
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '保存失败')
  } finally {
    saving.value = false
  }
}

async function runGroupCheck(row) {
  try {
    await checksApi.runGroup(row.id)
    ElMessage.success(`检查任务「${row.name}」已入队`)
  } catch {
    ElMessage.error('启动失败')
  }
}

async function updateGroup(row) {
  try {
    await syncGroupsApi.update(row.id, { enabled: row.enabled })
    ElMessage.success('已更新')
  } catch (e) {
    row.enabled = !row.enabled
    ElMessage.error('更新失败')
  }
}

async function deleteGroup(row) {
  await ElMessageBox.confirm(
    buildConfirmMessage([`确定删除同步组「${row.name}」？`]),
    '确认',
    buildConfirmDialogOptions(),
  )
  try {
    await syncGroupsApi.delete(row.id)
    ElMessage.success('已删除')
    await loadGroups()
  } catch (e) {
    ElMessage.error('删除失败')
  }
}

onMounted(async () => {
  await Promise.all([loadGroups(), loadSettings()])
})
</script>

<style scoped>
.sync-groups {
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

.settings-card,
.empty-card {
  border: none;
  background: transparent;
}

.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.section-title {
  font-size: 16px;
  font-weight: 600;
  color: #0f172a;
}

.section-desc,
.settings-help,
.group-card-meta,
.detail-label {
  color: #64748b;
  font-size: 13px;
  line-height: 1.5;
}

.settings-layout {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  flex-wrap: wrap;
}

.settings-copy {
  flex: 1 1 280px;
}

.settings-label {
  font-size: 15px;
  font-weight: 600;
  color: #1e293b;
  margin-bottom: 4px;
}

.settings-controls {
  flex: 1 1 420px;
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.settings-controls :deep(.el-input) {
  flex: 1;
  min-width: 240px;
}

.groups-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 18px;
}

.group-card {
  border: 1px solid rgba(148, 163, 184, 0.18);
}

.group-card-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;
}

.group-card-title-block {
  min-width: 0;
}

.group-card-title-line {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 4px;
}

.group-card-title {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: #0f172a;
}

.detail-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
  margin-bottom: 12px;
}

.detail-grid--rules {
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}

.detail-block {
  padding: 14px;
  border-radius: 14px;
  background: #ffffff;
  border: 1px solid rgba(148, 163, 184, 0.14);
}

.detail-block--soft {
  background: #f8fafc;
}

.detail-label {
  margin-bottom: 6px;
}

.path-value,
.rule-value {
  color: #0f172a;
  line-height: 1.6;
  word-break: break-all;
  white-space: pre-wrap;
}

.checks-list {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.group-actions {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  margin-top: 16px;
}

@media (max-width: 768px) {
  .settings-controls {
    flex-direction: column;
    align-items: stretch;
  }

  .settings-controls :deep(.el-input) {
    width: 100%;
  }

  .group-card-head {
    flex-direction: column;
    align-items: stretch;
  }

  .group-actions > * {
    flex: 1 1 100%;
  }
}
</style>