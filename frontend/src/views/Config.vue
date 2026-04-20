<template>
  <div class="config">
    <div class="page-head">
      <h1 class="page-title">系统配置</h1>
      <p class="page-subtitle">统一管理识别、刮削、Emby 刷新与系统行为开关。</p>
    </div>

    <el-card class="config-card">
      <div class="section-intro">
        <div class="section-title">连接配置</div>
        <div class="section-desc">建议先配置 TMDB，再测试 Emby 连通性。</div>
      </div>

      <el-form :model="form" label-width="180px" style="max-width: 860px">
        <el-form-item label="Bangumi API Key">
          <div style="display: flex; gap: 10px; width: 100%">
            <el-input v-model="form.bangumi_api_key" type="password" show-password placeholder="可选" />
            <el-button @click="testConnection('bangumi')" :loading="testing.bangumi">测试</el-button>
          </div>
        </el-form-item>
        <el-form-item label="TMDB API Key">
          <div style="display: flex; gap: 10px; width: 100%">
            <el-input v-model="form.tmdb_api_key" type="password" show-password placeholder="必填，用于识别与刮削" />
            <el-button @click="testConnection('tmdb')" :loading="testing.tmdb">测试</el-button>
          </div>
        </el-form-item>
        <el-form-item label="Emby 地址">
          <el-input v-model="form.emby_url" placeholder="http://emby:8096" />
        </el-form-item>
        <el-form-item label="Emby API Key">
          <div style="display: flex; gap: 10px; width: 100%">
            <el-input v-model="form.emby_api_key" type="password" show-password placeholder="用于刷新媒体库" />
            <el-button @click="testConnection('emby')" :loading="testing.emby">测试</el-button>
          </div>
        </el-form-item>
        <el-form-item label="Emby 目标媒体库">
          <div style="display: flex; gap: 10px; width: 100%">
            <el-select
              v-model="form.emby_library_ids"
              multiple
              clearable
              filterable
              collapse-tags
              collapse-tags-tooltip
              placeholder="留空=刷新全部媒体库"
              style="flex: 1"
            >
              <el-option
                v-for="opt in embyLibraryOptions"
                :key="opt.value"
                :label="opt.collection_type ? `${opt.label} (${opt.collection_type})` : opt.label"
                :value="opt.value"
              />
            </el-select>
            <el-button @click="loadEmbyLibraries()" :loading="loadingLibraries">加载媒体库</el-button>
          </div>
          <div class="section-desc" style="margin-top: 6px">
            刷新流程：先按媒体库刷新元数据，再触发媒体库扫描。
          </div>
        </el-form-item>
        <el-divider content-position="left">整理策略</el-divider>
        <div class="section-desc section-gap">用于多 Movie 同步组时选择目标路径。</div>
        <el-form-item label="Movie fallback 路由">
          <el-select v-model="form.movie_fallback_strategy" style="width: 320px">
            <el-option label="auto（名称优先，其次 source 前缀）" value="auto" />
            <el-option label="unique_only（仅唯一目标时使用）" value="unique_only" />
            <el-option label="prefer_name_match（按同步组名称）" value="prefer_name_match" />
            <el-option label="prefer_source_prefix（按 source 前缀）" value="prefer_source_prefix" />
          </el-select>
        </el-form-item>
        <el-form-item label="Movie fallback 关键词">
          <el-input
            v-model="form.movie_fallback_hints"
            type="textarea"
            :rows="2"
            placeholder="逗号/分号/换行分隔；留空使用默认通用关键词"
          />
          <div class="section-desc" style="margin-top: 6px">
            示例：ordinal scale, progressive, 剧场版, the movie, 序列之争, 进击篇
          </div>
        </el-form-item>
        <el-divider content-position="left">仪表盘统计口径</el-divider>
        <div class="section-desc section-gap">按需排除特典/花絮类目录，统计更贴近“正片规模”。</div>
        <el-form-item label="忽略 Specials">
          <el-switch v-model="form.stats_ignore_specials" />
        </el-form-item>
        <el-form-item label="忽略 Extras">
          <el-switch v-model="form.stats_ignore_extras" />
        </el-form-item>
        <el-form-item label="忽略 Trailer/Featurette">
          <el-switch v-model="form.stats_ignore_trailers_featurettes" />
        </el-form-item>
        <el-divider content-position="left">日志清理</el-divider>
        <div class="section-desc section-gap">自动清理任务日志，避免长期运行占用过多磁盘空间。</div>
        <el-form-item label="日志保留天数">
          <el-input-number v-model="form.log_retention_days" :min="0" :max="3650" />
        </el-form-item>
        <el-form-item label="保留任务日志数">
          <el-input-number v-model="form.log_max_task_files" :min="10" :max="100000" />
        </el-form-item>
        <el-form-item label="清理检查间隔(秒)">
          <el-input-number v-model="form.log_cleanup_interval_seconds" :min="60" :max="86400" />
        </el-form-item>
        <div class="section-desc section-gap">字幕备份根目录已迁移到“同步组管理”页面配置。</div>

        <el-divider content-position="left">低置信度识别兜底</el-divider>
        <div class="section-desc section-gap">
          仅在 TV 或 Movie 目录识别已进入低置信度失败路径时，额外调用 AniList 获取英文名，再用英文名重试一次 TMDB。正常成功路径不受影响。
        </div>
        <el-form-item label="启用 AniList 兜底">
          <el-switch v-model="form.anilist_fallback_enabled" />
        </el-form-item>
        <el-form-item label="AniList 超时(秒)">
          <el-input-number
            v-model="form.anilist_fallback_timeout_seconds"
            :min="1"
            :max="30"
            :step="1"
          />
        </el-form-item>

        <el-form-item>
          <el-button type="primary" :loading="saving" @click="save">保存</el-button>
          <el-button @click="load">重新加载</el-button>
          <el-button type="warning" :loading="restarting" @click="restartService">
            <el-icon><RefreshRight /></el-icon>
            重启服务
          </el-button>
        </el-form-item>
      </el-form>
      <el-divider content-position="left">账号安全</el-divider>
      <el-form :model="passwordForm" label-width="180px" style="max-width: 860px">
        <el-form-item label="当前密码">
          <el-input
            v-model="passwordForm.current_password"
            type="password"
            show-password
            placeholder="请输入当前登录密码"
          />
        </el-form-item>
        <el-form-item label="新密码">
          <el-input
            v-model="passwordForm.new_password"
            type="password"
            show-password
            placeholder="至少 6 位"
          />
        </el-form-item>
        <el-form-item label="确认新密码">
          <el-input
            v-model="passwordForm.confirm_password"
            type="password"
            show-password
            placeholder="请再次输入新密码"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="changingPassword" @click="changePassword">修改密码</el-button>
        </el-form-item>
      </el-form>
      <el-divider content-position="left">系统操作</el-divider>
      <div class="danger-zone">
        <div class="danger-text">
          <div class="danger-title">高风险操作</div>
          删除媒体记录、任务日志和 Inode 缓存。源文件不会被删除。
        </div>
        <el-button type="danger" plain :loading="resetting" @click="resetSystem">
          重置系统
        </el-button>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { RefreshRight } from '@element-plus/icons-vue'
import { checksApi, configApi, embyApi, inodesApi, mediaApi, tasksApi } from '../api/client'

const form = ref({
  bangumi_api_key: '',
  tmdb_api_key: '',
  emby_url: '',
  emby_api_key: '',
  emby_library_ids: [],
  movie_fallback_strategy: 'auto',
  movie_fallback_hints: '',
  stats_ignore_specials: true,
  stats_ignore_extras: true,
  stats_ignore_trailers_featurettes: true,
  log_retention_days: 14,
  log_max_task_files: 200,
  log_cleanup_interval_seconds: 600,
  anilist_fallback_enabled: true,
  anilist_fallback_timeout_seconds: 8,
})
const saving = ref(false)
const restarting = ref(false)
const resetting = ref(false)
const testing = ref({
  bangumi: false,
  tmdb: false,
  emby: false,
})
const loadingLibraries = ref(false)
const embyLibraryOptions = ref([])
const changingPassword = ref(false)
const passwordForm = ref({
  current_password: '',
  new_password: '',
  confirm_password: '',
})

async function load() {
  const { data } = await configApi.get()
  form.value = { ...form.value, ...data, emby_library_ids: data.emby_library_ids || [] }
  if (form.value.emby_url && form.value.emby_api_key) {
    await loadEmbyLibraries(false)
  }
}

async function testConnection(type) {
  if (type === 'emby' && !form.value.emby_url) {
    ElMessage.warning('请先填写 Emby 地址')
    return
  }
  
  testing.value[type] = true
  try {
    // 构造只包含需要测试的字段的 payload
    const payload = {}
    if (type === 'bangumi') payload.bangumi_api_key = form.value.bangumi_api_key
    if (type === 'tmdb') payload.tmdb_api_key = form.value.tmdb_api_key
    if (type === 'emby') {
      payload.emby_url = form.value.emby_url
      payload.emby_api_key = form.value.emby_api_key
    }

    const { data } = await configApi.testConnection(payload)
    const result = data[type]
    
    if (result && result.ok) {
      ElMessage.success(result.message)
      if (type === 'emby') {
        await loadEmbyLibraries(true)
      }
    } else {
      ElMessage.error(result?.message || '连接失败')
    }
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '测试请求失败')
  } finally {
    testing.value[type] = false
  }
}

async function loadEmbyLibraries(showSuccess = true) {
  if (!form.value.emby_url || !form.value.emby_api_key) return
  loadingLibraries.value = true
  try {
    const { data } = await embyApi.libraries()
    embyLibraryOptions.value = (data.items || []).map((x) => ({
      label: x.name || x.id,
      value: x.id,
      collection_type: x.collection_type || '',
    }))
    if (showSuccess) {
      ElMessage.success(`已加载 ${embyLibraryOptions.value.length} 个媒体库`)
    }
  } catch (e) {
    if (showSuccess) {
      ElMessage.error(e.response?.data?.detail || '加载媒体库失败')
    }
  } finally {
    loadingLibraries.value = false
  }
}

async function save() {
  saving.value = true
  try {
    await configApi.update(form.value)
    ElMessage.success('保存成功，重启后生效')
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '保存失败')
  } finally {
    saving.value = false
  }
}

async function restartService() {
  restarting.value = true
  try {
    await configApi.restart()
    ElMessage.success('服务重启中，请稍候...')
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '重启失败')
  } finally {
    restarting.value = false
  }
}

async function changePassword() {
  const currentPassword = passwordForm.value.current_password || ''
  const newPassword = passwordForm.value.new_password || ''
  const confirmPassword = passwordForm.value.confirm_password || ''

  if (!currentPassword || !newPassword || !confirmPassword) {
    ElMessage.warning('请完整填写密码字段')
    return
  }
  if (newPassword.length < 6) {
    ElMessage.warning('新密码长度至少 6 位')
    return
  }
  if (newPassword !== confirmPassword) {
    ElMessage.warning('两次输入的新密码不一致')
    return
  }

  changingPassword.value = true
  try {
    await configApi.changePassword({
      current_password: currentPassword,
      new_password: newPassword,
    })
    passwordForm.value = { current_password: '', new_password: '', confirm_password: '' }
    ElMessage.success('密码修改成功，请使用新密码重新登录')
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '修改密码失败')
  } finally {
    changingPassword.value = false
  }
}

async function resetSystem() {
  try {
    await ElMessageBox.confirm(
      '危险操作：将删除媒体记录、扫描任务、Inode 缓存与检查中心数据。是否继续？',
      '第一次确认',
      { type: 'warning', confirmButtonText: '继续', cancelButtonText: '取消' }
    )

    const { value } = await ElMessageBox.prompt(
      '请输入 RESET 继续',
      '第二次确认',
      {
        inputPlaceholder: 'RESET',
        confirmButtonText: '确认',
        cancelButtonText: '取消',
      }
    )
    if ((value || '').trim() !== 'RESET') {
      ElMessage.warning('输入不正确，已取消重置')
      return
    }

    await ElMessageBox.confirm(
      '最后确认：重置后需要重新扫描。确定执行？',
      '第三次确认',
      { type: 'error', confirmButtonText: '确定重置', cancelButtonText: '取消' }
    )

    resetting.value = true
    await Promise.all([mediaApi.deleteAll(), inodesApi.deleteAll(), tasksApi.deleteAll(), checksApi.deleteAll()])
    ElMessage.success('系统已重置，页面即将刷新')
    setTimeout(() => window.location.reload(), 1000)
  } catch (e) {
    if (e !== 'cancel') {
      ElMessage.error(e?.response?.data?.detail || '重置失败')
    }
  } finally {
    resetting.value = false
  }
}

onMounted(load)
</script>

<style scoped>
.config {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.page-head {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.page-subtitle {
  color: #64748b;
  font-size: 14px;
  line-height: 1.5;
}

.config-card {
  border: none;
  background: transparent;
}

:deep(.el-form-item__label) {
  white-space: nowrap;
  line-height: 32px;
}

.section-intro {
  margin-bottom: 16px;
}

.section-title {
  font-size: 16px;
  font-weight: 600;
  color: #0f172a;
}

.section-desc {
  margin-top: 6px;
  color: #64748b;
  font-size: 13px;
  line-height: 1.5;
}

.form-tip {
  margin-top: 4px;
  color: #94a3b8;
  font-size: 12px;
  line-height: 1.5;
}

.readonly-path {
  min-height: 32px;
  width: 100%;
  display: flex;
  align-items: center;
  padding: 0 12px;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  background: #f8fafc;
  color: #334155;
  line-height: 1.5;
}

.section-gap {
  margin: 0 0 14px 2px;
}

.danger-zone {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 16px;
  padding: 16px;
  border: 1px solid #fecaca;
  border-radius: 10px;
  background: #fff7f7;
}

.danger-text {
  color: #7f1d1d;
  font-size: 13px;
  line-height: 1.55;
}

.danger-title {
  font-weight: 600;
  margin-bottom: 2px;
}

</style>
