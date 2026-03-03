<template>
  <div class="sync-groups">
    <div class="header">
      <h1 class="page-title">同步组管理</h1>
      <el-button type="primary" @click="openDialog()">
        <el-icon><Plus /></el-icon>
        新建同步组
      </el-button>
    </div>

    <el-card shadow="never" class="table-card">
      <el-table :data="groups" stripe>
        <el-table-column prop="name" label="名称" width="150" />
        <el-table-column prop="source" label="源路径" />
        <el-table-column prop="target" label="目标路径" />
        <el-table-column prop="source_type" label="类型" width="80">
          <template #default="{ row }">
            <el-tag :type="row.source_type === 'tv' ? 'primary' : 'success'">
              {{ row.source_type === 'tv' ? 'TV' : '电影' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="include" label="Include" width="120" show-overflow-tooltip />
        <el-table-column prop="exclude" label="Exclude" width="120" show-overflow-tooltip />
        <el-table-column prop="enabled" label="启用" width="80">
          <template #default="{ row }">
            <el-switch v-model="row.enabled" @change="updateGroup(row)" />
          </template>
        </el-table-column>
        <el-table-column label="操作" width="180" fixed="right">
          <template #default="{ row }">
            <el-button size="small" @click="openDialog(row)">编辑</el-button>
            <el-button size="small" type="danger" @click="deleteGroup(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog
      v-model="dialogVisible"
      :title="editing ? '编辑同步组' : '新建同步组'"
      width="560px"
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
          <el-input v-model="form.include" placeholder="*.mkv,*.mp4 留空表示全部" />
        </el-form-item>
        <el-form-item label="Exclude">
          <el-input v-model="form.exclude" placeholder="*.nfo,*.srt 留空表示无" />
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="form.enabled" />
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
import { syncGroupsApi } from '../api/client'

const groups = ref([])
const dialogVisible = ref(false)
const editing = ref(false)
const saving = ref(false)
const form = ref({
  name: '',
  source: '',
  source_type: 'tv',
  target: '',
  include: '',
  exclude: '',
  enabled: true,
})

async function loadGroups() {
  const { data } = await syncGroupsApi.list()
  groups.value = data
}

function openDialog(row) {
  editing.value = !!row
  form.value = row
    ? {
        ...row,
        include: row.include || '',
        exclude: row.exclude || '',
      }
    : {
        name: '',
        source: '',
        source_type: 'tv',
        target: '',
        include: '',
        exclude: '',
        enabled: true,
      }
  dialogVisible.value = true
}

async function save() {
  saving.value = true
  try {
    if (editing.value) {
      await syncGroupsApi.update(form.value.id, form.value)
      ElMessage.success('更新成功')
    } else {
      await syncGroupsApi.create(form.value)
      ElMessage.success('创建成功')
    }
    dialogVisible.value = false
    loadGroups()
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '保存失败')
  } finally {
    saving.value = false
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
  await ElMessageBox.confirm(`确定删除同步组「${row.name}」？`, '确认')
  try {
    await syncGroupsApi.delete(row.id)
    ElMessage.success('已删除')
    loadGroups()
  } catch (e) {
    ElMessage.error('删除失败')
  }
}

onMounted(loadGroups)
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

.table-card {
  border: none;
  background: transparent;
}
</style>
