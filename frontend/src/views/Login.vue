<template>
  <div class="login-page">
    <div class="login-card">
      <h1 class="title">Anime Media Manager</h1>
      <p class="subtitle">请登录后继续使用</p>
      <el-form :model="form" @submit.prevent>
        <el-form-item>
          <el-input v-model="form.username" placeholder="用户名" @keyup.enter="submitLogin" />
        </el-form-item>
        <el-form-item>
          <el-input
            v-model="form.password"
            type="password"
            show-password
            placeholder="密码"
            @keyup.enter="submitLogin"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="loading" style="width: 100%" @click="submitLogin">
            登录
          </el-button>
        </el-form-item>
      </el-form>
    </div>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { authApi } from '../api/client'

const router = useRouter()
const loading = ref(false)
const form = reactive({
  username: '',
  password: '',
})

async function submitLogin() {
  const username = (form.username || '').trim()
  const password = form.password || ''
  if (!username || !password) {
    ElMessage.warning('请输入用户名和密码')
    return
  }
  loading.value = true
  try {
    const { data } = await authApi.login({ username, password })
    if (data?.auth_enabled && data?.access_token) {
      localStorage.setItem('amm_token', data.access_token)
    }
    ElMessage.success('登录成功')
    await router.replace('/')
  } catch (e) {
    ElMessage.error(e.response?.data?.detail || '登录失败')
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.login-page {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}
.login-card {
  width: 100%;
  max-width: 420px;
  padding: 28px;
  border-radius: 14px;
  border: 1px solid var(--line-soft, #e5e7eb);
  background: var(--panel-bg, #fff);
}
.title {
  font-size: 24px;
  font-weight: 600;
  margin-bottom: 8px;
}
.subtitle {
  color: var(--text-secondary, #6b7280);
  margin-bottom: 20px;
}
</style>
