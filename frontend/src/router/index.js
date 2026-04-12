import { createRouter, createWebHistory } from 'vue-router'
import { authApi } from '../api/client'

const routes = [
  { path: '/login', component: () => import('../views/Login.vue'), meta: { title: '登录', public: true } },
  { path: '/', component: () => import('../views/Dashboard.vue'), meta: { title: '仪表盘' } },
  { path: '/sync-groups', component: () => import('../views/SyncGroups.vue'), meta: { title: '同步组' } },
  { path: '/pending', component: () => import('../views/Pending.vue'), meta: { title: '待办清单' } },
  { path: '/pending-logs', component: () => import('../views/PendingLogs.vue'), meta: { title: '人工修正日志' } },
  { path: '/media', component: () => import('../views/Media.vue'), meta: { title: '媒体记录' } },
  { path: '/checks', component: () => import('../views/Checks.vue'), meta: { title: '检查中心' } },
  { path: '/inodes', component: () => import('../views/Inodes.vue'), meta: { title: 'Inode 管理' } },
  { path: '/config', component: () => import('../views/Config.vue'), meta: { title: '配置' } },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

let authEnabledCache = null
let authStatusLoading = null

async function ensureAuthEnabled() {
  if (authEnabledCache !== null) return authEnabledCache
  if (authStatusLoading) return authStatusLoading
  authStatusLoading = authApi
    .status()
    .then(({ data }) => {
      authEnabledCache = !!data?.enabled
      return authEnabledCache
    })
    .catch(() => {
      authEnabledCache = true
      return true
    })
    .finally(() => {
      authStatusLoading = null
    })
  return authStatusLoading
}

router.beforeEach(async (to, _from, next) => {
  document.title = to.meta.title ? `${to.meta.title} - Anime Media Manager` : 'Anime Media Manager'

  const authEnabled = await ensureAuthEnabled()
  if (!authEnabled) {
    if (to.path === '/login') return next('/')
    return next()
  }

  const token = localStorage.getItem('amm_token')
  if (to.meta.public) {
    if (token && to.path === '/login') return next('/')
    return next()
  }
  if (!token) return next('/login')
  next()
})

export default router
