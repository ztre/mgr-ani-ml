import axios from 'axios'

const client = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
})

client.interceptors.request.use((config) => {
  const token = localStorage.getItem('amm_token')
  if (token) {
    config.headers = config.headers || {}
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

client.interceptors.response.use(
  (resp) => resp,
  (error) => {
    const status = error?.response?.status
    const url = String(error?.config?.url || '')
    if (status === 401 && !url.includes('/auth/login') && !url.includes('/auth/status')) {
      localStorage.removeItem('amm_token')
      if (window.location.pathname !== '/login') {
        window.location.href = '/login'
      }
    }
    return Promise.reject(error)
  },
)

export const syncGroupsApi = {
  list: () => client.get('/sync-groups'),
  create: (data) => client.post('/sync-groups', data),
  get: (id) => client.get(`/sync-groups/${id}`),
  update: (id, data) => client.put(`/sync-groups/${id}`, data),
  delete: (id) => client.delete(`/sync-groups/${id}`),
}

export const mediaApi = {
  list: (params) => client.get('/media', { params }),
  pending: (params) => client.get('/media/pending', { params }),
  byTargetDir: (params) => client.get('/media/by-target-dir', { params }),
  searchTmdb: (params) => client.get('/media/search', { params }),
  seasonPoster: (params) => client.get('/media/season-poster', { params }),
  stats: () => client.get('/media/stats'),
  deleteAll: () => client.delete('/media/all'),
  batchDelete: (data) => client.post('/media/batch-delete', data),
  deduplicate: () => client.post('/media/deduplicate'),
  reidentify: (id, data) => client.post(`/media/${id}/reidentify`, data),
  reidentifyByTargetDir: (data) => client.post('/media/reidentify-by-target-dir', data),
  manualOrganize: (id, data) => client.post(`/media/${id}/manual-organize`, data),
}

export const inodesApi = {
  list: (params) => client.get('/inodes', { params }),
  delete: (id) => client.delete(`/inodes/${id}`),
  deleteAll: () => client.delete('/inodes/all'),
  cleanup: () => client.delete('/inodes/cleanup'),
}

export const scanApi = {
  run: () => client.post('/scan/run'),
  runGroup: (groupId) => client.post(`/scan/run/${groupId}`),
}

export const embyApi = {
  libraries: () => client.get('/emby/libraries'),
  refresh: () => client.post('/emby/refresh'),
}

export const configApi = {
  get: () => client.get('/config'),
  update: (data) => client.put('/config', data),
  restart: () => client.post('/config/restart'),
  testConnection: (data) => client.post('/config/test-connection', data),
  changePassword: (data) => client.post('/config/change-password', data),
}

export const authApi = {
  status: () => client.get('/auth/status'),
  login: (data) => client.post('/auth/login', data),
}

export const tasksApi = {
  list: (params) => client.get('/tasks', { params }),
  getLogs: (id) => client.get(`/tasks/${id}/logs`),
  deleteAll: () => client.delete('/tasks/all'),
}
