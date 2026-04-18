import axios from 'axios'

const client = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
})

const LONG_TASK_TIMEOUT = 0

function postLongTask(url, data) {
  return client.post(url, data, { timeout: LONG_TASK_TIMEOUT })
}

function deleteLongTask(url) {
  return client.delete(url, { timeout: LONG_TASK_TIMEOUT })
}

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
  resources: (params) => client.get('/media/resources', { params }),
  resourceTree: (params) => client.get('/media/resource-tree', { params }),
  pending: (params) => client.get('/media/pending', { params }),
  sourceDirs: (params) => client.get('/media/source-dirs', { params }),
  pendingFiles: (id) => client.get(`/media/pending/${id}/files`),
  pendingLogs: (params) => client.get('/media/pending-logs', { params }),
  pendingLogKinds: (params) => client.get('/media/pending-logs-kinds', { params }),
  createPendingReview: (data) => client.post('/media/pending-logs/review', data),
  byTargetDir: (params) => client.get('/media/by-target-dir', { params }),
  searchTmdb: (params) => client.get('/media/search', { params }),
  poster: (params) => client.get('/media/poster', { params }),
  seasonPoster: (params) => client.get('/media/season-poster', { params }),
  stats: () => client.get('/media/stats'),
  deleteAll: () => deleteLongTask('/media/all'),
  batchDelete: (data) => postLongTask('/media/batch-delete', data),
  deleteScope: (data) => postLongTask('/media/delete-scope', data),
  deduplicate: () => postLongTask('/media/deduplicate'),
  reidentify: (id, data) => postLongTask(`/media/${id}/reidentify`, data),
  reidentifyByTargetDir: (data) => postLongTask('/media/reidentify-by-target-dir', data),
  reidentifyScope: (data) => postLongTask('/media/reidentify-scope', data),
  batchReidentify: (data) => postLongTask('/media/batch-reidentify', data),
  adjust: (id, data) => postLongTask(`/media/${id}/adjust`, data),
  manualOrganize: (id, data) => postLongTask(`/media/${id}/manual-organize`, data),
  fsList: (prefix) => client.get('/media/fs/list', { params: { prefix } }),
}

export const inodesApi = {
  list: (params) => client.get('/inodes', { params }),
  resources: (params) => client.get('/inodes/resources', { params }),
  resourceTree: (params) => client.get('/inodes/resource-tree', { params }),
  delete: (id) => client.delete(`/inodes/${id}`),
  batchDelete: (data) => postLongTask('/inodes/batch-delete', data),
  deleteScope: (data) => postLongTask('/inodes/delete-scope', data),
  deleteAll: () => deleteLongTask('/inodes/all'),
  cleanup: () => deleteLongTask('/inodes/cleanup'),
}

export const scanApi = {
  run: () => postLongTask('/scan/run'),
  runGroup: (groupId) => postLongTask(`/scan/run/${groupId}`),
}

export const embyApi = {
  libraries: () => client.get('/emby/libraries'),
  refresh: () => postLongTask('/emby/refresh'),
}

export const configApi = {
  get: () => client.get('/config'),
  update: (data) => client.put('/config', data),
  restart: () => postLongTask('/config/restart'),
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
  cancel: (id) => client.post(`/tasks/${id}/cancel`),
  deleteAll: () => client.delete('/tasks/all'),
}

export const checksApi = {
  runFull: () => postLongTask('/checks/run'),
  listRuns: (params) => client.get('/checks/runs', { params }),
  listIssues: (params) => client.get('/checks/issues', { params }),
  ignoreIssue: (id) => client.post(`/checks/issues/${id}/ignore`),
  resolveIssue: (id) => client.post(`/checks/issues/${id}/resolve`),
  reopenIssue: (id) => client.post(`/checks/issues/${id}/reopen`),
  batchAction: (ids, action) => client.post('/checks/issues/batch-action', { ids, action }),
  deleteAll: () => client.delete('/checks/all'),
}

export const washApi = {
  candidates: (params) => client.get('/media/wash/candidates', { params }),
  sourceScan: (params) => client.get('/media/wash/source-scan', { params }),
  execute: (data) => postLongTask('/media/wash/execute', data),
  sourceOrganize: (data) => postLongTask('/media/wash/source-dir-organize', data),
}

export const manualRecordApi = {
  create: (data) => client.post('/media/manual-record', data),
  subtitleBatchPreview: (data) => client.post('/media/subtitle-batch-preview', data),
  subtitleBatchImport: (formData) => client.post('/media/subtitle-batch-import', formData, {
    headers: { 'Content-Type': 'multipart/form-data' },
  }),
}
