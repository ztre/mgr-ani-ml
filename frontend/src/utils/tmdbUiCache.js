const CACHE_PREFIX = 'amm.tmdb-ui-cache.v1'
const DEFAULT_TTL_MS = 7 * 24 * 60 * 60 * 1000

function buildStorageKey(kind, key) {
  return `${CACHE_PREFIX}:${kind}:${key}`
}

function readCache(kind, key) {
  if (!key) return null
  try {
    const raw = localStorage.getItem(buildStorageKey(kind, key))
    if (!raw) return null
    const payload = JSON.parse(raw)
    if (!payload || typeof payload !== 'object') return null
    if (payload.expires_at && Number(payload.expires_at) < Date.now()) {
      localStorage.removeItem(buildStorageKey(kind, key))
      return null
    }
    return payload.data ?? null
  } catch {
    return null
  }
}

function writeCache(kind, key, data, ttlMs = DEFAULT_TTL_MS) {
  if (!key) return
  try {
    localStorage.setItem(
      buildStorageKey(kind, key),
      JSON.stringify({
        expires_at: Date.now() + ttlMs,
        data,
      }),
    )
  } catch {
    // ignore quota/storage failures
  }
}

export function getCachedPoster(key) {
  return readCache('poster', key)
}

export function setCachedPoster(key, url, ttlMs) {
  writeCache('poster', key, url || '', ttlMs)
}

export function getCachedMeta(key) {
  return readCache('meta', key)
}

export function setCachedMeta(key, data, ttlMs) {
  writeCache('meta', key, data || {}, ttlMs)
}
