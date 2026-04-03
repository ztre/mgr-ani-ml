import { ref } from 'vue'
import { getCachedMeta, setCachedMeta } from './tmdbUiCache'

const POSTER_META_TTL_MS = 7 * 24 * 60 * 60 * 1000
const posterMetaByKey = ref({})
const posterRequests = new Map()

function resourceMediaType(resource) {
  return resource?.type === 'movie' ? 'movie' : 'tv'
}

function posterCacheKey(resource) {
  const tmdbId = Number(resource?.tmdb_id || 0)
  if (!tmdbId) return ''
  return `tmdb:${resourceMediaType(resource)}:${tmdbId}`
}

function normalizePosterMeta(meta) {
  return {
    poster_path: meta?.poster_path || '',
    backdrop_path: meta?.backdrop_path || '',
  }
}

export function buildPosterUrl(posterPath, size = 'w342') {
  if (!posterPath) return ''
  if (String(posterPath).startsWith('http')) return posterPath
  return `https://image.tmdb.org/t/p/${size}${posterPath}`
}

function syncPosterMeta(key, meta) {
  if (!key) return normalizePosterMeta(meta)
  const normalized = normalizePosterMeta(meta)
  posterMetaByKey.value = {
    ...posterMetaByKey.value,
    [key]: normalized,
  }
  return normalized
}

export function useResourcePoster(fetchPoster) {
  function hydrateCachedPosterMeta(items) {
    const nextMeta = { ...posterMetaByKey.value }
    for (const item of items || []) {
      const key = posterCacheKey(item)
      if (!key || nextMeta[key]) continue
      const cached = getCachedMeta(key)
      if (cached && typeof cached === 'object') {
        nextMeta[key] = normalizePosterMeta(cached)
      }
    }
    posterMetaByKey.value = nextMeta
  }

  async function ensurePosterForResource(resource) {
    const key = posterCacheKey(resource)
    if (!key) return normalizePosterMeta(null)

    const inMemory = posterMetaByKey.value[key]
    if (inMemory) return inMemory

    const cached = getCachedMeta(key)
    if (cached && typeof cached === 'object') {
      return syncPosterMeta(key, cached)
    }

    if (posterRequests.has(key)) {
      return posterRequests.get(key)
    }

    const request = Promise.resolve(fetchPoster({
      tmdb_id: Number(resource.tmdb_id),
      media_type: resourceMediaType(resource),
    }))
      .then((data) => {
        const meta = syncPosterMeta(key, data)
        setCachedMeta(key, meta, POSTER_META_TTL_MS)
        return meta
      })
      .catch(() => {
        const meta = syncPosterMeta(key, null)
        setCachedMeta(key, meta, POSTER_META_TTL_MS)
        return meta
      })
      .finally(() => {
        posterRequests.delete(key)
      })

    posterRequests.set(key, request)
    return request
  }

  function resourcePosterUrl(resource, size = 'w342') {
    const key = posterCacheKey(resource)
    if (!key) return ''
    return buildPosterUrl(posterMetaByKey.value[key]?.poster_path, size)
  }

  function resourcePosterAlt(resource) {
    return `${resource?.resource_name || 'TMDB'} poster`
  }

  return {
    ensurePosterForResource,
    hydrateCachedPosterMeta,
    resourcePosterUrl,
    resourcePosterAlt,
  }
}