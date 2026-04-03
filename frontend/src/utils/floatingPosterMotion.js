export function resourceIdentityKey(resource) {
  if (!resource) return ''
  if (resource.key) return String(resource.key)
  return [
    resource.sync_group_id ?? '',
    resource.type ?? '',
    resource.tmdb_id ?? '',
    resource.resource_dir ?? '',
    resource.resource_name ?? '',
  ].join('|')
}

export function setResourceIconElement(iconMap, resource, el) {
  const key = resourceIdentityKey(resource)
  if (!key) return
  if (el instanceof HTMLElement) {
    iconMap.set(key, el)
    return
  }
  iconMap.delete(key)
}

function resolveDurationMs(el, cssVarName, fallbackMs) {
  const value = getComputedStyle(el).getPropertyValue(cssVarName).trim()
  if (!value) return fallbackMs
  if (value.endsWith('ms')) {
    const parsed = Number.parseFloat(value)
    return Number.isFinite(parsed) ? parsed : fallbackMs
  }
  if (value.endsWith('s')) {
    const parsed = Number.parseFloat(value)
    return Number.isFinite(parsed) ? parsed * 1000 : fallbackMs
  }
  return fallbackMs
}

function resolveCollapseTransform(shellEl, targetEl) {
  if (!(shellEl instanceof HTMLElement) || !(targetEl instanceof HTMLElement)) {
    return 'translate(24px, 14px) scale(0.95, 0.95)'
  }

  const shellRect = shellEl.getBoundingClientRect()
  const targetRect = targetEl.getBoundingClientRect()
  const shellCenterX = shellRect.left + shellRect.width / 2
  const shellCenterY = shellRect.top + shellRect.height / 2
  const targetCenterX = targetRect.left + targetRect.width / 2
  const targetCenterY = targetRect.top + targetRect.height / 2
  const scaleX = Math.max(0.12, targetRect.width / Math.max(shellRect.width, 1))
  const scaleY = Math.max(0.12, targetRect.height / Math.max(shellRect.height, 1))

  return `translate(${targetCenterX - shellCenterX}px, ${targetCenterY - shellCenterY}px) scale(${scaleX}, ${scaleY})`
}

function cleanupAnimatedStyles(shellEl, cardEl) {
  if (shellEl instanceof HTMLElement) {
    shellEl.style.transition = ''
    shellEl.style.transform = ''
    shellEl.style.opacity = ''
  }
  if (cardEl instanceof HTMLElement) {
    cardEl.style.transition = ''
    cardEl.style.boxShadow = ''
  }
}

export function animateFloatingPosterEnter(shellEl, targetEl, done) {
  if (!(shellEl instanceof HTMLElement)) {
    done()
    return
  }

  const cardEl = shellEl.querySelector('.drawer-floating-poster-card')
  const durationMs = resolveDurationMs(shellEl, '--el-transition-duration', 300)
  const startTransform = resolveCollapseTransform(shellEl, targetEl)

  shellEl.style.transition = 'none'
  shellEl.style.opacity = '0'
  shellEl.style.transform = startTransform
  if (cardEl instanceof HTMLElement) {
    cardEl.style.transition = 'none'
    cardEl.style.boxShadow = '0 12px 38px rgba(15, 23, 42, 0.12)'
  }

  void shellEl.offsetWidth

  requestAnimationFrame(() => {
    shellEl.style.transition = `opacity ${durationMs}ms cubic-bezier(0.55, 0, 0.1, 1), transform ${durationMs}ms cubic-bezier(0.55, 0, 0.1, 1)`
    shellEl.style.opacity = '1'
    shellEl.style.transform = 'none'
    if (cardEl instanceof HTMLElement) {
      cardEl.style.transition = `box-shadow ${durationMs}ms cubic-bezier(0.55, 0, 0.1, 1)`
      cardEl.style.boxShadow = '0 32px 96px rgba(15, 23, 42, 0.28)'
    }
  })

  window.setTimeout(() => {
    cleanupAnimatedStyles(shellEl, cardEl)
    done()
  }, durationMs)
}

export function animateFloatingPosterLeave(shellEl, targetEl, done) {
  if (!(shellEl instanceof HTMLElement)) {
    done()
    return
  }

  const cardEl = shellEl.querySelector('.drawer-floating-poster-card')
  const durationMs = 140
  const endTransform = resolveCollapseTransform(shellEl, targetEl)

  shellEl.style.transition = `opacity ${durationMs}ms cubic-bezier(0.4, 0, 1, 1), transform ${durationMs}ms cubic-bezier(0.4, 0, 1, 1)`
  shellEl.style.opacity = '1'
  shellEl.style.transform = 'none'
  if (cardEl instanceof HTMLElement) {
    cardEl.style.transition = `box-shadow ${durationMs}ms cubic-bezier(0.4, 0, 1, 1)`
    cardEl.style.boxShadow = '0 32px 96px rgba(15, 23, 42, 0.28)'
  }

  void shellEl.offsetWidth

  requestAnimationFrame(() => {
    shellEl.style.opacity = '0'
    shellEl.style.transform = endTransform
    if (cardEl instanceof HTMLElement) {
      cardEl.style.boxShadow = '0 12px 38px rgba(15, 23, 42, 0.12)'
    }
  })

  window.setTimeout(() => {
    cleanupAnimatedStyles(shellEl, cardEl)
    done()
  }, durationMs)
}