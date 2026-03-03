#!/bin/sh
set -eu

PUID="${PUID:-1000}"
PGID="${PGID:-100}"

if getent group "${PGID}" >/dev/null 2>&1; then
  APP_GROUP="$(getent group "${PGID}" | cut -d: -f1)"
else
  APP_GROUP="appgroup"
  groupadd -o -g "${PGID}" "${APP_GROUP}"
fi

if id -u appuser >/dev/null 2>&1; then
  CURRENT_UID="$(id -u appuser)"
  CURRENT_GID="$(id -g appuser)"
  if [ "${CURRENT_UID}" != "${PUID}" ]; then
    usermod -o -u "${PUID}" appuser
  fi
  if [ "${CURRENT_GID}" != "${PGID}" ]; then
    usermod -o -g "${PGID}" appuser
  fi
else
  useradd -o -m -u "${PUID}" -g "${PGID}" -s /usr/sbin/nologin appuser
fi

mkdir -p /app/data /app/logs /media
chown -R "${PUID}:${PGID}" /app/data /app/logs

exec gosu "${PUID}:${PGID}" "$@"
