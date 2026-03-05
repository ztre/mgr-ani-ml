#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   BASE_URL=http://localhost:8000 \
#   GROUP='MySyncGroup' \
#   DIRNAME='Some Pending Dir' \
#   AUTH_ENABLED=1 \
#   USERNAME='admin' \
#   PASSWORD='your-password' \
#   ./scripts/sendTask_webhook.sh

BASE_URL="${BASE_URL:-http://localhost:8000}"
GROUP="${GROUP:-}"
DIRNAME="${DIRNAME:-}"
AUTH_ENABLED="${AUTH_ENABLED:-1}"
USERNAME="${USERNAME:-admin}"
PASSWORD="${PASSWORD:-}"

if [[ -z "${GROUP}" || -z "${DIRNAME}" ]]; then
  echo "[ERROR] GROUP 和 DIRNAME 必填"
  echo "示例: GROUP='TV组' DIRNAME='刀剑神域 Alicization' ./scripts/sendTask_webhook.sh"
  exit 1
fi

TOKEN=""
if [[ "${AUTH_ENABLED}" == "1" ]]; then
  if [[ -z "${PASSWORD}" ]]; then
    echo "[ERROR] AUTH_ENABLED=1 时 PASSWORD 必填"
    exit 1
  fi

  login_payload=$(printf '{"username":"%s","password":"%s"}' "$USERNAME" "$PASSWORD")
  login_resp=$(curl -sS -f \
    -H 'Content-Type: application/json' \
    -d "$login_payload" \
    "${BASE_URL}/api/auth/login")

  TOKEN=$(printf '%s' "$login_resp" | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
  if [[ -z "${TOKEN}" ]]; then
    echo "[ERROR] 登录成功但未解析到 access_token"
    echo "response=${login_resp}"
    exit 1
  fi
fi

echo "[INFO] 提交 sendTask: group=${GROUP}, dirname=${DIRNAME}"

curl_args=(
  -sS
  -w '\nHTTP_STATUS=%{http_code}\n'
  -X POST
  -F "dirname=${DIRNAME}"
  -F "group=${GROUP}"
)

if [[ -n "${TOKEN}" ]]; then
  curl_args+=( -H "Authorization: Bearer ${TOKEN}" )
fi

response=$(curl "${curl_args[@]}" "${BASE_URL}/sendTask")
printf '%s\n' "$response"
