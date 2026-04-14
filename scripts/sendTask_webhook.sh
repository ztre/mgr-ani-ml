#!/usr/bin/env bash
set -euo pipefail

# qBittorrent「设置 → BitTorrent → Torrent 完成时运行外部程序」配置示例：
#   /path/to/sendTask_webhook.sh -g "%L" -d "%N"
#
# 所有连接参数（URL、账号密码）及分类映射均在配置文件中管理，
# 脚本本身只接收 qBittorrent 传入的两个变量：
#   %L  分类  → -g
#   %N  名称  → -d

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 配置文件搜索顺序（找到第一个存在的即使用）
_default_conf() {
  for f in \
      "${SCRIPT_DIR}/sendTask.conf" \
      "${HOME}/.config/anime-media-manager/sendTask.conf" \
      "/etc/anime-media-manager/sendTask.conf"; do
    [[ -f "$f" ]] && echo "$f" && return
  done
}

usage() {
  cat <<EOF
用法: $0 -g CATEGORY -d DIRNAME [-c CONF]

  -g CATEGORY   qBittorrent 分类 [qBit: "%L"]，通过配置文件映射到同步组名称
  -d DIRNAME    Torrent 名称    [qBit: "%N"]
  -c CONF       指定配置文件路径

配置文件搜索顺序:
  ${SCRIPT_DIR}/sendTask.conf
  ~/.config/anime-media-manager/sendTask.conf
  /etc/anime-media-manager/sendTask.conf

初始化配置:
  cp ${SCRIPT_DIR}/sendTask.conf.example ${SCRIPT_DIR}/sendTask.conf
EOF
  exit 1
}

CATEGORY=""
DIRNAME=""
CONF_FILE=""

while getopts ":g:d:c:h" opt; do
  case $opt in
    g) CATEGORY="$OPTARG" ;;
    d) DIRNAME="$OPTARG" ;;
    c) CONF_FILE="$OPTARG" ;;
    h) usage ;;
    :) echo "[ERROR] 选项 -$OPTARG 需要参数"; usage ;;
    \?) echo "[ERROR] 未知选项 -$OPTARG"; usage ;;
  esac
done

if [[ -z "${CATEGORY}" || -z "${DIRNAME}" ]]; then
  echo "[ERROR] -g CATEGORY 和 -d DIRNAME 必填"
  usage
fi

# 确定配置文件
[[ -z "${CONF_FILE}" ]] && CONF_FILE="$(_default_conf || true)"
if [[ -z "${CONF_FILE}" || ! -f "${CONF_FILE}" ]]; then
  echo "[ERROR] 未找到配置文件，请创建后重试"
  echo "  cp ${SCRIPT_DIR}/sendTask.conf.example ${SCRIPT_DIR}/sendTask.conf"
  exit 1
fi

# 配置默认值（可被配置文件覆盖）
BASE_URL="http://localhost:8000"
USERNAME="admin"
PASSWORD=""
NO_AUTH=0
declare -A GROUP_MAP

# shellcheck source=/dev/null
source "${CONF_FILE}"
echo "[INFO] 配置文件: ${CONF_FILE}"

# 分类 → 同步组名称映射
GROUP="${GROUP_MAP[${CATEGORY}]:-}"
if [[ -z "${GROUP}" ]]; then
  keys="${!GROUP_MAP[*]}"
  echo "[ERROR] 分类 '${CATEGORY}' 在 GROUP_MAP 中无映射"
  echo "  已配置的分类: ${keys:-（无）}"
  exit 1
fi
echo "[INFO] 分类映射: '${CATEGORY}' → '${GROUP}'"

# 临时文件存储响应体，退出时自动清理
RESP=$(mktemp)
trap 'rm -f "$RESP"' EXIT

# ── JSON 工具函数 ─────────────────────────────────────────────────────────────
_str() {
  if command -v jq &>/dev/null; then
    printf '%s' "$1" | jq -r --arg k "$2" '.[$k] // empty' 2>/dev/null || true
  else
    printf '%s' "$1" \
      | sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" \
      | head -1
  fi
}

_is_true() {
  if command -v jq &>/dev/null; then
    [[ "$(printf '%s' "$1" | jq -r --arg k "$2" '.[$k] // false' 2>/dev/null || echo false)" == "true" ]]
  else
    printf '%s' "$1" | grep -q "\"$2\"[[:space:]]*:[[:space:]]*true"
  fi
}

# ── Token 缓存 ────────────────────────────────────────────────────────────────
# 缓存文件命名包含 BASE_URL hash，支持多实例互不干扰
_token_cache_file() {
  local key
  key=$(printf '%s:%s' "$BASE_URL" "$USERNAME" \
    | tr -dc 'a-zA-Z0-9:._-' | tr '/:' '__' | cut -c1-60)
  printf '%s/.token_%s' "$SCRIPT_DIR" "$key"
}

# 从 JWT payload（base64url）提取 exp 字段（纯 bash + base64，无需 jq）
_jwt_exp() {
  local jwt="$1"
  local payload="${jwt#*.}"   # 去掉 header.
  payload="${payload%%.*}"    # 去掉 .signature
  # base64url → base64（补齐 padding）
  local padded="${payload//-/+}"
  padded="${padded//_//}"
  local rem=$(( ${#padded} % 4 ))
  [[ $rem -eq 2 ]] && padded="${padded}=="
  [[ $rem -eq 3 ]] && padded="${padded}="
  printf '%s' "$padded" | base64 -d 2>/dev/null \
    | sed -n 's/.*"exp"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' | head -1
}

_do_login() {
  local payload
  payload=$(printf '{"username":"%s","password":"%s"}' "$USERNAME" "$PASSWORD")
  local code
  code=$(curl -sS \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    -o "$RESP" -w '%{http_code}' \
    "${BASE_URL}/api/auth/login") || true
  local body
  body=$(cat "$RESP")

  if [[ ! "${code}" =~ ^2 ]]; then
    echo "[ERROR] 登录失败 (HTTP ${code}): $(_str "$body" "detail")"
    exit 1
  fi

  local token
  token=$(_str "$body" "access_token")
  if [[ -z "${token}" ]]; then
    echo "[ERROR] 登录成功但未解析到 access_token，响应: ${body}"
    exit 1
  fi

  # 写缓存（权限 600）
  local cache_file
  cache_file="$(_token_cache_file)"
  printf '%s\n' "$token" > "$cache_file"
  chmod 600 "$cache_file"
  echo "[INFO] 登录成功，token 已缓存: ${cache_file}"
  printf '%s' "$token"
}

# ── 登录（带缓存）─────────────────────────────────────────────────────────────
TOKEN=""
if [[ "${NO_AUTH}" == "0" ]]; then
  if [[ -z "${PASSWORD}" ]]; then
    echo "[ERROR] 配置文件中 PASSWORD 未设置"
    exit 1
  fi

  CACHE_FILE="$(_token_cache_file)"
  REUSED=0

  if [[ -f "$CACHE_FILE" ]]; then
    cached_token=$(cat "$CACHE_FILE")
    exp=$(_jwt_exp "$cached_token")
    now=$(date +%s)
    # 提前 60 秒视为过期，避免边界请求失败
    if [[ -n "$exp" && $(( exp - now )) -gt 60 ]]; then
      TOKEN="$cached_token"
      REUSED=1
      echo "[INFO] 复用已缓存 token（剩余 $(( exp - now )) 秒）"
    fi
  fi

  if [[ $REUSED -eq 0 ]]; then
    TOKEN="$(_do_login)"
  fi
fi

# ── 提交任务（含 401 自动重登录）────────────────────────────────────────────
echo "[INFO] 提交任务: group=${GROUP}, dirname=${DIRNAME}"

_do_send() {
  local curl_args=(-sS -X POST -F "dirname=${DIRNAME}" -F "group=${GROUP}")
  [[ -n "${TOKEN}" ]] && curl_args+=(-H "Authorization: Bearer ${TOKEN}")
  curl "${curl_args[@]}" -o "$RESP" -w '%{http_code}' "${BASE_URL}/sendTask" || true
}

code="$(_do_send)"
body=$(cat "$RESP")

# 若 token 已被服务端吊销，删缓存后重新登录再试一次
if [[ "${code}" == "401" && "${NO_AUTH}" == "0" ]]; then
  echo "[INFO] Token 已失效，重新登录..."
  rm -f "${CACHE_FILE:-}"
  TOKEN="$(_do_login)"
  code="$(_do_send)"
  body=$(cat "$RESP")
fi

# ── 结果判定 ──────────────────────────────────────────────────────────────────
echo "[INFO] HTTP 状态: ${code}"

if [[ "${code}" =~ ^2 ]] && _is_true "$body" "ok"; then
  msg=$(_str "$body" "message")
  task_id=$(_str "$body" "task_id")
  target_dir=$(_str "$body" "target_dir")
  src=$(_str "$body" "source")
  echo "[SUCCESS] ${msg:-任务已提交}"
  [[ -n "$task_id" ]]    && echo "[INFO]   task_id=${task_id}"
  [[ -n "$target_dir" ]] && echo "[INFO]   target_dir=${target_dir}"
  [[ -n "$src" ]]        && echo "[INFO]   source=${src}"
  exit 0
fi

detail=$(_str "$body" "detail")
echo "[FAILED] 提交失败 (HTTP ${code}): ${detail:-$body}"
exit 1
