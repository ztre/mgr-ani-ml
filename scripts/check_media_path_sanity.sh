#!/usr/bin/env bash
# check_media_path_sanity.sh
# 检查媒体库目标路径合理性，结合日志目录做多项验证：
#   1. DB 中 target_path 对应文件是否实际存在
#   2. 记录的 type 与所在目录是否匹配（tv 文件不应在 movie 根目录，反之亦然）
#   3. 日志中有成功记录但 DB 无对应条目且文件仍存在（孤立文件）
#   4. 日志中曾警报"旧目标目录未完全删除"的目录是否仍存在
#
# 用法:
#   bash check_media_path_sanity.sh [选项]
#
# 选项:
#   --db <path>         SQLite 数据库路径（默认: 脚本上级目录下的 anime_media.db
#                       或 /app/data/anime_media.db）
#   --log-dir <dir>     日志目录路径（默认: 脚本上级目录下的 logs/）
#   --tv-root <dir>     TV 媒体库根目录（覆盖从 DB 读取）
#   --movie-root <dir>  Movie 媒体库根目录（覆盖从 DB 读取）
#   --remap <old:new>   路径前缀重映射，DB 中存储的路径前缀 -> 实际文件系统路径前缀
#                       可多次指定。例如: --remap /media/links:/mnt/user/media/links
#   --verbose           输出所有 OK 条目
#
# 示例:
#   bash check_media_path_sanity.sh
#   bash check_media_path_sanity.sh --log-dir /var/log/mgr --tv-root /media/links/anime_tv --movie-root /media/links/anime_movies
#   bash check_media_path_sanity.sh --remap /media/links/anime_tv:/mnt/user/media/links/anime_tv --remap /media/links/anime_movies:/mnt/user/media/links/anime_movies
#   bash check_media_path_sanity.sh --verbose

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ── 默认值 ────────────────────────────────────────────────────────────────────
DB_PATH=""
LOG_DIR="$PROJECT_ROOT/logs"
TV_ROOT=""
MOVIE_ROOT=""
VERBOSE=0
REMAP_FROM=()   # DB 中的路径前缀
REMAP_TO=()     # 对应的实际路径前缀

# ── 参数解析 ──────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)         DB_PATH="$2";    shift 2 ;;
        --log-dir)    LOG_DIR="$2";    shift 2 ;;
        --tv-root)    TV_ROOT="$2";    shift 2 ;;
        --movie-root) MOVIE_ROOT="$2"; shift 2 ;;
        --remap)
            remap_val="$2"
            remap_from_part="${remap_val%%:*}"
            remap_to_part="${remap_val#*:}"
            if [[ -z "$remap_from_part" || -z "$remap_to_part" || "$remap_from_part" == "$remap_to_part" ]]; then
                echo "错误: --remap 格式应为 old_prefix:new_prefix，例如 /media/links:/mnt/user/media/links" >&2
                exit 1
            fi
            REMAP_FROM+=("$remap_from_part")
            REMAP_TO+=("$remap_to_part")
            shift 2
            ;;
        --verbose|-v) VERBOSE=1;       shift   ;;
        *) echo "未知参数: $1" >&2; exit 1 ;;
    esac
done

# ── 确定 DB 路径 ───────────────────────────────────────────────────────────────
if [[ -z "$DB_PATH" ]]; then
    if [[ -f "/app/data/anime_media.db" ]]; then
        DB_PATH="/app/data/anime_media.db"
    elif [[ -f "$PROJECT_ROOT/anime_media.db" ]]; then
        DB_PATH="$PROJECT_ROOT/anime_media.db"
    else
        echo "错误: 找不到数据库文件，请用 --db 指定路径" >&2
        exit 1
    fi
fi

[[ -f "$DB_PATH" ]]   || { echo "错误: 数据库不存在: $DB_PATH" >&2; exit 1; }
[[ -d "$LOG_DIR" ]]   || { echo "错误: 日志目录不存在: $LOG_DIR" >&2; exit 1; }
command -v sqlite3 &>/dev/null || { echo "错误: sqlite3 未安装" >&2; exit 1; }

# ── 从 DB 读取媒体库根目录（若未手动传入）────────────────────────────────────
if [[ -z "$TV_ROOT" ]]; then
    TV_ROOT="$(sqlite3 "$DB_PATH" \
        "SELECT target FROM sync_groups WHERE enabled=1 AND source_type='tv' LIMIT 1;")"
fi
if [[ -z "$MOVIE_ROOT" ]]; then
    MOVIE_ROOT="$(sqlite3 "$DB_PATH" \
        "SELECT target FROM sync_groups WHERE enabled=1 AND source_type='movie' LIMIT 1;")"
fi

# ── 路径重映射函数 ────────────────────────────────────────────────────────────
# 将 DB 中存储的路径转换为实际文件系统路径
remap_path() {
    local path="$1"
    local i
    for (( i=0; i<${#REMAP_FROM[@]}; i++ )); do
        local from="${REMAP_FROM[$i]}"
        local to="${REMAP_TO[$i]}"
        if [[ "$path" == "$from"* ]]; then
            echo "${to}${path#$from}"
            return
        fi
    done
    echo "$path"
}

echo "数据库:     $DB_PATH"
echo "日志目录:   $LOG_DIR"
echo "TV 根目录:  ${TV_ROOT:-(未配置)}"
echo "Movie根目录: ${MOVIE_ROOT:-(未配置)}"
if [[ ${#REMAP_FROM[@]} -gt 0 ]]; then
    echo "路径重映射:"
    for (( i=0; i<${#REMAP_FROM[@]}; i++ )); do
        echo "            ${REMAP_FROM[$i]}  ->  ${REMAP_TO[$i]}"
    done
fi
echo ""

# ── 解析日志 ──────────────────────────────────────────────────────────────────
# 提取所有"处理成功"或"单文件修正完成"的目标路径（-> 右侧）
LOGGED_TARGETS_FILE="$(mktemp)"
BLOCKED_DIRS_FILE="$(mktemp)"
trap 'rm -f "$LOGGED_TARGETS_FILE" "$BLOCKED_DIRS_FILE"' EXIT

# 日志格式: "处理成功: 源文件 -> /path/to/target"
#           "单文件修正完成: 源文件 -> /path/to/target（附件N个）"
#           "附件跟随正片: 源文件 -> /path/to/target (mode=...)"
grep -hE "(处理成功|单文件修正完成|附件跟随正片|附件修正完成): .+ -> /" \
    "$LOG_DIR"/*.log 2>/dev/null \
    | sed -E 's/.*-> ([^ (]+).*/\1/' \
    | sort -u > "$LOGGED_TARGETS_FILE" || true

# 曾被"未完全删除"警报的目录，格式: "-> /path/to/dir (Directory not empty)"
grep -hE "修正后旧目标目录未完全删除" "$LOG_DIR"/*.log 2>/dev/null \
    | grep -oE '/[^(]+\(Directory not empty\)' \
    | sed 's/ (Directory not empty)//' \
    | sed 's/[[:space:]]*$//' \
    | sort -u > "$BLOCKED_DIRS_FILE" || true

LOGGED_COUNT="$(wc -l < "$LOGGED_TARGETS_FILE")"
BLOCKED_COUNT="$(wc -l < "$BLOCKED_DIRS_FILE")"

# ── 查询 DB 记录 ───────────────────────────────────────────────────────────────
# 输出格式: id|type|target_path
DB_RECORDS="$(sqlite3 "$DB_PATH" \
    "SELECT id, type, target_path FROM media_records
     WHERE status IN ('scraped','manual_fixed')
       AND target_path IS NOT NULL AND target_path != '';")"

TOTAL=0
OK=0
ISSUES=0

declare -A DB_TARGET_SET  # target_path -> 1，用于孤立文件检测

echo "开始检查..."
echo ""

while IFS='|' read -r rec_id rec_type target_path; do
    [[ -z "$target_path" ]] && continue
    (( TOTAL++ )) || true
    DB_TARGET_SET["$target_path"]=1
    ok=1

    # 对 DB 存储路径应用重映射，得到实际文件系统路径
    real_path="$(remap_path "$target_path")"

    # ── 检查1：文件是否实际存在 ───────────────────────────────────────────────
    if [[ ! -e "$real_path" ]]; then
        echo "[MISSING]           id=$rec_id type=$rec_type"
        echo "                    db_path=$target_path"
        [[ "$real_path" != "$target_path" ]] && echo "                    real_path=$real_path"
        ok=0
    else
        # ── 检查2：type 与所在目录是否错位（基于重映射后的真实路径）──────────
        real_tv_root="$(remap_path "${TV_ROOT:-}")"
        real_movie_root="$(remap_path "${MOVIE_ROOT:-}")"
        if [[ "$rec_type" == "tv" && -n "$real_movie_root" ]]; then
            if [[ "$real_path" == "$real_movie_root"/* ]]; then
                echo "[TYPE_MISMATCH tv→in→movie]  id=$rec_id"
                echo "                    real_path=$real_path"
                echo "                    movie_root=$real_movie_root"
                ok=0
            fi
        fi
        if [[ "$rec_type" == "movie" && -n "$real_tv_root" ]]; then
            if [[ "$real_path" == "$real_tv_root"/* ]]; then
                echo "[TYPE_MISMATCH movie→in→tv]  id=$rec_id"
                echo "                    real_path=$real_path"
                echo "                    tv_root=$real_tv_root"
                ok=0
            fi
        fi
    fi

    if [[ $ok -eq 1 ]]; then
        (( OK++ )) || true
        [[ $VERBOSE -eq 1 ]] && echo "  OK  id=$rec_id type=$rec_type  $real_path"
    else
        (( ISSUES++ )) || true
    fi
done <<< "$DB_RECORDS"

# ── 检查3：日志目标仍存在但不在 DB 中（孤立文件）────────────────────────────
echo ""
echo "检查孤立文件（日志有记录但DB无、文件仍存在）..."
orphan_found=0
while IFS= read -r logged_target; do
    [[ -z "$logged_target" ]] && continue
    real_logged="$(remap_path "$logged_target")"
    # 不在 DB 当前记录中（同时检查原始路径和重映射后路径）
    if [[ -z "${DB_TARGET_SET[$logged_target]+x}" && -z "${DB_TARGET_SET[$real_logged]+x}" ]]; then
        # 文件还存在于磁盘
        if [[ -f "$real_logged" ]]; then
            echo "[ORPHAN_FILE]       日志有记录但DB无对应条目，文件仍存在:"
            echo "                    $real_logged"
            (( ISSUES++ )) || true
            (( orphan_found++ )) || true
        fi
    fi
done < "$LOGGED_TARGETS_FILE"
[[ $orphan_found -eq 0 ]] && echo "  (无孤立文件)"

# ── 检查4：曾警报"未完全删除"的旧目录是否已清理────────────────────────────
echo ""
echo "检查曾残留的旧目标目录..."
blocked_found=0
while IFS= read -r blocked_dir; do
    [[ -z "$blocked_dir" ]] && continue
    real_blocked="$(remap_path "$blocked_dir")"
    if [[ -d "$real_blocked" ]]; then
        blocked_dir="$real_blocked"  # 后续统一用真实路径
    fi
    if [[ -d "$blocked_dir" ]]; then
        # 统计目录内容
        total_files=$(find "$blocked_dir" -maxdepth 1 -type f 2>/dev/null | wc -l)
        total_dirs=$(find "$blocked_dir" -maxdepth 1 -mindepth 1 -type d 2>/dev/null | wc -l)
        real_media=$(find "$blocked_dir" -maxdepth 1 -type f \
            \( -iname "*.mkv" -o -iname "*.mp4" -o -iname "*.avi" \
               -o -iname "*.mov" -o -iname "*.webm" -o -iname "*.flv" \
               -o -iname "*.ass" -o -iname "*.srt" -o -iname "*.ssa" \
               -o -iname "*.vtt" -o -iname "*.mka" -o -iname "*.sup" \) \
            2>/dev/null | wc -l)
        if [[ $real_media -gt 0 ]]; then
            echo "[BLOCKED_HAS_MEDIA] 旧目录仍含真实媒体文件($real_media 个):"
            echo "                    $blocked_dir"
            (( ISSUES++ )) || true
        elif [[ $total_files -gt 0 || $total_dirs -gt 0 ]]; then
            echo "[BLOCKED_METADATA]  旧目录仍存在（仅元数据/子目录，可用 clean_orphaned_emby_metadata.sh 清理）:"
            echo "                    $blocked_dir  (${total_files} 文件, ${total_dirs} 子目录)"
            (( ISSUES++ )) || true
        else
            echo "[BLOCKED_EMPTY]     旧目录仍存在但已为空（可手动 rmdir）:"
            echo "                    $blocked_dir"
            (( ISSUES++ )) || true
        fi
        (( blocked_found++ )) || true
    fi
done < "$BLOCKED_DIRS_FILE"
[[ $blocked_found -eq 0 ]] && echo "  (无残留旧目录)"

# ── 汇总 ─────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "扫描 DB 记录: $TOTAL 条 | 日志目标: $LOGGED_COUNT 条 | 曾残留目录: $BLOCKED_COUNT 条"
echo "正常: $OK  | 问题: $ISSUES"
echo "============================================================"
if [[ $ISSUES -eq 0 ]]; then
    echo ""
    echo "✓ 未发现路径合理性问题"
    exit 0
else
    exit 1
fi
