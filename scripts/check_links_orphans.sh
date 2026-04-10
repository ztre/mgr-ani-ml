#!/usr/bin/env bash
# check_links_orphans.sh
# 扫描 links 目标目录（TV/Movie 媒体库硬链接目录），找出文件系统中存在
# 但 DB 无对应 target_path 记录的媒体文件（孤立链接）。
# 反向也检查：DB 中有记录但 links 目录下找不到文件（broken record）。
#
# 遍历算法镜像 scanner._collect_video_leaf_dirs：
#   深度优先栈遍历，找到含视频文件的叶目录后不再向下深入；
#   结果按 <root>/<show_dir>/... 的第一级 show 目录汇总报告。
#
# 用法:
#   bash check_links_orphans.sh [选项]
#
# 选项:
#   --db <path>          SQLite 数据库路径（默认: 同 check_media_path_sanity.sh）
#   --tv-root <dir>      TV 媒体库根目录（覆盖从 DB 读取）
#   --movie-root <dir>   Movie 媒体库根目录（覆盖从 DB 读取）
#   --remap <old:new>    路径前缀重映射（可多次）：DB 中存储的路径前缀 → 实际 FS 路径前缀
#                        与 check_media_path_sanity.sh 方向一致
#                        例如: --remap /media/links:/mnt/user/media/links
#   --max-depth <n>      从媒体库根目录向下扫描的最大层数（默认: 8）
#   --no-attachments     只检查视频文件，跳过字幕/音轨等附件
#   --skip-orphan-check  跳过"DB 有记录但文件不存在"的反向检查
#   --verbose            列出每一个通过的文件（OK）
#
# 示例:
#   bash check_links_orphans.sh
#   bash check_links_orphans.sh --tv-root /mnt/user/media/links/anime_tv
#   bash check_links_orphans.sh \
#       --remap /media/links/anime_tv:/mnt/user/media/links/anime_tv \
#       --remap /media/links/anime_movie:/mnt/user/media/links/anime_movie
#   bash check_links_orphans.sh --verbose

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ── 默认值 ────────────────────────────────────────────────────────────────────
DB_PATH=""
TV_ROOT=""
MOVIE_ROOT=""
MAX_DEPTH=8
NO_ATTACHMENTS=0
SKIP_ORPHAN_CHECK=0
REMAP_FROM=()
REMAP_TO=()

# ── 参数解析 ──────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)              DB_PATH="$2";      shift 2 ;;
        --tv-root)         TV_ROOT="$2";      shift 2 ;;
        --movie-root)      MOVIE_ROOT="$2";   shift 2 ;;
        --max-depth)       MAX_DEPTH="$2";    shift 2 ;;
        --no-attachments)  NO_ATTACHMENTS=1;  shift   ;;
        --skip-orphan-check) SKIP_ORPHAN_CHECK=1; shift ;;
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
        *) echo "未知参数: $1" >&2; exit 1 ;;
    esac
done

# ── 确定 DB 路径 ───────────────────────────────────────────────────────────────
if [[ -z "$DB_PATH" ]]; then
    if   [[ -f "/app/data/anime_media.db" ]];      then DB_PATH="/app/data/anime_media.db"
    elif [[ -f "$PROJECT_ROOT/anime_media.db" ]];  then DB_PATH="$PROJECT_ROOT/anime_media.db"
    else
        echo "错误: 找不到数据库文件，请用 --db 指定路径" >&2; exit 1
    fi
fi

[[ -f "$DB_PATH" ]] || { echo "错误: 数据库不存在: $DB_PATH" >&2; exit 1; }
command -v sqlite3 &>/dev/null || { echo "错误: sqlite3 未安装" >&2; exit 1; }

# ── 从 DB 读取媒体库根目录（若未手动传入）──────────────────────────────────
if [[ -z "$TV_ROOT" ]]; then
    TV_ROOT="$(sqlite3 "$DB_PATH" \
        "SELECT target FROM sync_groups WHERE enabled=1 AND source_type='tv' LIMIT 1;" 2>/dev/null || true)"
fi
if [[ -z "$MOVIE_ROOT" ]]; then
    MOVIE_ROOT="$(sqlite3 "$DB_PATH" \
        "SELECT target FROM sync_groups WHERE enabled=1 AND source_type='movie' LIMIT 1;" 2>/dev/null || true)"
fi

# ── 路径重映射函数 ────────────────────────────────────────────────────────────
# DB 中存储的路径 → 实际文件系统路径
remap_path() {
    local path="$1"
    local i
    for (( i=0; i<${#REMAP_FROM[@]}; i++ )); do
        if [[ "$path" == "${REMAP_FROM[$i]}"* ]]; then
            echo "${REMAP_TO[$i]}${path#${REMAP_FROM[$i]}}"
            return
        fi
    done
    echo "$path"
}

# 实际文件系统路径 → DB 中存储的路径（remap_path 的反向）
unmap_path() {
    local path="$1"
    local i
    for (( i=0; i<${#REMAP_TO[@]}; i++ )); do
        if [[ "$path" == "${REMAP_TO[$i]}"* ]]; then
            echo "${REMAP_FROM[$i]}${path#${REMAP_TO[$i]}}"
            return
        fi
    done
    echo "$path"
}

# ── 打印配置信息 ──────────────────────────────────────────────────────────────
echo "数据库:        $DB_PATH"
echo "TV 根目录:     $(remap_path "${TV_ROOT:-(未配置)}")"
echo "Movie 根目录:  $(remap_path "${MOVIE_ROOT:-(未配置)}")"
echo "最大扫描深度:  $MAX_DEPTH"
if [[ ${#REMAP_FROM[@]} -gt 0 ]]; then
    echo "路径重映射:"
    for (( i=0; i<${#REMAP_FROM[@]}; i++ )); do
        echo "               ${REMAP_FROM[$i]}  →  ${REMAP_TO[$i]}"
    done
fi
echo ""

# 确定实际要扫描的根目录（经过 remap 映射）
REAL_TV_ROOT="$(remap_path "$TV_ROOT")"
REAL_MOVIE_ROOT="$(remap_path "$MOVIE_ROOT")"

# ── 媒体扩展名定义（镜像 scanner.VIDEO_EXTS / ATTACHMENT_EXTS）──────────────
VIDEO_EXTS=("mkv" "mp4" "avi" "mov" "webm" "flv" "ts" "m2ts")
ATTACH_EXTS=("ass" "srt" "ssa" "vtt" "mka" "sup" "idx" "sub")

# 构造 find 的 -iname 表达式
build_find_names() {
    local first=1
    for ext in "${VIDEO_EXTS[@]}"; do
        [[ $first -eq 0 ]] && printf ' -o '
        printf -- '-iname "*.%s"' "$ext"
        first=0
    done
    if [[ $NO_ATTACHMENTS -eq 0 ]]; then
        for ext in "${ATTACH_EXTS[@]}"; do
            printf ' -o -iname "*.%s"' "$ext"
        done
    fi
}

# ── 从 DB 加载所有已知目标路径 ────────────────────────────────────────────────
# 同时包含 scraped / manual_fixed / pending_manual（有 target_path 的）
TMP_DB_PATHS="$(mktemp)"
TMP_FS_PATHS="$(mktemp)"
TMP_REPORT="$(mktemp)"
trap 'rm -f "$TMP_DB_PATHS" "$TMP_FS_PATHS" "$TMP_REPORT"' EXIT

echo "正在加载 DB 记录..."
sqlite3 "$DB_PATH" \
    "SELECT target_path FROM media_records
     WHERE target_path IS NOT NULL AND target_path != ''
     ORDER BY target_path;" \
    > "$TMP_DB_PATHS"

DB_TOTAL=$(wc -l < "$TMP_DB_PATHS")
echo "DB 中 target_path 记录数: $DB_TOTAL"
echo ""

# 将 DB 路径全部 remap 为实际 FS 路径，存入关联数组
declare -A DB_REAL_PATHS   # 实际FS路径 -> 1
while IFS= read -r db_path; do
    [[ -z "$db_path" ]] && continue
    real="$(remap_path "$db_path")"
    DB_REAL_PATHS["$real"]=1
done < "$TMP_DB_PATHS"

# ── 深度优先目录遍历：镜像 _collect_video_leaf_dirs ─────────────────────────
# 扫描指定根目录，返回含视频文件的叶目录（子目录一旦含视频就不再向下）
# 输出格式: show_dir|leaf_dir （show_dir = 根目录直接子目录）
collect_leaf_dirs() {
    local root="$1"
    local depth_limit="$2"
    [[ -d "$root" ]] || return 0

    # 使用 find 找到所有含视频文件的目录（maxdepth 限制）
    # 然后取每个目录的 show-level（root 的直接子目录）
    find "$root" -mindepth 1 -maxdepth "$depth_limit" -type d 2>/dev/null | sort
}

# ── 扫描文件系统 ──────────────────────────────────────────────────────────────
scan_root() {
    local root="$1"
    local label="$2"
    [[ -d "$root" ]] || { echo "  $label 根目录不存在，跳过: $root"; return; }

    echo "扫描 $label: $root"

    local found_files=0
    local orphan_files=0
    local ok_files=0
    # 关联数组：show_dir -> 孤立文件数
    declare -A show_orphan_count
    declare -A show_total_count

    # 使用 find 遍历所有媒体文件（深度优先顺序）
    # 镜像 _collect_media_files_under_dir 的 rglob 逻辑
    # 构造 find 扩展名条件：视频（始终包含）+ 附件（可选）
    local find_names=( \
        -iname "*.mkv" -o -iname "*.mp4" -o -iname "*.avi" \
        -o -iname "*.mov" -o -iname "*.webm" -o -iname "*.flv" \
        -o -iname "*.ts"  -o -iname "*.m2ts" \
    )
    if [[ $NO_ATTACHMENTS -eq 0 ]]; then
        find_names+=( \
            -o -iname "*.ass" -o -iname "*.srt" -o -iname "*.ssa" \
            -o -iname "*.vtt" -o -iname "*.mka" -o -iname "*.sup" \
            -o -iname "*.idx" -o -iname "*.sub" \
        )
    fi

    while IFS= read -r -d '' fs_file; do
        # 跳过 _is_ignored_name 对应的文件（bdmv/menu/sample/scan/disc 等）
        # 镜像 scanner.IGNORED_TOKENS = {"bdmv","menu","sample","scan","disc","iso","font"}
        fname="${fs_file##*/}"
        stem="${fname%.*}"
        stem_lower="${stem,,}"
        skip=0
        for tok in bdmv menu sample scan disc iso font; do
            if [[ "$stem_lower" == *"$tok"* ]]; then skip=1; break; fi
        done
        [[ $skip -eq 1 ]] && continue

        (( found_files++ )) || true

        # 计算 show 目录（root 直接子目录）
        rel="${fs_file#$root/}"
        show_dir_name="${rel%%/*}"
        show_dir="$root/$show_dir_name"

        show_total_count["$show_dir"]=$(( ${show_total_count["$show_dir"]:-0} + 1 ))

        if [[ -n "${DB_REAL_PATHS[$fs_file]+x}" ]]; then
            (( ok_files++ )) || true
        else
            (( orphan_files++ )) || true
            show_orphan_count["$show_dir"]=$(( ${show_orphan_count["$show_dir"]:-0} + 1 ))
            echo "  [ORPHAN_FILE]  $fs_file" >> "$TMP_REPORT"
        fi

        # 同时记录扫描到的文件，用于反向检查
        echo "$fs_file" >> "$TMP_FS_PATHS"

    done < <(
        find "$root" -maxdepth "$MAX_DEPTH" -type f \( "${find_names[@]}" \) \
            -print0 2>/dev/null | sort -z
    )

    # ── 输出本根目录的汇总 ────────────────────────────────────────────────────
    echo "  文件总数: $found_files  |  DB 已记录: $ok_files  |  孤立文件: $orphan_files"
    echo ""

    if [[ $orphan_files -gt 0 ]]; then
        echo "  ── 孤立文件按 show 目录汇总 ──────────────────────────────────"
        # 遍历有孤立文件的 show 目录（null 分隔避免路径含空格被词拆分）
        while IFS= read -r -d '' show_dir; do
            orphan_cnt="${show_orphan_count[$show_dir]:-0}"
            total_cnt="${show_total_count[$show_dir]:-0}"
            show_name="${show_dir##*/}"

            if [[ $orphan_cnt -eq $total_cnt ]]; then
                echo "  [SHOW_ALL_ORPHAN]  $show_name  ($orphan_cnt 个文件全部未记录)"
            else
                echo "  [SHOW_PARTIAL]     $show_name  ($orphan_cnt/$total_cnt 个文件未记录)"
            fi

            # 打印该 show 目录下的具体孤立文件
            if [[ -f "$TMP_REPORT" ]]; then
                grep -F "  [ORPHAN_FILE]  $show_dir/" "$TMP_REPORT" | \
                    sed "s|  \[ORPHAN_FILE\]  $root/||" | \
                    sed 's/^/                 /' || true
            fi
            echo ""
        done < <(printf '%s\0' "${!show_orphan_count[@]}" | sort -z)
    else
        echo "  (该目录下无孤立文件)"
        echo ""
    fi

    return 0
}

# ── 执行扫描 ──────────────────────────────────────────────────────────────────
TOTAL_ORPHANS=0
TOTAL_BROKEN=0

echo "════════════════════════════════════════════════════════════"
echo " 正向检查：links 文件系统 → DB 记录"
echo "════════════════════════════════════════════════════════════"
echo ""

# 分别统计孤立文件总数
if [[ -n "$REAL_TV_ROOT" ]]; then
    scan_root "$REAL_TV_ROOT" "TV"
fi
if [[ -n "$REAL_MOVIE_ROOT" && "$REAL_MOVIE_ROOT" != "$REAL_TV_ROOT" ]]; then
    scan_root "$REAL_MOVIE_ROOT" "Movie"
fi

if [[ -z "$REAL_TV_ROOT" && -z "$REAL_MOVIE_ROOT" ]]; then
    echo "未配置任何媒体库根目录，无法扫描。请用 --tv-root 或 --movie-root 指定。" >&2
    exit 1
fi

# 统计孤立文件总数
if [[ -f "$TMP_REPORT" ]]; then
    TOTAL_ORPHANS=$(grep -c '^\s*\[ORPHAN_FILE\]' "$TMP_REPORT" 2>/dev/null || true)
fi

# ── 反向检查：DB 有记录但文件系统找不到 ──────────────────────────────────────
if [[ $SKIP_ORPHAN_CHECK -eq 0 ]]; then
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo " 反向检查：DB 记录 → links 文件系统（仅检查 tv/movie 根目录下）"
    echo "════════════════════════════════════════════════════════════"
    echo ""

    # 只检查属于已配置根目录的 DB 记录（避免误报其他路径）
    broken_count=0
    while IFS= read -r db_path; do
        [[ -z "$db_path" ]] && continue
        real_path="$(remap_path "$db_path")"

        # 判断是否属于已配置的根目录
        in_scope=0
        [[ -n "$REAL_TV_ROOT"    && "$real_path" == "$REAL_TV_ROOT"/*    ]] && in_scope=1
        [[ -n "$REAL_MOVIE_ROOT" && "$real_path" == "$REAL_MOVIE_ROOT"/* ]] && in_scope=1
        [[ $in_scope -eq 0 ]] && continue

        if [[ ! -f "$real_path" ]]; then
            echo "  [BROKEN_RECORD]  DB 有记录但文件不存在:"
            echo "                   db_path   = $db_path"
            [[ "$real_path" != "$db_path" ]] && echo "                   real_path = $real_path"
            (( broken_count++ )) || true
        fi
    done < "$TMP_DB_PATHS"

    TOTAL_BROKEN=$broken_count
    if [[ $broken_count -eq 0 ]]; then
        echo "  (所有 DB 记录文件均存在)"
    fi
fi

# ── 汇总 ─────────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo " 汇总"
echo "════════════════════════════════════════════════════════════"
echo "  孤立文件（links 有但 DB 无）: $TOTAL_ORPHANS"
[[ $SKIP_ORPHAN_CHECK -eq 0 ]] && echo "  损坏记录（DB 有但 links 无）: $TOTAL_BROKEN"
echo ""

TOTAL_ISSUES=$(( TOTAL_ORPHANS + TOTAL_BROKEN ))
if [[ $TOTAL_ISSUES -eq 0 ]]; then
    echo "  ✓ links 目录与 DB 记录完全同步，无孤立文件"
    exit 0
else
    echo "  ✗ 发现 $TOTAL_ISSUES 处不一致"
    exit 1
fi
