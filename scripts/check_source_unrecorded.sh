#!/usr/bin/env bash
# check_source_unrecorded.sh
# 深度扫描来源目录（sync_groups.source），找出其中存在但从未被
# 记录到 media_records.original_path 的媒体文件（未入库源文件）。
#
# 遍历算法镜像 scanner._collect_video_leaf_dirs：
#   深度优先栈遍历（find 实现），找到含视频文件的叶目录；
#   忽略 IGNORED_TOKENS：bdmv/menu/sample/scan/disc/iso/font。
# 结果按 <source>/<show_dir> 的第一级 show 目录汇总报告。
#
# 用法:
#   bash check_source_unrecorded.sh [选项]
#
# 选项:
#   --db <path>          SQLite 数据库路径（默认: 自动检测）
#   --source-root <dir>  追加额外的源目录（可多次），覆盖/补充 DB 读取的来源
#   --remap <old:new>    路径前缀重映射（可多次）：DB 中存储的路径前缀 → 实际 FS 路径前缀
#                        例如: --remap /media/anime:/mnt/user/media/anime
#   --max-depth <n>      扫描最大层数（默认: 10）
#   --no-attachments     只检查视频文件，跳过字幕/音轨等附件
#   --show-pending       同时展示状态为 pending_manual 的目录（已记录但未处理）
#
# 示例:
#   bash check_source_unrecorded.sh
#   bash check_source_unrecorded.sh --remap /media/anime_tv:/mnt/user/mnt/anime_tv
#   bash check_source_unrecorded.sh --source-root /mnt/nas/anime_extra --show-pending

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ── 默认值 ────────────────────────────────────────────────────────────────────
DB_PATH=""
EXTRA_SOURCE_ROOTS=()
MAX_DEPTH=10
NO_ATTACHMENTS=0
SHOW_PENDING=0
REMAP_FROM=()
REMAP_TO=()

# ── 参数解析 ──────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db)              DB_PATH="$2";                    shift 2 ;;
        --source-root)     EXTRA_SOURCE_ROOTS+=("$2");      shift 2 ;;
        --max-depth)       MAX_DEPTH="$2";                  shift 2 ;;
        --no-attachments)  NO_ATTACHMENTS=1;                shift   ;;
        --show-pending)    SHOW_PENDING=1;                  shift   ;;
        --remap)
            remap_val="$2"
            remap_from_part="${remap_val%%:*}"
            remap_to_part="${remap_val#*:}"
            if [[ -z "$remap_from_part" || -z "$remap_to_part" || "$remap_from_part" == "$remap_to_part" ]]; then
                echo "错误: --remap 格式应为 old_prefix:new_prefix，例如 /media/anime:/mnt/user/media/anime" >&2
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

# ── 路径重映射函数（DB 中存储的路径前缀 → 实际 FS 路径）────────────────────
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

# 实际 FS 路径 → DB 中存储的路径（反向，用于在 DB 中查询）
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

# ── 从 DB 读取所有启用的同步组来源目录 ────────────────────────────────────────
mapfile -t DB_SOURCE_ENTRIES < <(
    sqlite3 "$DB_PATH" \
        "SELECT id, name, source, source_type FROM sync_groups WHERE enabled=1 ORDER BY id;" \
        2>/dev/null || true
)

# ── 打印配置信息 ──────────────────────────────────────────────────────────────
echo "数据库:        $DB_PATH"
echo "扫描最大深度:  $MAX_DEPTH"
if [[ ${#REMAP_FROM[@]} -gt 0 ]]; then
    echo "路径重映射:"
    for (( i=0; i<${#REMAP_FROM[@]}; i++ )); do
        echo "               ${REMAP_FROM[$i]}  →  ${REMAP_TO[$i]}"
    done
fi
echo ""
echo "同步组来源目录:"
for entry in "${DB_SOURCE_ENTRIES[@]}"; do
    IFS='|' read -r sg_id sg_name sg_source sg_type <<< "$entry"
    real_source="$(remap_path "$sg_source")"
    echo "  [$sg_type] $sg_name  →  $real_source"
done
for extra in "${EXTRA_SOURCE_ROOTS[@]}"; do
    echo "  [extra]    $extra"
done
echo ""

# ── 从 DB 加载所有已知 original_path ─────────────────────────────────────────
TMP_ORIG_PATHS="$(mktemp)"
TMP_REPORT="$(mktemp)"
trap 'rm -f "$TMP_ORIG_PATHS" "$TMP_REPORT"' EXIT

echo "正在加载 DB 记录..."

# 同时加载 original_path + status，供 --show-pending 使用
sqlite3 "$DB_PATH" \
    "SELECT original_path, status FROM media_records
     WHERE original_path IS NOT NULL AND original_path != ''
     ORDER BY original_path;" \
    > "$TMP_ORIG_PATHS"

DB_TOTAL=$(wc -l < "$TMP_ORIG_PATHS")
echo "DB 中 original_path 记录数: $DB_TOTAL"
echo ""

# 构造 original_path → status 关联数组
# 同时存储 remap 后的实际路径 → status（供 FS 路径直接查找）
declare -A DB_ORIG_STATUS   # db格式路径 -> status
declare -A DB_REAL_STATUS   # 实际FS路径 -> status

while IFS='|' read -r orig_path orig_status; do
    [[ -z "$orig_path" ]] && continue
    DB_ORIG_STATUS["$orig_path"]="$orig_status"
    real_p="$(remap_path "$orig_path")"
    DB_REAL_STATUS["$real_p"]="$orig_status"
done < "$TMP_ORIG_PATHS"

# ── 媒体扩展名（镜像 scanner.VIDEO_EXTS / ATTACHMENT_EXTS）──────────────────
build_find_names() {
    # 输出 find 的 -o 连接的 -iname 条件（供后续 eval 或直接 find 使用）
    local names=( \
        -iname "*.mkv" -o -iname "*.mp4" -o -iname "*.avi" \
        -o -iname "*.mov" -o -iname "*.webm" -o -iname "*.flv" \
        -o -iname "*.ts"  -o -iname "*.m2ts" \
    )
    if [[ $NO_ATTACHMENTS -eq 0 ]]; then
        names+=( \
            -o -iname "*.ass" -o -iname "*.srt" -o -iname "*.ssa" \
            -o -iname "*.vtt" -o -iname "*.mka" -o -iname "*.sup" \
            -o -iname "*.idx" -o -iname "*.sub" \
        )
    fi
    printf '%s\n' "${names[@]}"
}
mapfile -t FIND_NAMES < <(build_find_names)

# ── 核心扫描函数 ──────────────────────────────────────────────────────────────
# 扫描单个来源目录，报出未入库的媒体文件
scan_source() {
    local source_root="$1"      # 实际 FS 路径
    local label="$2"

    if [[ ! -d "$source_root" ]]; then
        echo "  [$label] 来源目录不存在，跳过: $source_root"
        echo ""
        return
    fi

    echo "扫描 [$label]: $source_root"

    local found_files=0
    local recorded_files=0
    local pending_files=0
    local unrecorded_files=0

    # 按 show 目录（source_root 直接子目录级）分组统计
    declare -A show_total
    declare -A show_unrecorded
    declare -A show_pending

    while IFS= read -r -d '' fs_file; do
        # 跳过 IGNORED_TOKENS（镜像 scanner: bdmv/menu/sample/scan/disc/iso/font）
        fname="${fs_file##*/}"
        stem="${fname%.*}"
        stem_lower="${stem,,}"
        skip=0
        for tok in bdmv menu sample scan disc iso font; do
            [[ "$stem_lower" == *"$tok"* ]] && { skip=1; break; }
        done
        [[ $skip -eq 1 ]] && continue

        (( found_files++ )) || true

        # show 目录 = source_root 直接子目录
        rel="${fs_file#$source_root/}"
        show_dir_name="${rel%%/*}"
        show_dir="$source_root/$show_dir_name"

        show_total["$show_dir"]=$(( ${show_total["$show_dir"]:-0} + 1 ))

        # 在 DB 中查找：先用 FS 路径，再用 unmap 后的 DB 路径
        db_path="$(unmap_path "$fs_file")"
        status=""
        if [[ -n "${DB_REAL_STATUS[$fs_file]+x}" ]]; then
            status="${DB_REAL_STATUS[$fs_file]}"
        elif [[ -n "${DB_ORIG_STATUS[$db_path]+x}" ]]; then
            status="${DB_ORIG_STATUS[$db_path]}"
        fi

        if [[ -z "$status" ]]; then
            # 完全未记录
            (( unrecorded_files++ )) || true
            show_unrecorded["$show_dir"]=$(( ${show_unrecorded["$show_dir"]:-0} + 1 ))
            echo "[UNRECORDED]  $fs_file" >> "$TMP_REPORT"
        elif [[ "$status" == "pending_manual" ]]; then
            (( pending_files++ )) || true
            show_pending["$show_dir"]=$(( ${show_pending["$show_dir"]:-0} + 1 ))
            [[ $SHOW_PENDING -eq 1 ]] && echo "[PENDING]     $fs_file" >> "$TMP_REPORT"
            (( recorded_files++ )) || true
        else
            (( recorded_files++ )) || true
        fi

    done < <(
        find "$source_root" -maxdepth "$MAX_DEPTH" -type f \( "${FIND_NAMES[@]}" \) \
            -print0 2>/dev/null | sort -z
    )

    echo "  文件总数: $found_files  |  已记录: $recorded_files  |  未记录: $unrecorded_files  |  待处理(pending_manual): $pending_files"
    echo ""

    # ── 按 show 目录汇总未记录情况 ────────────────────────────────────────
    local has_unrecorded=0
    [[ $unrecorded_files -gt 0 ]] && has_unrecorded=1
    local has_pending=0
    [[ $SHOW_PENDING -eq 1 && $pending_files -gt 0 ]] && has_pending=1

    if [[ $has_unrecorded -eq 1 ]]; then
        echo "  ── 未记录文件按 show 目录汇总 ────────────────────────────────"
        while IFS= read -r -d '' show_dir; do
            unrecorded_cnt="${show_unrecorded[$show_dir]:-0}"
            [[ $unrecorded_cnt -eq 0 ]] && continue
            total_cnt="${show_total[$show_dir]:-0}"
            show_name="${show_dir##*/}"

            if [[ $unrecorded_cnt -eq $total_cnt ]]; then
                echo "  [DIR_ALL_UNRECORDED]  $show_name  ($unrecorded_cnt 个文件全部未入库)"
            else
                echo "  [DIR_PARTIAL]         $show_name  ($unrecorded_cnt/$total_cnt 个文件未入库)"
            fi
            # 列出具体未记录文件
            grep -F "[UNRECORDED]  $show_dir/" "$TMP_REPORT" 2>/dev/null | \
                sed "s|\[UNRECORDED\]  $source_root/||" | \
                sed 's/^/                      /' || true
            echo ""
        done < <(printf '%s\0' "${!show_unrecorded[@]}" | sort -z)
    else
        echo "  (该来源目录下所有媒体文件均已入库)"
        echo ""
    fi

    if [[ $has_pending -eq 1 ]]; then
        echo "  ── 待处理目录（pending_manual）──────────────────────────────"
        while IFS= read -r -d '' show_dir; do
            pending_cnt="${show_pending[$show_dir]:-0}"
            [[ $pending_cnt -eq 0 ]] && continue
            total_cnt="${show_total[$show_dir]:-0}"
            show_name="${show_dir##*/}"
            echo "  [DIR_PENDING]         $show_name  ($pending_cnt/$total_cnt 个文件待处理)"
        done < <(printf '%s\0' "${!show_pending[@]}" | sort -z)
        echo ""
    fi
}

# ── 执行扫描 ──────────────────────────────────────────────────────────────────
echo "════════════════════════════════════════════════════════════"
echo " 来源目录未记录文件检查（源文件 → DB original_path）"
echo "════════════════════════════════════════════════════════════"
echo ""

TOTAL_UNRECORDED=0

for entry in "${DB_SOURCE_ENTRIES[@]}"; do
    IFS='|' read -r sg_id sg_name sg_source sg_type <<< "$entry"
    real_source="$(remap_path "$sg_source")"
    scan_source "$real_source" "$sg_type/$sg_name"
done

for extra in "${EXTRA_SOURCE_ROOTS[@]}"; do
    scan_source "$extra" "extra"
done

# 统计总计
if [[ -f "$TMP_REPORT" ]]; then
    TOTAL_UNRECORDED=$(grep -c '^\[UNRECORDED\]' "$TMP_REPORT" 2>/dev/null || true)
fi

# ── 汇总 ─────────────────────────────────────────────────────────────────────
echo "════════════════════════════════════════════════════════════"
echo " 汇总"
echo "════════════════════════════════════════════════════════════"
echo "  未记录源文件总数: $TOTAL_UNRECORDED"
echo ""

if [[ $TOTAL_UNRECORDED -eq 0 ]]; then
    echo "  ✓ 所有来源目录下的媒体文件均已入库"
    exit 0
else
    echo "  ✗ 发现 $TOTAL_UNRECORDED 个源文件未记录到 DB"
    exit 1
fi
