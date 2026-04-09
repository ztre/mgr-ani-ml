#!/usr/bin/env bash
# clean_orphaned_emby_metadata.sh
# 清理 Emby/Jellyfin 刮削产生的、没有对应媒体文件的孤立元数据文件
#
# 会删除：
#   - 分集 NFO（如 "番剧 - S01E01.nfo"）—— 若同目录下无同名视频文件
#   - 分集缩略图（如 "番剧 - S01E01-thumb.jpg"）—— 同上
#
# 会保留（show/season 级别，无需对应媒体文件）：
#   tvshow.nfo  season.nfo  movie.nfo
#   poster.jpg  fanart.jpg  banner.jpg  backdrop.jpg
#
# 用法:
#   bash clean_orphaned_emby_metadata.sh [--dry-run] <目标根目录>
#
# 示例:
#   bash clean_orphaned_emby_metadata.sh --dry-run /media/links/anime_tv
#   bash clean_orphaned_emby_metadata.sh /media/links/anime_tv

set -euo pipefail

VIDEO_EXTS=(mkv mp4 avi mov webm flv m4v)

DRY_RUN=0
TARGET_DIR=""

for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        *) TARGET_DIR="$arg" ;;
    esac
done

if [[ -z "$TARGET_DIR" ]]; then
    echo "用法: $0 [--dry-run] <目标根目录>" >&2
    exit 1
fi

if [[ ! -d "$TARGET_DIR" ]]; then
    echo "错误: 目录不存在: $TARGET_DIR" >&2
    exit 1
fi

removed=0

has_video_peer() {
    local stem="$1"
    local dir="$2"
    local ext
    for ext in "${VIDEO_EXTS[@]}"; do
        if [[ -f "$dir/$stem.$ext" ]]; then
            return 0
        fi
    done
    return 1
}

while IFS= read -r -d '' file; do
    dir="$(dirname "$file")"
    base="$(basename "$file")"
    lower="${base,,}"  # bash 4+ lowercase

    # 跳过 show/season 级别文件，这些不需要对应媒体文件
    case "$lower" in
        tvshow.nfo|season.nfo|movie.nfo|poster.jpg|fanart.jpg|banner.jpg|backdrop.jpg)
            continue
            ;;
    esac

    # 提取对应的媒体文件 stem
    stem=""
    if [[ "$lower" == *-thumb.jpg || "$lower" == *-thumb.png ]]; then
        # "番剧 - S01E01-thumb.jpg" -> "番剧 - S01E01"
        stem="${base%-thumb.*}"
    elif [[ "$lower" == *.nfo ]]; then
        stem="${base%.*}"
    else
        continue
    fi

    # 若同目录有对应视频文件则保留
    if has_video_peer "$stem" "$dir"; then
        continue
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        echo "[DRY-RUN] 孤立元数据: $file"
    else
        echo "删除孤立元数据: $file"
        rm -f -- "$file"
    fi
    (( removed++ )) || true

done < <(find "$TARGET_DIR" -type f \( -iname "*.nfo" -o -iname "*-thumb.jpg" -o -iname "*-thumb.png" \) -print0)

if [[ $DRY_RUN -eq 1 ]]; then
    echo "完成 (dry-run): 发现 $removed 个孤立元数据文件，未实际删除"
else
    echo "完成: 已删除 $removed 个孤立元数据文件"
fi
