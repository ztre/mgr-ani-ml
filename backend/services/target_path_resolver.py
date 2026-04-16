from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path

from ..api.logs import append_log
from .linker import is_same_inode
from .media_content_types import EXTRA_CATEGORIES, SPECIAL_CATEGORIES
from .parser import ParseResult, _looks_like_technical_extra_label, _looks_title_like_fallback_label, extract_strong_extra_fallback_label
from .renamer import compute_movie_target_path, compute_tv_target_path

SPECIAL_ANCHOR_CATEGORIES = {
    "special",
    "oped",
    "making",
    "trailer",
    "preview",
    "pv",
    "cm",
    "teaser",
    "character_pv",
}


@dataclass
class TargetDecision:
    parse_result: ParseResult
    dst_path: Path | None
    status: str = "ok"
    reason: str | None = None
    file_type: str | None = None
    remapped: bool = False
    assigned_by_allocator: bool = False
    item: dict | None = None
    original_special_label: str | None = None
    merged_tags: list[str] = field(default_factory=list)
    dropped_tags: list[str] = field(default_factory=list)
    used_conflict_enrichment: bool = False


@dataclass
class AttachmentAnchorResult:
    """附件锚点解析结果。

    follow_mode 表示 scanner 选择的附件分流策略：
    - mainline-follow: 普通主视频附件，允许逐集 / 同 stem / 弱兜底。
    - extra-follow: 明确 extra 附件，只允许精确命中 extra 相关锚点。
    - special-follow: 明确 special/OPED 附件，只允许精确命中特典锚点。

    layer 用来记录最终命中的层级，方便日志直接判断是“精确命中”
    还是“弱兜底命中”。
    """

    anchor_dst: Path | None
    follow_mode: str
    layer: str | None = None


def build_attachment_target_from_anchor(anchor_dst: Path, parse_result: ParseResult, ext: str, src_path: Path | None = None) -> Path:
    # anchor_dst 已是重命名后的干净目标路径，直接以其 stem 为基准，
    # 保证字幕/音轨文件名与正片完全一致，播放器才能自动加载。
    base = anchor_dst.stem
    if src_path is not None:
        suffix = _build_attachment_source_suffix(src_path, parse_result, base, anchor_dir=anchor_dst.parent)
        if suffix:
            base = f"{base} {suffix}".strip()
    lang = parse_result.subtitle_lang or ""
    return anchor_dst.with_name(f"{base}{lang}{ext}")


def deduplicate_target_or_raise(seen_targets: dict[Path, Path], src_path: Path, dst_path: Path) -> None:
    prev = seen_targets.get(dst_path)
    if prev is None:
        seen_targets[dst_path] = src_path
        return
    if prev != src_path:
        raise ValueError(f"多个源文件映射到同一目标: {prev.name} / {src_path.name}")


def resolve_attachment_follow_target(src_path: Path, parse_result: ParseResult, dir_runtime: dict | None) -> Path | None:
    return resolve_attachment_follow_target_details(src_path, parse_result, dir_runtime).anchor_dst


def resolve_attachment_follow_target_details(
    src_path: Path,
    parse_result: ParseResult,
    dir_runtime: dict | None,
    follow_mode: str | None = None,
) -> AttachmentAnchorResult:
    """分层为附件寻找锚点。

    设计重点：
    - 把“精确索引命中”和“弱兜底回退”拆开。
    - 只有普通主视频附件允许走最近锚点/目录默认锚点。
    - extra/special 附件一旦没有命中精确索引，直接返回 None，
      避免再次出现 task41x2 那种“不同附件被兜底绑到同一个 SPxx”的回归。
    """
    mode = follow_mode or _infer_attachment_follow_mode(parse_result)
    if dir_runtime is None:
        return AttachmentAnchorResult(None, mode)

    # 精确索引：用于“有明确信号就必须命中正确视频”的路径。
    by_parent_ep = dir_runtime.get("video_anchor_by_parent_episode", {})
    by_parent_stem = dir_runtime.get("video_anchor_by_parent_stem", {})
    by_parent_special = dir_runtime.get("video_anchor_by_parent_special", {})
    by_parent_special_prefix = dir_runtime.get("video_anchor_by_parent_special_prefix", {})
    special_by_raw = dir_runtime.get("special_target_by_raw_label", {})
    special_by_raw_multi = dir_runtime.get("special_targets_by_raw_label_multi", {})
    special_by_fine = dir_runtime.get("special_target_by_fine_label", {})

    # 弱回退索引：只给普通主视频附件使用，extra/special 严禁走到这里。
    by_parent_recent = dir_runtime.get("video_anchor_recent_by_parent", {})
    by_parent = dir_runtime.get("video_anchor_by_parent", {})
    parent_key = str(src_path.parent)

    def _result(anchor: Path | None, layer: str | None = None) -> AttachmentAnchorResult:
        return AttachmentAnchorResult(anchor, mode, layer)

    def _lookup_label_anchor(layer_prefix: str) -> AttachmentAnchorResult | None:
        raw_key = _build_special_raw_label_key(parent_key, parse_result.extra_label)
        if raw_key is not None:
            raw_target = special_by_raw.get(raw_key, "__missing__")
            if raw_target is None:
                candidates = list(special_by_raw_multi.get(raw_key) or [])
                best = _pick_best_anchor_candidate(candidates, src_path, parse_result)
                if best is not None:
                    return _result(best, f"{layer_prefix}:raw-multi")
                fine_key = _build_special_fine_label_key(
                    parent_key,
                    raw_label=parse_result.extra_label,
                    source_text=src_path.stem,
                    category=parse_result.extra_category,
                )
                if fine_key is not None:
                    fine_target = special_by_fine.get(fine_key, "__missing__")
                    if fine_target not in {"__missing__", None}:
                        return _result(fine_target, f"{layer_prefix}:fine")
                return None
            if raw_target != "__missing__":
                return _result(raw_target, f"{layer_prefix}:raw")

        fine_key = _build_special_fine_label_key(
            parent_key,
            raw_label=parse_result.extra_label,
            source_text=src_path.stem,
            category=parse_result.extra_category,
        )
        if fine_key is not None:
            fine_target = special_by_fine.get(fine_key, "__missing__")
            if fine_target is None:
                raw_key = _build_special_raw_label_key(parent_key, parse_result.extra_label)
                candidates = list(special_by_raw_multi.get(raw_key) or [])
                best = _pick_best_anchor_candidate(candidates, src_path, parse_result)
                if best is not None:
                    return _result(best, f"{layer_prefix}:fine-multi")
                return None
            if fine_target not in {"__missing__", None}:
                return _result(fine_target, f"{layer_prefix}:fine")
        return None

    def _lookup_special_anchor(layer_prefix: str) -> AttachmentAnchorResult | None:
        prefix_ambiguous = False
        if parse_result.extra_category in SPECIAL_CATEGORIES:
            prefix = _special_label_prefix(parse_result.extra_category, parse_result.extra_label)
            if prefix:
                prefix_value = by_parent_special_prefix.get(
                    (parent_key, parse_result.extra_category, prefix.upper()),
                    "__missing__",
                )
                if prefix_value is None:
                    prefix_ambiguous = True
                if prefix_value != "__missing__":
                    return _result(prefix_value, f"{layer_prefix}:prefix")
        special_token = _special_anchor_token(parse_result, src_path, original_label=parse_result.extra_label)
        category = str(parse_result.extra_category or "")
        seen_special_keys: set[tuple[str, str, str, str]] = set()
        special_keys: list[tuple[tuple[str, str, str, str], str]] = []
        if special_token and category in SPECIAL_ANCHOR_CATEGORIES:
            for stem_key, stem_layer in (
                (_normalize_media_stem(src_path.stem), "token"),
                (_normalize_attachment_stem(src_path.stem), "token-attachment-stem"),
            ):
                if not stem_key:
                    continue
                special_key = (parent_key, stem_key, category, special_token)
                if special_key in seen_special_keys:
                    continue
                seen_special_keys.add(special_key)
                special_keys.append((special_key, stem_layer))
        for special_key, stem_layer in special_keys:
            candidates = list(by_parent_special.get(special_key) or [])
            if len(candidates) == 1:
                return _result(candidates[0], f"{layer_prefix}:{stem_layer}")
            if len(candidates) > 1:
                append_log(f"WARNING: 特典附件锚点冲突: {src_path.name} -> {len(candidates)} 个候选")
                return _result(None, f"{layer_prefix}:ambiguous")
        if prefix_ambiguous:
            return _result(None, f"{layer_prefix}:prefix-ambiguous")
        return None

    def _lookup_episode_anchor(layer_prefix: str) -> AttachmentAnchorResult | None:
        ep_key = _normalized_episode_key(parse_result, src_path, src_path.name)
        if ep_key is None:
            return None
        candidate = by_parent_ep.get((parent_key, int(ep_key)))
        if not candidate:
            return None
        # 标题一致性校验：防止混合目录下跨作品附件通过 episode 序号绑到错误锚点。
        # 若附件 stem 能在 by_parent_stem 中命中不同锚点，说明与当前 episode 锚点
        # 来自不同作品 → 拒绝。若附件 stem 在 by_parent_stem 中完全找不到，但当前
        # 目录已有其他视频注册了 stem，同样说明附件来自未知作品 → 拒绝。
        att_stem_keys = [
            _normalize_attachment_stem(src_path.stem),
            _normalize_media_stem(src_path.stem),
        ]
        stem_hit: Path | None = next(
            (
                by_parent_stem[(parent_key, sk)]
                for sk in att_stem_keys
                if sk and (parent_key, sk) in by_parent_stem
            ),
            None,
        )
        if stem_hit is not None:
            if stem_hit != candidate:
                return None  # 附件 stem 指向其他锚点 → 跨作品，拒绝
        elif any(k[0] == parent_key for k in by_parent_stem):
            return None  # 目录有 stem 注册但本附件不匹配任何一条 → 跨作品，拒绝
        return _result(candidate, f"{layer_prefix}:episode")

    def _lookup_stem_anchor(layer_prefix: str) -> AttachmentAnchorResult | None:
        stem_candidates = [_normalize_media_stem(src_path.stem), _normalize_attachment_stem(src_path.stem)]
        for stem_key in stem_candidates:
            if not stem_key:
                continue
            candidate = by_parent_stem.get((parent_key, stem_key))
            if candidate:
                return _result(candidate, f"{layer_prefix}:stem")
        return None

    def _lookup_weak_fallback(layer_prefix: str) -> AttachmentAnchorResult | None:
        recent = by_parent_recent.get(parent_key)
        if recent:
            return _result(recent, f"{layer_prefix}:recent")
        candidate = by_parent.get(parent_key)
        if candidate:
            return _result(candidate, f"{layer_prefix}:parent")
        return None

    if mode == "special-follow":
        # 中文说明：
        # special-follow 只允许走“明确可解释”的精确索引：
        # 1. raw/fine label 命中；
        # 2. special token 命中。
        #
        # 这里故意不再允许 stem 回退。因为 task41x2 的核心回归之一，就是
        # 字幕/音轨附件在 raw label 或特典 token 已经出现歧义时，仍然被 stem
        # 或 recent 锚点“猜中”，最后错误绑到某一个 SPxx 上。对于特典附件，
        # 宁可返回 unmatched，也不能把含糊匹配伪装成成功。
        for lookup in (_lookup_label_anchor("special"), _lookup_special_anchor("special")):
            if lookup is not None and lookup.anchor_dst is not None:
                return lookup
        return _result(None)

    if mode == "extra-follow":
        for lookup in (_lookup_stem_anchor("extra"), _lookup_label_anchor("extra"), _lookup_episode_anchor("extra")):
            if lookup is not None and lookup.anchor_dst is not None:
                return lookup
        return _result(None)

    ep_anchor = _lookup_episode_anchor("mainline")
    if ep_anchor is not None and ep_anchor.anchor_dst is not None:
        return ep_anchor
    stem_anchor = _lookup_stem_anchor("mainline")
    if stem_anchor is not None and stem_anchor.anchor_dst is not None:
        return stem_anchor
    # 如果附件自带 episode 编号信号，则跳过弱回退：
    # episode/stem 均未命中说明附件来自其他作品，弱回退只会造成跨作品污染。
    if _normalized_episode_key(parse_result, src_path, src_path.name) is None:
        weak = _lookup_weak_fallback("mainline")
        if weak is not None and weak.anchor_dst is not None:
            return weak
    return _result(None)


def _infer_attachment_follow_mode(parse_result: ParseResult) -> str:
    if parse_result.extra_category in SPECIAL_ANCHOR_CATEGORIES:
        return "special-follow"
    if parse_result.extra_category in EXTRA_CATEGORIES:
        return "extra-follow"
    return "mainline-follow"


def apply_readable_suffix_for_unnumbered_extra(parse_result: ParseResult, src_path: Path, dir_runtime: dict | None) -> ParseResult:
    if not _needs_readable_suffix(parse_result):
        return parse_result
    if parse_result.extra_category in SPECIAL_CATEGORIES:
        parse_result = _assign_sequential_episode_for_unnumbered(parse_result, src_path, dir_runtime)
    suffix = _build_readable_suffix(src_path, parse_result)
    if not suffix:
        suffix = f"h{hashlib.md5(src_path.name.encode('utf-8')).hexdigest()[:6]}"
    candidate = _ensure_unique_suffix_label(suffix, src_path, parse_result, dir_runtime)
    return parse_result._replace(extra_label=candidate)


def apply_special_indexing(parse_result: ParseResult, src_path: Path, dir_runtime: dict | None, force_next: bool = False) -> ParseResult:
    if dir_runtime is None:
        return parse_result
    category = parse_result.extra_category
    if category not in (SPECIAL_CATEGORIES | EXTRA_CATEGORIES):
        return parse_result
    label = parse_result.extra_label or ""
    prefix = _special_label_prefix(category, label)
    used = dir_runtime.setdefault("special_used", {}).setdefault((category, prefix), set())
    idx = None if force_next else _extract_stable_label_index(label)
    version_suffix = ""
    m_ver = re.search(r"\b(?:ver\.?\s*\d+|v\d+)\b", label, re.I)
    if m_ver:
        version_suffix = m_ver.group(0).strip()
    if idx is None or force_next:
        next_idx = 1
        while next_idx in used:
            next_idx += 1
        if force_next:
            append_log(f"INFO: 特典重映射: {prefix}{next_idx:02d} ← {src_path.name}")
        idx = next_idx
    used.add(idx)
    new_label = _compose_indexed_extra_label(prefix, idx, label)
    if version_suffix and version_suffix.lower() not in new_label.lower():
        new_label = f"{new_label} {version_suffix}"
    updated = parse_result._replace(extra_label=new_label)
    if category in SPECIAL_CATEGORIES:
        updated = updated._replace(episode=idx)
    return updated


def compute_target_path(
    media_type: str,
    target_root: Path,
    parse_result: ParseResult,
    tmdb_id: int | None,
    ext: str,
    src_filename: str,
) -> Path:
    if media_type == "tv":
        return compute_tv_target_path(target_root, parse_result, tmdb_id, ext, src_filename=src_filename)
    return compute_movie_target_path(target_root, parse_result, tmdb_id, ext)


def _has_explicit_season_token(name: str) -> bool:
    text = str(name or "")
    if re.search(r"\bS\d{1,2}\b", text, re.I):
        return True
    if re.search(r"\bSeason\s*\d{1,2}\b", text, re.I):
        return True
    if re.search(r"\b\d{1,2}(?:st|nd|rd|th)\s+Season\b", text, re.I):
        return True
    if re.search(r"第\s*\d{1,2}\s*季", text):
        return True
    return False


def _apply_resolved_season_zero_final_guard(
    parse_result: ParseResult,
    *,
    media_type: str,
    src_path: Path,
    resolved_season: int | None,
) -> ParseResult:
    if media_type != "tv" or resolved_season != 0:
        return parse_result
    if parse_result.season == 0:
        return parse_result
    if _has_explicit_season_token(src_path.name):
        return parse_result
    append_log(
        f"INFO: [season00 final-guard] 目标解析前强制季号归零: "
        f"file={src_path.name!r}, candidate={parse_result.season} -> 0"
    )
    return parse_result._replace(season=0)


def resolve_final_target(
    *,
    src_path: Path,
    parse_result: ParseResult,
    media_type: str,
    target_root: Path,
    tmdb_id: int | None,
    ext: str,
    seen_targets: dict[Path, Path],
    dir_runtime: dict | None,
    assignments: dict,
    item_map: dict,
    is_attachment: bool = False,
    resolved_season: int | None = None,
    deduplicate_func=deduplicate_target_or_raise,
) -> TargetDecision:
    parse_result = _apply_resolved_season_zero_final_guard(
        parse_result,
        media_type=media_type,
        src_path=src_path,
        resolved_season=resolved_season,
    )
    original_special_label = parse_result.extra_label
    source_diff_tags = _extract_distinguish_source_tags(src_path.stem)
    remapped = False
    assigned_by_allocator = False
    item = item_map.get(str(src_path))
    assigned_index = assignments.get(str(src_path))
    if item and assigned_index is not None:
        prefix = item.get("prefix") or _special_label_prefix(parse_result.extra_category, parse_result.extra_label)
        preferred = item.get("preferred")
        if preferred and int(preferred) != int(assigned_index):
            append_log(f"INFO: 特典重映射: {prefix}{int(preferred):02d} → {prefix}{int(assigned_index):02d} ← {src_path.name}")
            remapped = True
        seed = item.get("final_label_seed") or parse_result.extra_label
        parse_result = parse_result._replace(extra_label=_compose_indexed_extra_label(prefix, int(assigned_index), seed))
        if parse_result.extra_category in SPECIAL_CATEGORIES:
            parse_result = parse_result._replace(episode=int(assigned_index))
        assigned_by_allocator = True
    else:
        before_label = parse_result.extra_label
        parse_result = apply_special_indexing(parse_result, src_path, dir_runtime)
        remapped = bool(before_label and parse_result.extra_label and before_label != parse_result.extra_label)
    parse_result = _apply_variant_label_hint(parse_result, src_path)
    dst_path = compute_target_path(
        media_type=media_type,
        target_root=target_root,
        parse_result=parse_result,
        tmdb_id=tmdb_id,
        ext=ext,
        src_filename=src_path.name,
    )
    merged_tags_once: list[str] = []
    dropped_diffs_once: list[str] = []
    used_conflict_enrichment = False

    def _recalc_target() -> Path:
        return compute_target_path(
            media_type=media_type,
            target_root=target_root,
            parse_result=parse_result,
            tmdb_id=tmdb_id,
            ext=ext,
            src_filename=src_path.name,
        )

    conflict_now = dst_path in seen_targets or (dst_path.exists() and not is_same_inode(src_path, dst_path))
    if conflict_now and source_diff_tags:
        new_label, merged_tags, dropped_diffs = _merge_distinguish_tags_into_label(parse_result.extra_label, source_diff_tags)
        if merged_tags and new_label and new_label != parse_result.extra_label:
            parse_result = parse_result._replace(extra_label=new_label)
            dst_path = _recalc_target()
            merged_tags_once = merged_tags
            dropped_diffs_once = dropped_diffs
            used_conflict_enrichment = True

    if parse_result.extra_category is None:
        attempts = 0
        used_conflict_enrichment = False
        while attempts < 3:
            conflict = dst_path in seen_targets or (dst_path.exists() and not is_same_inode(src_path, dst_path))
            if not conflict:
                break
            if not used_conflict_enrichment and source_diff_tags:
                new_label, merged_tags, _dropped = _merge_distinguish_tags_into_label(parse_result.extra_label, source_diff_tags)
                if merged_tags and new_label and new_label != parse_result.extra_label:
                    parse_result = parse_result._replace(extra_label=new_label)
                    dst_path = _recalc_target()
                    used_conflict_enrichment = True
                    attempts += 1
                    continue
                used_conflict_enrichment = True
            # 第三次尝试：若有 version_tag 且尚未并入 extra_label，则以版本标签区分
            if attempts == 2:
                vtag = getattr(parse_result, "version_tag", None)
                if vtag and (not parse_result.extra_label or vtag not in parse_result.extra_label):
                    new_label = f"{parse_result.extra_label}.{vtag}" if parse_result.extra_label else vtag
                    parse_result = parse_result._replace(extra_label=new_label)
                    dst_path = _recalc_target()
                    attempts += 1
                    continue
            return TargetDecision(
                parse_result=parse_result,
                dst_path=dst_path,
                status="pending",
                reason="main video target conflict unresolved",
                file_type="video",
            )
    elif parse_result.extra_category in (SPECIAL_CATEGORIES | EXTRA_CATEGORIES):
        attempts = 0
        merged_tags_once = []
        dropped_diffs_once = []
        used_conflict_enrichment = False
        while True:
            conflict = dst_path in seen_targets or (dst_path.exists() and not is_same_inode(src_path, dst_path))
            if not conflict:
                break
            if (not used_conflict_enrichment) and source_diff_tags:
                new_label, merged_tags, dropped_diffs = _merge_distinguish_tags_into_label(parse_result.extra_label, source_diff_tags)
                if merged_tags and new_label and new_label != parse_result.extra_label:
                    parse_result = parse_result._replace(extra_label=new_label)
                    dst_path = _recalc_target()
                    merged_tags_once = merged_tags
                    dropped_diffs_once = dropped_diffs
                    used_conflict_enrichment = True
                    continue
                if dropped_diffs and not dropped_diffs_once:
                    dropped_diffs_once = dropped_diffs
                used_conflict_enrichment = True
            if assigned_by_allocator:
                break
            remapped = True
            parse_result = apply_special_indexing(parse_result, src_path, dir_runtime, force_next=True)
            dst_path = _recalc_target()
            attempts += 1
            if attempts > 50:
                return TargetDecision(
                    parse_result=parse_result,
                    dst_path=dst_path,
                    status="skip",
                    reason="extra target conflict after remap attempts",
                    file_type="special" if parse_result.extra_category in SPECIAL_CATEGORIES else "extra",
                    remapped=remapped,
                    assigned_by_allocator=assigned_by_allocator,
                    item=item,
                    original_special_label=original_special_label,
                    merged_tags=merged_tags_once,
                    dropped_tags=dropped_diffs_once,
                    used_conflict_enrichment=used_conflict_enrichment,
                )
    file_type = "attachment" if is_attachment else ("special" if parse_result.extra_category in SPECIAL_CATEGORIES else ("extra" if parse_result.extra_category in EXTRA_CATEGORIES else "video"))
    if dst_path.exists() and not is_same_inode(src_path, dst_path):
        if is_attachment:
            return TargetDecision(
                parse_result=parse_result,
                dst_path=dst_path,
                status="skip",
                reason="attachment target occupied by other source",
                file_type="attachment",
                remapped=remapped,
                assigned_by_allocator=assigned_by_allocator,
                item=item,
                original_special_label=original_special_label,
                merged_tags=merged_tags_once,
                dropped_tags=dropped_diffs_once,
                used_conflict_enrichment=used_conflict_enrichment,
            )
        if parse_result.extra_category in (SPECIAL_CATEGORIES | EXTRA_CATEGORIES):
            return TargetDecision(
                parse_result=parse_result,
                dst_path=dst_path,
                status="skip",
                reason="extra target occupied",
                file_type=file_type,
                remapped=remapped,
                assigned_by_allocator=assigned_by_allocator,
                item=item,
                original_special_label=original_special_label,
                merged_tags=merged_tags_once,
                dropped_tags=dropped_diffs_once,
                used_conflict_enrichment=used_conflict_enrichment,
            )
        return TargetDecision(
            parse_result=parse_result,
            dst_path=dst_path,
            status="pending",
            reason="target occupied by other source",
            file_type="video",
            remapped=remapped,
            assigned_by_allocator=assigned_by_allocator,
            item=item,
            original_special_label=original_special_label,
            merged_tags=merged_tags_once,
            dropped_tags=dropped_diffs_once,
            used_conflict_enrichment=used_conflict_enrichment,
        )
    try:
        deduplicate_func(seen_targets, src_path, dst_path)
    except ValueError:
        if is_attachment:
            return TargetDecision(
                parse_result=parse_result,
                dst_path=dst_path,
                status="skip",
                reason="attachment target duplicated in current batch",
                file_type="attachment",
                remapped=remapped,
                assigned_by_allocator=assigned_by_allocator,
                item=item,
                original_special_label=original_special_label,
                merged_tags=merged_tags_once,
                dropped_tags=dropped_diffs_once,
                used_conflict_enrichment=used_conflict_enrichment,
            )
        if parse_result.extra_category in (SPECIAL_CATEGORIES | EXTRA_CATEGORIES):
            return TargetDecision(
                parse_result=parse_result,
                dst_path=dst_path,
                status="skip",
                reason="extra target duplicated in current batch",
                file_type=file_type,
                remapped=remapped,
                assigned_by_allocator=assigned_by_allocator,
                item=item,
                original_special_label=original_special_label,
                merged_tags=merged_tags_once,
                dropped_tags=dropped_diffs_once,
                used_conflict_enrichment=used_conflict_enrichment,
            )
        return TargetDecision(
            parse_result=parse_result,
            dst_path=dst_path,
            status="pending",
            reason="batch target conflict",
            file_type="video",
            remapped=remapped,
            assigned_by_allocator=assigned_by_allocator,
            item=item,
            original_special_label=original_special_label,
            merged_tags=merged_tags_once,
            dropped_tags=dropped_diffs_once,
            used_conflict_enrichment=used_conflict_enrichment,
        )
    return TargetDecision(
        parse_result=parse_result,
        dst_path=dst_path,
        status="ok",
        file_type=file_type,
        remapped=remapped,
        assigned_by_allocator=assigned_by_allocator,
        item=item,
        original_special_label=original_special_label,
        merged_tags=merged_tags_once,
        dropped_tags=dropped_diffs_once,
        used_conflict_enrichment=used_conflict_enrichment,
    )


def _build_special_raw_label_key(parent_key: str, raw_label: str | None) -> tuple[str, str] | None:
    """构造目录级 raw label 键。

    raw label 只表达“附件/视频表面上属于哪个标签”，适合作为第一层精确索引，
    但一旦同名目标超过一个，就必须继续细分，不能把它当作最终兜底条件。
    """
    label = re.sub(r"\s+", " ", str(raw_label or "")).strip()
    if not label:
        return None
    return (parent_key, label.upper())


def _build_special_fine_label_key(parent_key: str, raw_label: str | None, source_text: str, category: str | None) -> tuple[str, str, str] | None:
    """构造更细粒度的标签键。

    fine label = raw label + 源文件差异 token。
    它的职责不是扩大匹配范围，而是在 raw label 已经发生歧义时继续收敛。
    """
    coarse = _build_special_raw_label_key(parent_key, raw_label)
    if coarse is None:
        return None
    token = _build_source_diff_token(source_text, category=category)
    if not token:
        return None
    return (coarse[0], coarse[1], token)


def _build_source_diff_token(source_text: str, category: str | None) -> str:
    tags = _extract_distinguish_source_tags(source_text)
    token_parts: list[str] = []
    if tags:
        token_parts.extend(tags)
    scene = _extract_scene_fragment(source_text)
    if scene:
        normalized_scene = _normalize_special_anchor_token(scene, category)
        if normalized_scene:
            token_parts.append(normalized_scene)
    if not token_parts:
        normalized = ""
        if category in SPECIAL_CATEGORIES:
            normalized = _normalize_attachment_stem(source_text)
        if not normalized:
            normalized = _normalize_special_anchor_token(source_text, category)
        if normalized:
            token_parts.append(normalized)
    if not token_parts:
        return ""
    joined = "|".join(x for x in token_parts if x)
    return joined[:96]


def _build_special_anchor_key(parse_result: ParseResult, src_path: Path, original_label: str | None = None) -> tuple[str, str, str, str] | None:
    category = str(parse_result.extra_category or "")
    if category not in SPECIAL_ANCHOR_CATEGORIES:
        return None
    parent_key = str(src_path.parent)
    stem_key = _normalize_media_stem(src_path.stem)
    if not stem_key:
        return None
    token = _special_anchor_token(parse_result, src_path, original_label=original_label)
    if not token:
        return None
    return (parent_key, stem_key, category, token)


def _special_anchor_token(parse_result: ParseResult, src_path: Path, original_label: str | None = None) -> str:
    label = str(original_label or parse_result.extra_label or "").strip()
    if not label:
        label = _extract_scene_fragment(src_path.stem) or _normalize_suffix_text(src_path.stem)
    return _normalize_special_anchor_token(label, parse_result.extra_category)


def _normalize_special_anchor_token(text: str, category: str | None) -> str:
    s = str(text or "")
    s = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", s)
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"[^\w\u4e00-\u9fff\- ]+", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip().lower()
    if not s:
        return ""
    cat = str(category or "")
    if cat == "making":
        s = re.sub(r"^making\s*\d{0,3}\b", "", s, flags=re.I)
    elif cat == "special":
        s = re.sub(r"^sp\s*\d{0,3}\b", "", s, flags=re.I)
    elif cat == "oped":
        s = re.sub(r"^(?:op|ed)\s*\d{0,3}\b", "", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip(" -_")


def _extract_scene_fragment(text: str) -> str | None:
    s = str(text or "")
    if not s:
        return None
    m = re.search(r"\b(Scene)\s*[-_ ]*([0-9]{1,3}(?:[A-Za-z]{1,8})?)\b", s, re.I)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    return None


def _normalized_episode_key(parse_result: ParseResult, src_path: Path, filename: str) -> int | None:
    if parse_result.extra_category in SPECIAL_CATEGORIES:
        season = parse_result.season or _extract_season_from_path(src_path) or 1
        index = parse_result.episode or _extract_episode_from_filename_loose(filename) or 1
        if parse_result.extra_category == "oped":
            label = str(parse_result.extra_label or "").upper()
            if label.startswith("ED"):
                return season * 100 + int(index * 2)
            return season * 100 + int(index * 2 - 1)
        return season * 100 + int(index)
    ep = parse_result.episode or _extract_episode_from_filename_loose(filename)
    return int(ep) if ep is not None else None


def _extract_episode_from_filename_loose(filename: str) -> int | None:
    stem = Path(filename).stem
    patterns = [
        r"\b\d{1,2}\s*[xX]\s*(\d{1,3})\b",
        r"\[(\d{2,3})\]",
        r"\((\d{1,3})\)",
        r"\bEP?(\d{2,3})\b",
        r"\bEP[_\-\s]*(\d{1,3})\b",
        r"\bEpisode\s*(\d{1,3})\b",
        r"\bEp\s*(\d{1,3})\b",
        r"\bE(\d{1,3})\b",
        r"第\s*(\d{1,3})\s*[集话話]",
    ]
    for pattern in patterns:
        m = re.search(pattern, stem, re.I)
        if not m:
            continue
        ep = int(m.group(1))
        if 1 <= ep <= 999 and ep not in {2160, 1080, 720, 480, 265, 264}:
            return ep
    return None


def _extract_season_from_path(src_path: Path) -> int | None:
    for part in src_path.parts:
        m1 = re.match(r"^season\s*(\d{1,2})$", part, re.I)
        if m1:
            return int(m1.group(1))
        m2 = re.match(r"^s(\d{1,2})$", part, re.I)
        if m2:
            return int(m2.group(1))
        m3 = re.match(r"^第\s*(\d{1,2})\s*季$", part)
        if m3:
            return int(m3.group(1))
    return None


def _normalize_media_stem(stem: str) -> str:
    text = str(stem or "")
    text = re.sub(r"[\[\]\(\)\{\}]", " ", text)
    # 先剥除声道标（如 5.1ch、6ch）和版本标（如 v2、v3）——必须在 dot→space 之前
    text = re.sub(r"\b\d+(?:\.\d)?ch\b", " ", text, flags=re.I)
    text = re.sub(r"\bv\d+(?:\.\d+)?\b", " ", text, flags=re.I)
    text = text.replace(".", " ").replace("_", " ")
    text = re.sub(r"\b(?:2160p|1080p|720p|480p|4k)\b", " ", text, flags=re.I)
    text = re.sub(r"\b(?:x264|x265|h264|h265|hevc|av1|hi10p|ma10p)\b", " ", text, flags=re.I)
    text = re.sub(r"\b(?:flac|aac|ac3|dts|ddp\d?\.\d?)\b", " ", text, flags=re.I)
    # 点替换后再次剥除版本标（如 _v2 转换为空格后的 v2）
    text = re.sub(r"\bv\d+(?:\.\d+)?\b", " ", text, flags=re.I)
    text = re.sub(r"\b(?:webrip|web-dl|bdrip|bluray|remux)\b", " ", text, flags=re.I)
    text = re.sub(r"\b(?:chs|cht|sc|tc|gb|big5|jpn|jp|ja|jap|eng|en|zh[-_ ]?(?:cn|tw))\b", " ", text, flags=re.I)
    text = re.sub(r"[^\w\u4e00-\u9fff\- ]+", " ", text, flags=re.U)
    text = re.sub(r"\s+", " ", text).strip().lower()
    if not text:
        return ""
    return text[:96]


def _normalize_attachment_stem(stem: str) -> str:
    s = str(stem or "")
    if not s:
        return ""
    # 剥除语言后缀：.SC .TC .jap .eng 等，以及带版本标变体如 .sc_v2 .tc_v2
    s = re.sub(r"\.(?:jap|eng|sc|tc|chs|cht|jpsc|jptc|zh[\-_]?(?:cn|tw)|ass|srt|ssa|vtt)(?:_v\d+)?\s*$", "", s, flags=re.I)
    s = re.sub(r"\b(?:sc|tc|chs|cht|jpsc|jptc|zh[\-_]?(?:cn|tw))\s*$", "", s, flags=re.I)
    # 剥离以点或空格分隔的中日文语言名称后缀，如 .简体中文 .繁體中文 .日文 .中文 等
    s = re.sub(
        r"[.\s_](?:简体中文|繁體中文|繁体中文|简中|繁中|简体|繁体|繁體|中文|日文|日语|日本語|日本语|英文|英语)\s*$",
        "",
        s,
    )
    return _normalize_media_stem(s)


def _pick_best_anchor_candidate(candidates: list[Path], src_path: Path, parse_result: ParseResult) -> Path | None:
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    tags = _extract_distinguish_source_tags(src_path.stem)
    label_hint = _normalize_media_stem(parse_result.extra_label or "")
    scene_hint = _normalize_media_stem(_extract_scene_fragment(src_path.stem) or "")
    stem_hint = _normalize_attachment_stem(src_path.stem)
    best: Path | None = None
    best_score = -1
    tie = False
    for candidate in candidates:
        candidate_key = _normalize_media_stem(candidate.stem)
        score = 0
        if stem_hint and stem_hint in candidate_key:
            score += 5
        if label_hint and label_hint in candidate_key:
            score += 4
        if scene_hint and scene_hint in candidate_key:
            score += 3
        for tag in tags:
            token = _normalize_media_stem(tag)
            if token and token in candidate_key:
                score += 2
        if score > best_score:
            best_score = score
            best = candidate
            tie = False
        elif score == best_score:
            tie = True
    if best_score <= 0 or tie:
        return None
    return best


def _build_attachment_source_suffix(src_path: Path, parse_result: ParseResult, anchor_stem: str, anchor_dir: Path | None = None) -> str:
    if parse_result.extra_category in (SPECIAL_CATEGORIES | EXTRA_CATEGORIES):
        return ""
    # 仅在 TV Season 目录（Season XX）下才追加集号区分后缀；movie 上下文无集号。
    _in_season_dir = anchor_dir is not None and bool(re.match(r"season\s+\d+", anchor_dir.name, re.I))
    anchor_key = _normalize_media_stem(anchor_stem)
    # 预计算：附件自身标题的规范化（把连字符当空格），用于过滤"系列/篇名"标签。
    # 若 token 只是标题的一部分（如 Raihousha-hen 来自系列副标题），不应作为区分后缀。
    # 注意：scanner 会用 context["title"]（中文）覆盖 parse_result.title，
    # 导致 title 里不含原始的 ASCII 副标题词。因此同时从 src_path.stem 的非 bracket 主干
    # 里提取一份原始标题空间作为补充检查来源。
    _src_title_space = _normalize_media_stem(parse_result.title or "").replace("-", " ")
    _stem_title_raw = re.sub(r"\[[^\]]*\]|\([^)]*\)", " ", src_path.stem)
    _stem_title_space = _normalize_media_stem(_stem_title_raw).replace("-", " ")
    parts: list[str] = []
    attachment_token = _extract_attachment_distinguish_token(src_path.stem)
    if attachment_token:
        parts.append(attachment_token)
    scene = _extract_scene_fragment(src_path.stem)
    if scene:
        parts.append(re.sub(r"\s+", " ", scene).strip())
    ep = _extract_episode_from_filename_loose(src_path.name)
    if _in_season_dir and ep is not None and (parse_result.episode is None or int(ep) == int(parse_result.episode)):
        parts.append(f"E{int(ep):02d}")
    for tag in _extract_distinguish_source_tags(src_path.stem):
        # 版本标（v2/v3/ver.2 等）不加入附件区分后缀：
        # 视频目标名已剥除版本，附件名须与视频一致才能被播放器自动加载。
        if re.fullmatch(r"ver\.?\s*\d+|v\d+(?:\.\d+)?", tag.strip(), re.I):
            continue
        parts.append(tag)
    seen: set[str] = set()
    for part in parts:
        token = re.sub(r"\s+", " ", str(part or "")).strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized = _normalize_media_stem(token)
        if normalized and normalized in anchor_key:
            continue
        # 若 token 来自附件文件名的标题部分（系列名/篇名），它对区分没有意义，跳过。
        # 双重检查：parse_result.title（可能已被上下文覆盖为中文）和从 stem 直接提取的原始标题。
        norm_token_sp = normalized.replace("-", " ")
        if normalized and (
            (_src_title_space and norm_token_sp in _src_title_space)
            or (_stem_title_space and norm_token_sp in _stem_title_space)
        ):
            continue
        return token[:40]
    return ""


def _extract_attachment_distinguish_token(stem: str) -> str:
    tokens = re.findall(r"\[([^\]]+)\]", str(stem or ""))
    for raw in tokens:
        text = re.sub(r"\s+", " ", str(raw or "")).strip()
        if not text:
            continue
        lower = text.lower()
        if re.search(r"(?:2160p|1080p|720p|x26[45]|h26[45]|hevc|av1|ma\d+p(?:[_-]?\d{3,4}p)?|main\d+|hi\d+p+|flac|aac|ac3|dts|vcb|studio)", lower):
            continue
        text = re.sub(r"\((?:NC|On\s*Air)\s*Ver\.?\)", "", text, flags=re.I).strip(" -_")
        if not text:
            continue
        if re.search(r"[a-zA-Z\u4e00-\u9fff]", text) and re.search(r"\d", text):
            return text[:40]
    return ""


def _strip_attachment_base_noise(stem: str) -> str:
    s = re.sub(r"\s+", " ", str(stem or "")).strip()
    if not s:
        return ""
    s = _strip_release_group_fragments(s)
    s = re.sub(r"\s+(?:ma\d+p[_\s-]*\d{3,4}p|\d{3,4}p)\b", "", s, flags=re.I)
    s = re.sub(r"\s+(?:x26[45]|h26[45]|hevc|av1|flac|aac|ac3|dts)\b", "", s, flags=re.I)
    # 清理剥离质量标签后遗留的尾随分隔符（如 "Title - 1080p" → "Title -" → "Title"）
    s = re.sub(r"[\s\-_]+$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _special_label_prefix(category: str | None, label: str | None) -> str:
    label = str(label or "").strip()
    if category == "oped":
        upper = label.upper()
        if upper.startswith("NCED") or upper.startswith("ED"):
            return "ED"
        if upper.startswith("NCOP") or upper.startswith("OP"):
            return "OP"
        for token in ("NCOP", "NCED", "OP", "ED"):
            if re.search(rf"\b{token}\b", label, re.I):
                return "ED" if token == "NCED" else ("OP" if token == "NCOP" else token)
        return "OP"
    if category == "special":
        upper = label.upper()
        if upper.startswith("OVA"):
            return "OVA"
        if upper.startswith("OAD"):
            return "OAD"
        if upper.startswith("SP") or upper.startswith("SPECIAL"):
            return "SP"
        for token in ("OVA", "OAD", "SP", "SPECIAL"):
            if re.search(rf"\b{token}\s*0*\d{{0,3}}\b", label, re.I):
                return "SP" if token == "SPECIAL" else token
        return "SP"
    if category == "pv":
        return "PV"
    if category == "cm":
        return "CM"
    if category == "preview":
        if re.search(r"\bWEBPREVIEW\b", label, re.I):
            return "WebPreview"
        return "Preview"
    if category == "character_pv":
        return "CharacterPV"
    if category == "trailer":
        return "Trailer"
    if category == "teaser":
        return "Teaser"
    if category == "mv":
        return "MV"
    if category == "making":
        return "Making"
    if category == "interview":
        return "Interview"
    if category == "bdextra":
        return "BDExtra"
    return "Extra"


def _extract_stable_label_index(label: str | None) -> int | None:
    s = re.sub(r"\s+", " ", str(label or "")).strip()
    if not s:
        return None
    patterns = [
        r"\b(?:SP|OVA|OAD|OAV|SPECIAL)\s*0*(\d{1,3})\b",
        r"\b(?:OP|ED|NCOP|NCED)\s*0*(\d{1,3})\b",
        r"\b(?:PV|CM|Preview|Trailer|Teaser|CharacterPV|WebPreview|MV|Interview|Making|BDExtra)\s*0*(\d{1,3})\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, s, re.I)
        if not m:
            continue
        val = int(m.group(1))
        if 1 <= val <= 999:
            return val
    return None


def _compose_indexed_extra_label(prefix: str, idx: int, seed_label: str | None) -> str:
    base = f"{prefix}{int(idx):02d}"
    suffix = _extract_preserved_extra_suffix(seed_label, prefix)
    if not suffix:
        return base
    return f"{base} {suffix}"


def _extract_preserved_extra_suffix(label: str | None, prefix: str) -> str:
    s = re.sub(r"\s+", " ", str(label or "")).strip()
    if not s:
        return ""
    s = re.sub(rf"^\s*{re.escape(prefix)}\s*0*\d{{1,3}}\b", "", s, flags=re.I)
    s = re.sub(rf"^\s*{re.escape(prefix)}\b", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    return _sanitize_extra_suffix(s)


def _merge_distinguish_tags_into_label(label: str | None, tags: list[str], max_len: int = 120) -> tuple[str | None, list[str], list[str]]:
    base = re.sub(r"\s+", " ", str(label or "")).strip()
    if not tags:
        return (base or None), [], []
    lower_base = base.lower()
    merged: list[str] = []
    dropped: list[str] = []
    for tag in tags:
        token = re.sub(r"\s+", " ", str(tag or "")).strip()
        if not token:
            continue
        if lower_base and token.lower() in lower_base:
            continue
        candidate = f"{base} {token}".strip() if base else token
        if len(candidate) > max_len:
            dropped.append(token)
            continue
        base = candidate
        lower_base = base.lower()
        merged.append(token)
    return (base or None), merged, dropped


def _extract_distinguish_source_tags(text: str) -> list[str]:
    s = str(text or "")
    if not s:
        return []
    patterns = [
        (r"\bNC(?:OP|ED)?(?=\d|\b)", "NC Ver"),
        (r"\bOn\s*Air\s*Ver\.?\b", "On Air Ver"),
        (r"\bTrue\s*Birth\s*Edition\b", "True Birth Edition"),
        (r"\b([A-Za-z][A-Za-z0-9]{1,24})\s*[-_ ]hen\b", r"\1-hen"),
        (r"\bEX\s*Season\s*0*(\d{1,2})\b", r"EX Season \1"),
        (r"\bOriginal\s*Staff\s*Credit\s*Ver\.?\b", "Original Staff Credit Ver"),
        (r"\b(?:ver\.?\s*\d+|v\d+)\b", None),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for pattern, template in patterns:
        for m in re.finditer(pattern, s, re.I):
            if template:
                tag = template
                if r"\1" in template and m.lastindex and m.lastindex >= 1:
                    tag = template.replace(r"\1", m.group(1))
                tag = re.sub(r"\s+", " ", tag).strip()
            else:
                tag = re.sub(r"\s+", " ", str(m.group(0) or "")).strip().rstrip(".")
            if not tag:
                continue
            key = tag.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(tag)
    return out[:6]


def _apply_variant_label_hint(parse_result: ParseResult, src_path: Path) -> ParseResult:
    if parse_result.extra_category not in (SPECIAL_CATEGORIES | EXTRA_CATEGORIES):
        return parse_result
    tags = _extract_distinguish_source_tags(src_path.stem)
    if not tags:
        return parse_result
    label = re.sub(r"\s+", " ", str(parse_result.extra_label or "")).strip()
    if not label:
        return parse_result
    # 预计算标题空间，用于过滤系列篇名标签（如 Raihousha-hen）。
    _src_title_space = _normalize_media_stem(parse_result.title or "").replace("-", " ")
    _stem_title_raw = re.sub(r"\[[^\]]*\]|\([^)]*\)", " ", src_path.stem)
    _stem_title_space = _normalize_media_stem(_stem_title_raw).replace("-", " ")
    out = label
    lower = out.lower()
    for tag in tags:
        token = re.sub(r"\s+", " ", str(tag or "")).strip()
        if not token:
            continue
        if token.lower() in lower:
            continue
        # 若 token 来自文件名的标题部分（系列/篇名），不属于变体区分标签，跳过。
        norm_token_sp = _normalize_media_stem(token).replace("-", " ")
        if norm_token_sp and (
            (_src_title_space and norm_token_sp in _src_title_space)
            or (_stem_title_space and norm_token_sp in _stem_title_space)
        ):
            continue
        candidate = f"{out} {token}".strip()
        if len(candidate) > 120:
            continue
        out = candidate
        lower = out.lower()
    if out == label:
        return parse_result
    return parse_result._replace(extra_label=out)


def _needs_readable_suffix(parse_result: ParseResult) -> bool:
    if parse_result.extra_category not in (SPECIAL_CATEGORIES | EXTRA_CATEGORIES):
        return False
    label = re.sub(r"\s+", " ", str(parse_result.extra_label or "")).strip()
    if label and _has_explicit_descriptive_extra_label(label, parse_result.title):
        return False
    stable_idx = _extract_stable_label_index(parse_result.extra_label)
    if stable_idx is not None:
        return False
    if parse_result.episode is None:
        return True
    return int(parse_result.episode) == 1


def _has_explicit_descriptive_extra_label(label: str, parsed_title: str | None) -> bool:
    normalized = re.sub(r"\s+", " ", str(label or "")).strip()
    if not normalized:
        return False
    if _looks_like_technical_extra_label(normalized):
        return False
    if _looks_title_like_fallback_label(normalized, parsed_title):
        return False
    if re.fullmatch(
        r"(?:SP|SPECIAL|OVA|OAD|OAV|OP|ED|NCOP|NCED|PV|TRAILER|PREVIEW|CM|BDEXTRA|MAKING|INTERVIEW)(?:\s*\d{0,3})?",
        normalized,
        re.I,
    ):
        return False
    if len(normalized) < 3 and re.search(r"[\u4e00-\u9fff]", normalized) is None:
        return False
    return True


def _normalize_suffix_text(text: str) -> str:
    s = str(text or "")
    s = re.sub(r"\[[^\]]*\]|\([^\)]*\)|\{[^\}]*\}", " ", s)
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"[^\w\u4e00-\u9fff\- ]+", " ", s, flags=re.U)
    return re.sub(r"\s+", " ", s).strip(" -_")


def _build_readable_suffix(src_path: Path, parse_result: ParseResult) -> str:
    raw = re.sub(r"\s*-\s*(?:mawen\d*|vcb(?:-?studio)?|mysilu)\s*$", "", src_path.stem, flags=re.I)
    raw = re.sub(r"\[[^\]]*\]", " ", raw)
    candidates = re.split(r"\s*-\s*", raw)
    title_norm = _normalize_suffix_text(parse_result.title).lower()
    for segment in candidates:
        cleaned = _normalize_suffix_text(segment)
        if not cleaned:
            continue
        if title_norm and cleaned.lower() == title_norm:
            continue
        if re.fullmatch(r"\d+", cleaned):
            continue
        if re.search(r"[A-Za-z\u4e00-\u9fff]", cleaned) is None:
            continue
        if len(cleaned) < 3 and re.search(r"[\u4e00-\u9fff]", cleaned) is None:
            continue
        if _is_release_group_phrase(cleaned):
            continue
        return cleaned[:28]
    cleaned_all = _normalize_suffix_text(raw)
    if cleaned_all and not re.fullmatch(r"\d+", cleaned_all) and not _is_release_group_phrase(cleaned_all):
        return cleaned_all[:28]
    fallback_label = extract_strong_extra_fallback_label(src_path.name)
    if fallback_label and not _is_release_group_phrase(fallback_label):
        return fallback_label[:28]
    return f"h{hashlib.md5(str(src_path).encode('utf-8')).hexdigest()[:6]}"


def _ensure_unique_suffix_label(base_label: str, src_path: Path, parse_result: ParseResult, dir_runtime: dict | None) -> str:
    if dir_runtime is None:
        return base_label
    registry = dir_runtime.setdefault("suffix_label_registry", {})
    parent_key = str(src_path.parent)
    category_key = str(parse_result.extra_category or "extra")
    key = (parent_key, category_key, base_label.lower())
    idx = int(registry.get(key, 0)) + 1
    registry[key] = idx
    if idx == 1:
        return base_label
    candidate = f"{base_label} #{idx:02d}"
    if len(candidate) > 80:
        candidate = candidate[:80].rstrip()
    return candidate


def _assign_sequential_episode_for_unnumbered(parse_result: ParseResult, src_path: Path, dir_runtime: dict | None) -> ParseResult:
    if dir_runtime is None:
        return parse_result
    stable_idx = _extract_stable_label_index(parse_result.extra_label)
    if stable_idx is not None:
        return parse_result._replace(episode=stable_idx)
    parent_key = str(src_path.parent)
    key = (parent_key, str(parse_result.extra_category or "special"))
    seq_map = dir_runtime.setdefault("unnumbered_special_seq", {})
    current = int(seq_map.get(key, 0)) + 1
    seq_map[key] = current
    return parse_result._replace(episode=current)


def _sanitize_extra_suffix(text: str) -> str:
    s = re.sub(r"\s+", " ", str(text or "")).strip(" -_")
    if not s:
        return ""
    s = _strip_release_group_fragments(s)
    s = re.sub(r"\b(?:nc|on\s*air)\s*ver\.?\b", " ", s, flags=re.I)
    s = re.sub(r"\((?:nc|on\s*air)\s*ver\.?\)", " ", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -_")
    if not s:
        return ""
    if _is_release_group_phrase(s):
        return ""
    return s


def _strip_release_group_fragments(text: str) -> str:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    if not s:
        return ""
    patterns = [
        r"\bnekomoe(?:\s+kissaten)?\b",
        r"\bvcb(?:\s*-\s*studio|\s+studio)?\b",
        r"\bmawen\d*\b",
        r"\bmysilu\b",
        r"\bairota\b",
        r"\bdmhy\b",
        r"\b(?:fansub|raws?|sub)\b",
        r"(?:字幕组|字幕社|压制组|搬运组)",
    ]
    for pattern in patterns:
        s = re.sub(pattern, " ", s, flags=re.I)
    s = re.sub(r"\s*[&/|+]+\s*", " ", s)
    s = re.sub(r"\(\s*\)", " ", s)
    s = re.sub(r"\[\s*\]", " ", s)
    return re.sub(r"\s+", " ", s).strip(" -_")


def _is_release_group_phrase(text: str) -> bool:
    s = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not s:
        return False
    if re.search(r"[\u4e00-\u9fff]", s):
        return bool(re.search(r"(字幕组|字幕社|压制组|搬运组)$", s))
    normalized = re.sub(r"\b(?:[a-z]\.){2,}[a-z]?\b", lambda m: m.group(0).replace(".", ""), s)
    tokens = [tok for tok in re.split(r"[\s\-_.&+/]+", normalized) if tok]
    if not tokens:
        return False
    noise_patterns = [
        r"vcb(?:studio)?",
        r"mawen\d*",
        r"mysilu",
        r"nekomoe",
        r"kissaten",
        r"airota",
        r"dmhy",
        r"sub",
        r"fansub",
        r"raws?",
        r"studio",
    ]
    matched = 0
    for tok in tokens:
        if any(re.fullmatch(p, tok, flags=re.I) for p in noise_patterns):
            matched += 1
    if matched == len(tokens):
        return True

    raw_text = str(text or "")
    if re.search(r"[&+/]", raw_text):
        indicator_count = 0
        groupish_count = 0
        for tok in tokens:
            if re.fullmatch(r"(?:\d{3,4}p|\d{3,4}x\d{3,4}|\d+(?:\.\d+)?fps)", tok, flags=re.I):
                continue
            if re.search(r"(?:sub|studio|raws?|fansub)$", tok, flags=re.I):
                indicator_count += 1
            if re.fullmatch(r"[a-z][a-z0-9]{1,19}|[a-z]{2,20}(?:sub|studio)|[a-z]{2,10}\d*", tok, flags=re.I):
                groupish_count += 1
        if indicator_count >= 1 and groupish_count == len(tokens):
            return True
    return False
