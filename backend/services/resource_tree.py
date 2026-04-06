from __future__ import annotations

import re


AUX_RESOURCE_DIRS = {"extras", "specials", "trailers", "interviews"}


def normalize_path(path_value: str | None) -> str:
    return str(path_value or "").replace("\\", "/").rstrip("/")


def extract_filename(path_value: str | None) -> str:
    normalized = normalize_path(path_value)
    if not normalized:
        return ""
    return normalized.split("/")[-1]


def extract_target_dir(path_value: str | None) -> str:
    normalized = normalize_path(path_value)
    if not normalized:
        return ""
    parts = normalized.split("/")
    if len(parts) <= 1:
        return normalized
    return "/".join(parts[:-1])


def extract_season_dir(path_value: str | None) -> str:
    normalized = normalize_path(path_value)
    match = re.match(r"^(.*?/Season\s+\d{1,2})(?:/|$)", normalized, re.I)
    if match and match.group(1):
        return match.group(1)
    return extract_target_dir(normalized)


def extract_show_dir(path_value: str | None) -> str:
    season_dir = extract_season_dir(path_value)
    normalized = normalize_path(path_value)
    if not season_dir or season_dir == normalized:
        return extract_target_dir(path_value)
    return extract_target_dir(season_dir)


def extract_movie_resource_dir(path_value: str | None) -> str:
    target_dir = extract_target_dir(path_value)
    if not target_dir:
        return ""
    leaf = extract_filename(target_dir).lower()
    if leaf in AUX_RESOURCE_DIRS:
        return extract_target_dir(target_dir)
    return target_dir


def extract_tv_primary_season(path_value: str | None) -> int | None:
    normalized = normalize_path(path_value)
    match = re.match(r"^.*?/Season\s+(\d{1,2})(?:/|$)", normalized, re.I)
    if not match:
        return None
    season = int(match.group(1))
    return season  # Season 00 也允许（即 season=0）


def extract_tv_aux_season(path_value: str | None) -> int | None:
    normalized = normalize_path(path_value)
    if not normalized:
        return None

    target_dir = extract_target_dir(normalized)
    leaf = extract_filename(target_dir).lower()
    filename = extract_filename(normalized)

    if leaf in {"season 00", "specials"}:
        match = re.search(r"\bS00E(\d{3,4})\b", filename, re.I)
        if not match:
            return None
        encoded_episode = int(match.group(1))
        if encoded_episode < 100:
            return None
        season = encoded_episode // 100
        return season if season > 0 else None

    if leaf in AUX_RESOURCE_DIRS:
        match = re.search(r"\bS(\d{1,2})(?:[_ .-]|$)", filename, re.I)
        if not match:
            return None
        season = int(match.group(1))
        return season  # S00_CM01 等 season=0 的特典也允许

    return None


def extract_tmdb_id_from_path(path_value: str | None) -> int | None:
    normalized = normalize_path(path_value)
    match = re.search(r"\[tmdbid=(\d+)\]", normalized, re.I)
    if not match:
        return None
    return int(match.group(1))


def infer_media_type_from_target_path(path_value: str | None) -> str:
    normalized = normalize_path(path_value)
    if re.search(r"/Season\s+\d{1,2}(?:/|$)", normalized, re.I):
        return "tv"
    return "movie"


def _resolve_item_media_type(item: dict) -> str:
    target_path = normalize_path(item.get("target_path"))
    if extract_tv_primary_season(target_path) is not None or extract_tv_aux_season(target_path) is not None:
        return "tv"

    item_type = str(item.get("type") or "").strip().lower()
    if item_type in {"tv", "movie"}:
        return item_type

    return infer_media_type_from_target_path(target_path)


def _resolve_item_resource_dir(item: dict, media_type: str) -> str:
    target_path = normalize_path(item.get("target_path"))
    if media_type == "tv":
        resource_dir = extract_show_dir(target_path)
    else:
        resource_dir = extract_movie_resource_dir(target_path)
    if resource_dir:
        return normalize_path(resource_dir)
    fallback = extract_movie_resource_dir(target_path) or extract_show_dir(target_path)
    return normalize_path(fallback)


def _resource_group_key(sync_group_id: int | None, tmdb_id: int | None, resource_dir: str) -> str:
    return f"{sync_group_id or 0}:{tmdb_id or 0}:{resource_dir}"


def _resolve_group_media_type(group: dict) -> str:
    if group.get("explicit_tv_count"):
        return "tv"
    if group.get("type_tv_count"):
        return "tv"
    if group.get("explicit_movie_count"):
        return "movie"
    if group.get("type_movie_count"):
        return "movie"
    return str(group.get("fallback_type") or "movie")


def clean_resource_name(path_value: str | None) -> str:
    name = extract_filename(path_value)
    if not name:
        return "未识别资源"
    return re.sub(r"\s*\[tmdbid=\d+\]", "", name, flags=re.I).strip() or "未识别资源"


def is_auxiliary_target_path(path_value: str | None) -> bool:
    normalized = normalize_path(path_value).lower()
    return any(
        token in normalized
        for token in ("/season 00/", "/specials/", "/extras/", "/trailers/", "/interviews/")
    )


def _resource_identity(item: dict) -> tuple[int | None, str, int | None, str] | None:
    target_path = normalize_path(item.get("target_path"))
    if not target_path:
        return None
    media_type = _resolve_item_media_type(item)
    tmdb_id = item.get("tmdb_id")
    if tmdb_id is None:
        tmdb_id = extract_tmdb_id_from_path(target_path)
    resource_dir = _resolve_item_resource_dir(item, media_type)
    if not resource_dir:
        return None
    sync_group_id = item.get("sync_group_id")
    return sync_group_id, media_type, tmdb_id if tmdb_id is not None else None, resource_dir


def _resource_key(sync_group_id: int | None, media_type: str, tmdb_id: int | None, resource_dir: str) -> str:
    return f"{sync_group_id or 0}:{media_type}:{tmdb_id or 0}:{resource_dir}"


def _latest_sort_value(item: dict) -> str:
    return str(item.get("updated_at") or item.get("created_at") or "")


def _clean_item(item: dict) -> dict:
    out = dict(item)
    out["target_path"] = normalize_path(out.get("target_path"))
    out["original_path"] = str(out.get("original_path") or out.get("source_path") or "")
    return out


def _build_scope(
    *,
    scope_level: str,
    item_ids: list[int],
    resource_dir: str,
    media_type: str,
    sync_group_id: int | None,
    tmdb_id: int | None,
    season: int | None = None,
    group_kind: str | None = None,
    group_label: str | None = None,
    delete_files: bool = False,
) -> dict:
    return {
        "scope_level": scope_level,
        "item_ids": sorted({int(item_id) for item_id in item_ids if int(item_id) > 0}),
        "resource_dir": resource_dir,
        "type": media_type,
        "sync_group_id": sync_group_id,
        "tmdb_id": tmdb_id,
        "season": season,
        "group_kind": group_kind,
        "group_label": group_label,
        "can_delete_files": bool(delete_files),
    }


def _sorted_item_ids(items: list[dict]) -> list[int]:
    return sorted({int(item.get("id")) for item in items if item.get("id") is not None})


def build_resource_summaries(items: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for raw_item in items or []:
        item = _clean_item(raw_item)
        identity = _resource_identity(item)
        if identity is None:
            continue
        sync_group_id, media_type, tmdb_id, resource_dir = identity
        key = _resource_group_key(sync_group_id, tmdb_id, resource_dir)
        latest_value = _latest_sort_value(item)
        primary_season = extract_tv_primary_season(item.get("target_path")) if media_type == "tv" else None
        aux_season = extract_tv_aux_season(item.get("target_path")) if media_type == "tv" else None
        is_aux = is_auxiliary_target_path(item.get("target_path"))

        if key not in grouped:
            grouped[key] = {
                "summary_key": key,
                "sample_id": item.get("id"),
                "sync_group_id": sync_group_id,
                "tmdb_id": tmdb_id,
                "resource_dir": resource_dir,
                "resource_name": clean_resource_name(resource_dir),
                "record_count": 0,
                "latest_updated_at": item.get("updated_at") or item.get("created_at"),
                "latest_sort_value": latest_value,
                "explicit_tv_count": 0,
                "explicit_movie_count": 0,
                "type_tv_count": 0,
                "type_movie_count": 0,
                "fallback_type": media_type,
                "items": [],
            }

        group = grouped[key]
        group["record_count"] += 1
        group["items"].append(item)
        if latest_value > str(group["latest_sort_value"] or ""):
            group["latest_sort_value"] = latest_value
            group["latest_updated_at"] = item.get("updated_at") or item.get("created_at")

        raw_type = str(item.get("type") or "").strip().lower()
        if raw_type == "tv":
            group["type_tv_count"] += 1
        elif raw_type == "movie":
            group["type_movie_count"] += 1

        if extract_tv_primary_season(item.get("target_path")) is not None or extract_tv_aux_season(item.get("target_path")) is not None:
            group["explicit_tv_count"] += 1
        elif not is_aux:
            group["explicit_movie_count"] += 1

    out: list[dict] = []
    for group in grouped.values():
        resolved_type = _resolve_group_media_type(group)
        season_numbers: set[int] = set()
        main_count = 0
        aux_count = 0
        misc_count = 0
        for item in group.get("items") or []:
            target_path = item.get("target_path")
            if resolved_type == "tv":
                aux_season = extract_tv_aux_season(target_path)
                primary_season = extract_tv_primary_season(target_path) if aux_season is None else None
                if aux_season is not None:
                    season_numbers.add(aux_season)
                    aux_count += 1
                elif primary_season is not None:
                    season_numbers.add(primary_season)
                    main_count += 1
                else:
                    misc_count += 1
            else:
                if is_auxiliary_target_path(target_path):
                    aux_count += 1
                else:
                    main_count += 1

        seasons = sorted(int(value) for value in season_numbers)
        season_labels = [f"Season {season:02d}" for season in seasons]
        if resolved_type == "tv":
            season_summary = " / ".join(season_labels[:2]) if len(season_labels) <= 2 else f"{len(seasons)} seasons"
            if not season_summary:
                season_summary = "Misc"
        else:
            season_summary = "Movie"
        group["type"] = resolved_type
        group["key"] = _resource_key(group.get("sync_group_id"), resolved_type, group.get("tmdb_id"), group.get("resource_dir") or "")
        group["main_count"] = main_count
        group["aux_count"] = aux_count
        group["misc_count"] = misc_count
        group["season_count"] = len(seasons)
        group["season_labels"] = season_labels
        group["season_summary"] = season_summary
        group.pop("summary_key", None)
        group.pop("explicit_tv_count", None)
        group.pop("explicit_movie_count", None)
        group.pop("type_tv_count", None)
        group.pop("type_movie_count", None)
        group.pop("fallback_type", None)
        group.pop("items", None)
        out.append(group)

    out.sort(key=lambda item: str(item.get("latest_sort_value") or ""), reverse=True)
    for item in out:
        item.pop("latest_sort_value", None)
    return out


def _tv_bucket_info(item: dict) -> dict:
    target_path = normalize_path(item.get("target_path"))

    # 先判断 aux（包含 Season 00 下的编码 SP 和 extras/ 下的特典）
    aux = extract_tv_aux_season(target_path)
    if aux is not None:
        target_dir = extract_target_dir(target_path)
        return {
            "node_kind": "season",
            "node_key": f"season:{aux}",
            "node_label": f"Season {aux:02d}",
            "season": aux,
            "node_target_dir": target_dir,
            "group_kind": "aux",
            "group_key": f"season:{aux}:aux",
            "group_label": "关联 SP/Extras",
            "group_target_dir": target_dir,
            "group_delete_files": True,
            "tree_bucket": "aux",
        }

    primary = extract_tv_primary_season(target_path)
    if primary is not None:
        target_dir = extract_season_dir(target_path)
        if primary == 0:
            # Season 00 正片（S00E0X 形式）归到 main 分组，节点标签 Season 00
            group_kind = "main"
            group_key = "season:0:main"
            group_label = "正片目录"
        else:
            group_kind = "main"
            group_key = f"season:{primary}:main"
            group_label = "正片目录"
        return {
            "node_kind": "season",
            "node_key": f"season:{primary}",
            "node_label": f"Season {primary:02d}",
            "season": primary,
            "node_target_dir": target_dir,
            "group_kind": group_kind,
            "group_key": group_key,
            "group_label": group_label,
            "group_target_dir": target_dir,
            "group_delete_files": True,
            "tree_bucket": "main",
        }

    target_dir = extract_target_dir(target_path)
    return {
        "node_kind": "misc",
        "node_key": "misc",
        "node_label": "Misc",
        "season": None,
        "node_target_dir": target_dir,
        "group_kind": "misc",
        "group_key": "misc:misc",
        "group_label": "未归属文件",
        "group_target_dir": target_dir,
        "group_delete_files": True,
        "tree_bucket": "misc",
    }


def _movie_bucket_info(item: dict) -> dict:
    target_path = normalize_path(item.get("target_path"))
    if is_auxiliary_target_path(target_path):
        group_kind = "extras"
        group_label = "Extras"
        tree_bucket = "aux"
    else:
        group_kind = "main"
        group_label = "Main"
        tree_bucket = "main"
    return {
        "node_kind": group_kind,
        "node_key": group_kind,
        "node_label": group_label,
        "season": None,
        "node_target_dir": extract_target_dir(target_path),
        "group_kind": group_kind,
        "group_key": f"{group_kind}:{group_kind}",
        "group_label": group_label,
        "group_target_dir": extract_target_dir(target_path),
        "group_delete_files": True,
        "tree_bucket": tree_bucket,
    }


def _finalize_group(
    group: dict,
    *,
    resource_dir: str,
    media_type: str,
    sync_group_id: int | None,
    tmdb_id: int | None,
    delete_files: bool,
) -> None:
    group["items"].sort(key=lambda item: (normalize_path(item.get("target_path")), normalize_path(item.get("original_path"))))
    item_ids = _sorted_item_ids(group["items"])
    group["item_count"] = len(group["items"])
    for item in group["items"]:
        item_id = int(item.get("id")) if item.get("id") is not None else 0
        item["scope"] = _build_scope(
            scope_level="item",
            item_ids=[item_id] if item_id > 0 else [],
            resource_dir=resource_dir,
            media_type=media_type,
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id,
            season=group.get("season"),
            group_kind=group.get("kind"),
            group_label=group.get("label"),
            delete_files=delete_files,
        )
    group["scope"] = _build_scope(
        scope_level="group",
        item_ids=item_ids,
        resource_dir=resource_dir,
        media_type=media_type,
        sync_group_id=sync_group_id,
        tmdb_id=tmdb_id,
        season=group.get("season"),
        group_kind=group.get("kind"),
        group_label=group.get("label"),
        delete_files=delete_files,
    )


def _finalize_node(node: dict, *, resource_dir: str, media_type: str, sync_group_id: int | None, tmdb_id: int | None, delete_files: bool) -> None:
    groups = [group for group in node["groups"] if group["items"]]
    for group in groups:
        _finalize_group(
            group,
            resource_dir=resource_dir,
            media_type=media_type,
            sync_group_id=sync_group_id,
            tmdb_id=tmdb_id,
            delete_files=delete_files,
        )
    groups.sort(key=lambda group: (0 if group["kind"] == "main" else 1, group["label"]))
    node["groups"] = groups
    item_ids = []
    for group in groups:
        item_ids.extend(group["scope"]["item_ids"])
    node["record_count"] = len(item_ids)
    node["main_count"] = sum(group["item_count"] for group in groups if group["kind"] == "main")
    node["aux_count"] = sum(group["item_count"] for group in groups if group["kind"] in {"aux", "extras"})
    node["misc_count"] = sum(group["item_count"] for group in groups if group["kind"] == "misc")
    scope_level = "season" if media_type == "tv" and node.get("season") is not None else "group"
    node["scope"] = _build_scope(
        scope_level=scope_level,
        item_ids=item_ids,
        resource_dir=resource_dir,
        media_type=media_type,
        sync_group_id=sync_group_id,
        tmdb_id=tmdb_id,
        season=node.get("season"),
        group_kind=None,
        group_label=node.get("label"),
        delete_files=delete_files,
    )


def build_resource_tree(
    items: list[dict],
    *,
    resource_dir: str,
    media_type: str | None = None,
    sync_group_id: int | None = None,
    tmdb_id: int | None = None,
    delete_files: bool = True,
) -> dict:
    normalized_resource_dir = normalize_path(resource_dir)
    selected: list[dict] = []
    resolved_type = str(media_type or "").strip().lower() or None
    resolved_tmdb_id = tmdb_id

    for raw_item in items or []:
        item = _clean_item(raw_item)
        identity = _resource_identity(item)
        if identity is None:
            continue
        item_sync_group_id, item_type, item_tmdb_id, item_resource_dir = identity
        if normalized_resource_dir != item_resource_dir:
            continue
        if sync_group_id is not None and item_sync_group_id != sync_group_id:
            continue
        if resolved_type is not None and item_type != resolved_type:
            continue
        if resolved_tmdb_id is not None and item_tmdb_id != resolved_tmdb_id:
            continue
        if resolved_type is None:
            resolved_type = item_type
        if resolved_tmdb_id is None:
            resolved_tmdb_id = item_tmdb_id
        selected.append(item)

    summaries = build_resource_summaries(selected)
    base_summary = summaries[0] if summaries else {
        "key": _resource_key(sync_group_id, resolved_type or "tv", resolved_tmdb_id, normalized_resource_dir),
        "sample_id": None,
        "sync_group_id": sync_group_id,
        "type": resolved_type or "tv",
        "tmdb_id": resolved_tmdb_id,
        "resource_dir": normalized_resource_dir,
        "resource_name": clean_resource_name(normalized_resource_dir),
        "record_count": 0,
        "season_count": 0,
        "season_labels": [],
        "season_summary": "Misc" if (resolved_type or "tv") == "tv" else "Movie",
        "main_count": 0,
        "aux_count": 0,
        "misc_count": 0,
        "latest_updated_at": None,
    }

    media_type_value = resolved_type or str(base_summary.get("type") or "tv")
    sync_group_value = sync_group_id if sync_group_id is not None else base_summary.get("sync_group_id")
    tmdb_value = resolved_tmdb_id if resolved_tmdb_id is not None else base_summary.get("tmdb_id")

    nodes_by_key: dict[str, dict] = {}
    for item in selected:
        info = _tv_bucket_info(item) if media_type_value == "tv" else _movie_bucket_info(item)
        item["tree_bucket"] = info["tree_bucket"]
        item["tree_target_dir"] = info["group_target_dir"]
        item["tree_season"] = info["season"]

        node = nodes_by_key.setdefault(
            info["node_key"],
            {
                "key": info["node_key"],
                "kind": info["node_kind"],
                "label": info["node_label"],
                "season": info["season"],
                "target_dir": info["node_target_dir"],
                "groups": [],
            },
        )
        group = next((value for value in node["groups"] if value["key"] == info["group_key"]), None)
        if group is None:
            group = {
                "key": info["group_key"],
                "kind": info["group_kind"],
                "label": info["group_label"],
                "season": info["season"],
                "target_dir": info["group_target_dir"],
                "items": [],
            }
            node["groups"].append(group)
        group["items"].append(item)
        if not node["target_dir"] and info["group_target_dir"]:
            node["target_dir"] = info["group_target_dir"]

    nodes = list(nodes_by_key.values())
    for node in nodes:
        _finalize_node(
            node,
            resource_dir=normalized_resource_dir,
            media_type=media_type_value,
            sync_group_id=sync_group_value,
            tmdb_id=tmdb_value,
            delete_files=delete_files,
        )

    if media_type_value == "tv":
        nodes.sort(key=lambda node: (node["season"] is None, node["season"] if node["season"] is not None else 10_000, node["label"]))
    else:
        sort_order = {"main": 0, "extras": 1, "misc": 2}
        nodes.sort(key=lambda node: (sort_order.get(node["kind"], 99), node["label"]))

    resource_item_ids = _sorted_item_ids(selected)
    base_summary["scope"] = _build_scope(
        scope_level="resource",
        item_ids=resource_item_ids,
        resource_dir=normalized_resource_dir,
        media_type=media_type_value,
        sync_group_id=sync_group_value,
        tmdb_id=tmdb_value,
        delete_files=delete_files,
    )
    return {"resource": base_summary, "nodes": nodes}