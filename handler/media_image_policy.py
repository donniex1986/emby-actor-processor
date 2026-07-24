import threading

import config_manager
import constants
from database import image_policy_db
from database.metadata_provider_db import (
    build_image_language_priority,
    get_cached_original_language,
    load_emby_metadata,
    preferred_image_candidates,
    sort_image_candidates,
)
from handler import tmdb
from handler.media_image_cache import (
    archive_policy_images,
    cache_image_bytes,
    discard_unreferenced_policy_sources,
)


IMAGE_TYPES = ("Primary", "Art", "Backdrop", "Banner", "Logo", "Thumb", "Disc")
_SYNC_LOCKS = [threading.Lock() for _ in range(32)]


class ImagePolicySyncError(RuntimeError):
    pass


class ImagePolicyCollectionNotFound(ImagePolicySyncError):
    pass


def _sync_lock(item_type, tmdb_id, season_number=None, episode_number=None):
    identity = (item_type, str(tmdb_id), season_number, episode_number)
    return _SYNC_LOCKS[hash(identity) % len(_SYNC_LOCKS)]


def normalize_image_rules(values):
    by_type = {}
    for value in values or []:
        if not isinstance(value, dict):
            continue
        image_type = str(value.get("type") or value.get("Type") or "").strip().title()
        if image_type not in IMAGE_TYPES:
            continue
        try:
            limit = int(value.get("limit", value.get("Limit", 0)) or 0)
            min_width = int(value.get("min_width", value.get("MinWidth", 0)) or 0)
        except (TypeError, ValueError):
            continue
        if limit <= 0:
            continue
        by_type[image_type] = {
            "type": image_type,
            "limit": min(20, limit) if image_type == "Backdrop" else 1,
            "min_width": max(0, min(10000, min_width)),
        }
    return [by_type[image_type] for image_type in IMAGE_TYPES if image_type in by_type]


def get_live_image_candidates(
    requested_type,
    tmdb_id,
    media_type,
    *,
    season_number=None,
    episode_number=None,
    include_all_languages=False,
):
    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    if not api_key:
        raise ImagePolicySyncError("tmdb api key is not configured")

    requested_type = str(requested_type or "").strip().title()
    if requested_type == "Boxset":
        try:
            details = tmdb.get_collection_details(
                int(tmdb_id),
                api_key,
                skip_fallback=True,
                apply_image_preference=False,
                raise_not_found=True,
            )
        except tmdb.TmdbNotFoundError as exc:
            raise ImagePolicyCollectionNotFound(str(tmdb_id)) from exc
    else:
        details = None

    raw = tmdb.get_item_images_tmdb(
        requested_type,
        int(tmdb_id),
        api_key,
        season_number=season_number,
        episode_number=episode_number,
    )
    if raw is None:
        raise ImagePolicySyncError("tmdb image lookup failed")

    if requested_type == "Boxset":
        original_language = next((
            str(item.get("original_language") or "").strip()
            for item in (details or {}).get("parts") or []
            if isinstance(item, dict) and item.get("original_language")
        ), "")
    else:
        original_language = get_cached_original_language(tmdb_id, media_type)

    preference = config_manager.APP_CONFIG.get(
        constants.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE,
        "zh",
    )
    priorities = build_image_language_priority(original_language, preference)
    candidates = []

    def _append(values, image_type):
        selected = (
            sort_image_candidates(values, priorities)
            if include_all_languages
            else preferred_image_candidates(values, priorities)
        )
        for item in selected:
            path_value = str(item.get("file_path") or "")
            if not path_value:
                continue
            source_url = f"https://image.tmdb.org/t/p/original/{path_value.lstrip('/')}"
            candidates.append({
                "type": image_type,
                "source_url": source_url,
                "thumbnail_source_url": (
                    f"https://image.tmdb.org/t/p/w500/{path_value.lstrip('/')}"
                ),
                "url": source_url,
                "thumbnail_url": (
                    f"https://image.tmdb.org/t/p/w500/{path_value.lstrip('/')}"
                ),
                "language": item.get("iso_639_1") if include_all_languages else None,
                "width": item.get("width"),
                "height": item.get("height"),
                "community_rating": item.get("vote_average"),
                "vote_count": item.get("vote_count"),
            })

    if requested_type in {"Movie", "Series", "Boxset"}:
        _append(raw.get("posters"), "Primary")
        _append(raw.get("posters"), "Disc")
        _append(raw.get("backdrops"), "Backdrop")
        _append(raw.get("backdrops"), "Art")
        _append(raw.get("backdrops"), "Banner")
        _append(raw.get("backdrops"), "Thumb")
        if requested_type != "Boxset":
            _append(raw.get("logos"), "Logo")
    elif requested_type == "Season":
        _append(raw.get("posters"), "Primary")
        _append(raw.get("posters"), "Disc")
    else:
        _append(raw.get("stills"), "Primary")
        _append(raw.get("stills"), "Art")
        _append(raw.get("stills"), "Backdrop")
        _append(raw.get("stills"), "Banner")
        _append(raw.get("stills"), "Thumb")
        _append(raw.get("stills"), "Disc")
    return candidates


def _select_candidates(candidates, rules, previous_images=None, preserve_manual=True):
    selected = []
    previous_images = previous_images or []
    for rule in rules:
        image_type = rule["type"]
        seen = set()
        matches = []
        for candidate in candidates:
            if candidate.get("type") != image_type:
                continue
            width = candidate.get("width")
            if width is not None:
                try:
                    if int(width) < rule["min_width"]:
                        continue
                except (TypeError, ValueError):
                    continue
            source_url = str(candidate.get("source_url") or "").strip()
            if not source_url or source_url in seen:
                continue
            seen.add(source_url)
            matches.append(dict(candidate))
            if len(matches) >= rule["limit"]:
                break

        if preserve_manual:
            manual = next((
                dict(item) for item in previous_images
                if item.get("type") == image_type and item.get("manual")
            ), None)
            if manual:
                matches = [
                    manual,
                    *[
                        item for item in matches
                        if item.get("source_url") != manual.get("source_url")
                    ],
                ][:rule["limit"]]

        for index, candidate in enumerate(matches):
            candidate["index"] = index
            selected.append(candidate)
    return selected


def _stored_images(images):
    values = []
    for image in images:
        value = dict(image)
        value.pop("url", None)
        value.pop("thumbnail_url", None)
        values.append(value)
    return values


def _legacy_source_urls(
    requested_type,
    tmdb_id,
    media_type,
    season_number=None,
    episode_number=None,
):
    if str(requested_type or "").strip().title() == "Boxset":
        return set()
    payload = load_emby_metadata(
        tmdb_id,
        media_type,
        requested_type,
        season_number=season_number,
        episode_number=episode_number,
    ) or {}
    return {
        str(value).strip()
        for value in (payload.get("images") or {}).values()
        if str(value or "").startswith(("http://", "https://"))
    }


def sync_image_policy(
    requested_type,
    tmdb_id,
    media_type,
    rules,
    base_url,
    *,
    season_number=None,
    episode_number=None,
    force=False,
):
    rules = normalize_image_rules(rules)
    with _sync_lock(requested_type, tmdb_id, season_number, episode_number):
        existing = image_policy_db.get_image_policy(
            requested_type,
            tmdb_id,
            season_number,
            episode_number,
        )
        previous_images = list((existing or {}).get("images_json") or [])
        if existing and list(existing.get("policy_json") or []) == rules and not force:
            archived = archive_policy_images(previous_images, base_url)
            stored = _stored_images(archived)
            if stored != previous_images:
                image_policy_db.replace_image_policy(
                    requested_type,
                    tmdb_id,
                    rules,
                    stored,
                    season_number,
                    episode_number,
                )
            return archived

        candidates = [] if not rules else get_live_image_candidates(
            requested_type,
            tmdb_id,
            media_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        selected = _select_candidates(
            candidates,
            rules,
            previous_images,
            preserve_manual=not force,
        )
        archived = archive_policy_images(selected, base_url)
        stored = _stored_images(archived)
        previous = image_policy_db.replace_image_policy(
            requested_type,
            tmdb_id,
            rules,
            stored,
            season_number,
            episode_number,
        )
        current_sources = {item.get("source_url") for item in stored}
        legacy_sources = _legacy_source_urls(
            requested_type,
            tmdb_id,
            media_type,
            season_number,
            episode_number,
        )
        discard_unreferenced_policy_sources(
            [
                item.get("source_url")
                for item in previous
                if item.get("source_url") not in current_sources
            ] + [
                source for source in legacy_sources
                if source not in current_sources
            ]
        )
        return archived


def update_manual_policy_image(
    requested_type,
    tmdb_id,
    image_type,
    source_url,
    base_url,
    *,
    season_number=None,
    episode_number=None,
    content_hash=None,
    image_data=None,
    mime_type=None,
    force_archive=False,
):
    image_type = str(image_type or "").strip().title()
    if image_type not in IMAGE_TYPES or not source_url:
        return 0
    with _sync_lock(requested_type, tmdb_id, season_number, episode_number):
        existing = image_policy_db.get_image_policy(
            requested_type,
            tmdb_id,
            season_number,
            episode_number,
        ) or {}
        rules = normalize_image_rules(existing.get("policy_json") or [])
        if image_type not in {rule["type"] for rule in rules}:
            rules = normalize_image_rules([
                *rules,
                {"type": image_type, "limit": 1, "min_width": 0},
            ])
        images = [
            dict(item) for item in existing.get("images_json") or []
            if not (item.get("type") == image_type and int(item.get("index") or 0) == 0)
        ]
        manual_image = {
            "type": image_type,
            "index": 0,
            "source_url": str(source_url),
            "thumbnail_source_url": str(source_url),
            "manual": True,
        }
        content_hash = str(content_hash or "").strip().lower()
        if not content_hash and image_data:
            cached = cache_image_bytes(
                str(source_url),
                image_data,
                mime_type or "image/jpeg",
                force=force_archive,
            )
            content_hash = str((cached or {}).get("content_hash") or "").strip().lower()
        if content_hash:
            manual_image["content_hash"] = content_hash
        images.append(manual_image)
        images.sort(key=lambda item: (
            IMAGE_TYPES.index(item.get("type")),
            int(item.get("index") or 0),
        ))
        archived = archive_policy_images(images, base_url)
        previous = image_policy_db.replace_image_policy(
            requested_type,
            tmdb_id,
            rules,
            _stored_images(archived),
            season_number,
            episode_number,
        )
        current_sources = {item.get("source_url") for item in images}
        discard_unreferenced_policy_sources(
            item.get("source_url")
            for item in previous
            if item.get("source_url") not in current_sources
        )
    return 1
