"""片头片尾声纹提取服务。

设计目标：
- 入库、收藏、播放和查漏补缺只负责入队；
- 服务自身按策略和媒体库范围判断是否执行；
- 后台 worker 分批提取并匹配片头/片尾；
- 声纹提取失败允许后续重试，匹配失败会按算法版本记录失败标记；
- 成功后写回 ETK 媒体信息缓存、Emby 章节和共享片头索引。
"""

from __future__ import annotations

import json
import logging
import queue
import re
import shutil
import struct
import subprocess
import threading
import time
import zlib
from dataclasses import dataclass
from statistics import median
from typing import Any, Dict, List, Optional, Sequence, Tuple

import config_manager
import constants
from database import settings_db, user_db, watchlist_db
from database.connection import get_db_connection

logger = logging.getLogger(__name__)

SAMPLE_SECONDS = 600
INTRO_SAMPLE_STEPS = (180, 300, SAMPLE_SECONDS)
CREDITS_SAMPLE_STEPS = (180, 300)
INTRO_DETECTION_ALGORITHM_VERSION = 6
MAX_WORK_EPISODES_PER_SEASON = 3
MAX_WRITE_EPISODES_PER_JOB = MAX_WORK_EPISODES_PER_SEASON
BATCH_CONTINUATION_DELAY_SECONDS = 3
MIN_INTRO_SECONDS = 18
MAX_INTRO_SECONDS = 240
MAX_INTRO_START_SECONDS = 360
MIN_CREDITS_SECONDS = 18
MAX_CREDITS_SECONDS = 300
CREDITS_BOUNDARY_MARGIN_SECONDS = 8
CPU_YIELD_INTERVAL = 32
CPU_YIELD_SECONDS = 0.001
COMMON_MATCH_TIMEOUT_SECONDS = 60
EPISODE_MATCH_TIMEOUT_SECONDS = 20
INTRO_TICKS = 10_000_000
QUEUE_MAXSIZE = 512
RECENT_TTL_SECONDS = 6 * 3600
FFMPEG_TIMEOUT_SECONDS = 900
FINGERPRINT_HAMMING_DISTANCE = 6
# Chromaprint emits roughly eight values per second. Allowing a short audio
# discontinuity to recover prevents a valid long intro from being truncated.
FINGERPRINT_MAX_CONSECUTIVE_MISSES = 40
FINGERPRINT_MIN_MATCH_RATIO = 0.85
FINGERPRINT_ANCHOR_WIDTH = 3
FINGERPRINT_CACHE_TOLERANCE_SECONDS = 5
FINGERPRINT_ANCHOR_MASKS = (
    0x0F0F0F0F,
    0xF0F0F0F0,
    0x33333333,
    0xCCCCCCCC,
)
INTRO_TRIGGER_MODE_OFF = "off"
INTRO_TRIGGER_MODE_IMPORT = "import"
INTRO_TRIGGER_MODE_FAVORITE = "favorite"
INTRO_TRIGGER_MODE_PLAYBACK = "playback"
INTRO_TRIGGER_MODES = {
    INTRO_TRIGGER_MODE_OFF,
    INTRO_TRIGGER_MODE_IMPORT,
    INTRO_TRIGGER_MODE_FAVORITE,
    INTRO_TRIGGER_MODE_PLAYBACK,
}
INTRO_JOB_TRIGGER_IMPORT = "import"
INTRO_JOB_TRIGGER_FAVORITE = "favorite"
INTRO_JOB_TRIGGER_PLAYBACK = "playback"
INTRO_JOB_TRIGGER_BACKFILL = "backfill"
FINGERPRINT_KIND_INTRO = "intro"
FINGERPRINT_KIND_CREDITS = "credits"
# Keep this aligned with ffprobe and video screenshot extraction so the
# process-wide 115 direct-link cache can be shared by PickCode.
UA = "Mozilla/5.0"

_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAXSIZE)
_worker_lock = threading.Lock()
_worker_started = False
_recent_lock = threading.Lock()
_recent_jobs: Dict[str, float] = {}


class _FingerprintMatchTimeout(RuntimeError):
    """声纹匹配计算超过本轮预算。"""


def _new_match_deadline(seconds: int) -> float:
    return time.monotonic() + max(1, int(seconds or 1))


def _check_match_deadline(deadline: Optional[float]) -> None:
    if deadline and time.monotonic() > deadline:
        raise _FingerprintMatchTimeout("声纹匹配计算超时")


def _yield_match_cpu(deadline: Optional[float] = None) -> None:
    time.sleep(CPU_YIELD_SECONDS)
    _check_match_deadline(deadline)


@dataclass
class EpisodeRef:
    item_id: str
    series_id: str
    season_id: str
    series_tmdb_id: str
    title: str
    series_title: str
    season_number: int
    episode_number: int
    sha1: str
    pick_code: str
    source_library_id: str = ""
    path: str = ""


@dataclass
class FingerprintRef:
    episode: EpisodeRef
    values: List[int]
    sample_seconds: int
    window_start_seconds: float = 0.0
    kind: str = FINGERPRINT_KIND_INTRO

    @property
    def seconds_per_value(self) -> float:
        if not self.values:
            return 0.0
        return max(0.01, float(self.sample_seconds or SAMPLE_SECONDS) / float(len(self.values)))


def enqueue_item(
    item: Dict[str, Any],
    sha1: str = "",
    *,
    trigger: str = INTRO_JOB_TRIGGER_IMPORT,
) -> Dict[str, Any]:
    """Queue one Emby Episode for the autonomous intro detector.

    The function is deliberately no-throw so callers can use it from ingest
    callbacks without risking the main media-info flow.
    """
    try:
        mode = get_trigger_mode()
        trigger = _normalize_job_trigger(trigger)
        if mode == INTRO_TRIGGER_MODE_OFF:
            return {"ok": True, "queued": False, "skipped": True, "reason": "disabled"}
        if not _mode_accepts_job(mode, trigger):
            return {"ok": True, "queued": False, "skipped": True, "reason": "trigger_not_selected"}

        item = item if isinstance(item, dict) else {}
        item_id = str(item.get("Id") or "").strip()
        item_type = str(item.get("Type") or "").strip().title()
        series_id = str(item.get("SeriesId") or "").strip()
        if item_type != "Episode" or not item_id or not series_id:
            return {"ok": True, "queued": False, "skipped": True, "reason": "invalid_episode"}

        normalized_sha1 = _norm_sha1(sha1)
        job_key = f"{trigger}:{item_id}"
        if not _claim_recent(job_key):
            return {"ok": True, "queued": False, "skipped": True, "reason": "duplicate"}

        _start_worker()
        payload = {
            "kind": "item",
            "item": {
                "Id": item_id,
                "Type": item_type,
                "Name": item.get("Name"),
                "SeriesId": series_id,
                "SeasonId": item.get("SeasonId"),
                "SeriesName": item.get("SeriesName"),
                "ParentIndexNumber": item.get("ParentIndexNumber"),
                "IndexNumber": item.get("IndexNumber"),
                "Path": item.get("Path"),
                "SourceLibraryId": item.get("_SourceLibraryId") or item.get("SourceLibraryId") or item.get("LibraryId"),
            },
            "sha1": normalized_sha1,
            "trigger": trigger,
            "queued_at": time.time(),
        }
        _queue.put_nowait(payload)
        logger.info(
            "  ➜ [片头片尾提取] 已入队：%s S%sE%s（队列 %s）",
            item.get("SeriesName") or series_id,
            item.get("ParentIndexNumber") or "?",
            item.get("IndexNumber") or "?",
            _queue.qsize(),
        )
        return {"ok": True, "queued": True, "item_id": item_id}
    except queue.Full:
        return {"ok": False, "queued": False, "reason": "queue_full"}
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 入队失败: %s", e, exc_info=True)
        return {"ok": False, "queued": False, "reason": "enqueue_error", "error": str(e)}


def enqueue_imported_episode_ids(
    episode_ids: Sequence[Any],
    *,
    allow_playback_followup: bool = False,
) -> Dict[str, Any]:
    """Queue newly imported episodes only after ETK has persisted bindings."""
    try:
        mode = get_trigger_mode()
        if mode == INTRO_TRIGGER_MODE_OFF:
            return {"ok": True, "queued": 0, "skipped": True, "reason": "disabled"}
        import_trigger_allowed = _mode_accepts_job(mode, INTRO_JOB_TRIGGER_IMPORT)
        playback_followup = mode == INTRO_TRIGGER_MODE_PLAYBACK and allow_playback_followup
        if not import_trigger_allowed and not playback_followup:
            return {"ok": True, "queued": 0, "skipped": True, "reason": "trigger_not_selected"}

        ids = []
        for value in episode_ids or []:
            item_id = str(value or "").strip()
            if item_id and item_id not in ids:
                ids.append(item_id)
        if not ids:
            return {"ok": True, "queued": 0, "skipped": True, "reason": "empty"}

        _start_worker()
        queued = 0
        unresolved = 0
        for item_id in ids:
            ref = _load_episode_ref_by_item_id(item_id)
            if not ref:
                unresolved += 1
                continue
            if playback_followup and not _season_has_intro_detection_cache(ref):
                continue
            if not _claim_recent(f"{INTRO_JOB_TRIGGER_IMPORT}:{item_id}"):
                continue
            _queue.put_nowait({
                "kind": "episode",
                "episode": _episode_to_payload(ref),
                "trigger": INTRO_JOB_TRIGGER_PLAYBACK if playback_followup else INTRO_JOB_TRIGGER_IMPORT,
                "queued_at": time.time(),
            })
            queued += 1
        if queued:
            logger.info(
                "  ➜ [片头片尾提取] 入库后已确认分集落库，入队 %s/%s 集（队列 %s）。",
                queued,
                len(ids),
                _queue.qsize(),
            )
        if unresolved:
            logger.debug(
                "  ➜ [片头片尾提取] 入库后仍有 %s/%s 集未能定位分集记录，已跳过。",
                unresolved,
                len(ids),
            )
        return {"ok": True, "queued": queued, "unresolved": unresolved}
    except queue.Full:
        return {"ok": False, "queued": 0, "reason": "queue_full"}
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 入库后入队失败: %s", e, exc_info=True)
        return {"ok": False, "queued": 0, "reason": "enqueue_error", "error": str(e)}


def enqueue_favorite_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Start only the season(s) represented by a newly favorited item."""
    if get_trigger_mode() != INTRO_TRIGGER_MODE_FAVORITE:
        return {"ok": True, "queued": False, "skipped": True, "reason": "trigger_not_selected"}

    item = item if isinstance(item, dict) else {}
    item_id = str(item.get("Id") or "").strip()
    item_type = str(item.get("Type") or "").strip().title()
    if not item_id or item_type not in {"Series", "Season", "Episode"}:
        return {"ok": True, "queued": False, "skipped": True, "reason": "unsupported_favorite"}
    if item_type == "Episode":
        return enqueue_item(item, trigger=INTRO_JOB_TRIGGER_FAVORITE)

    refs = _load_favorite_scope_episode_refs(item_id, item_type)
    selected_library_ids = _intro_detection_library_ids()
    queued = 0
    for ref in refs:
        if not _is_intro_library_allowed(ref, selected_library_ids):
            continue
        result = enqueue_item(
            {
                "Id": ref.item_id,
                "Type": "Episode",
                "Name": ref.title,
                "SeriesId": ref.series_id,
                "SeasonId": ref.season_id,
                "SeriesName": ref.series_title,
                "ParentIndexNumber": ref.season_number,
                "IndexNumber": ref.episode_number,
                "SourceLibraryId": ref.source_library_id,
                "Path": ref.path,
            },
            sha1=ref.sha1,
            trigger=INTRO_JOB_TRIGGER_FAVORITE,
        )
        queued += int(bool(result.get("queued")))
    return {"ok": True, "queued": queued > 0, "count": queued, "candidates": len(refs)}


def enqueue_playback_halfway(item: Dict[str, Any]) -> Dict[str, Any]:
    """Queue just the currently played Episode's season after its halfway event."""
    return enqueue_item(item, trigger=INTRO_JOB_TRIGGER_PLAYBACK)


def enqueue_active_backfill(limit: int = 50, *, force: bool = False) -> Dict[str, Any]:
    """Queue one missing Episode per season for the configured trigger scope."""
    try:
        mode = get_trigger_mode()
        if mode == INTRO_TRIGGER_MODE_PLAYBACK:
            return {"ok": True, "queued": False, "skipped": True, "reason": "playback_realtime_only"}
        if mode == INTRO_TRIGGER_MODE_OFF or (not force and mode != INTRO_TRIGGER_MODE_IMPORT):
            return {"ok": True, "queued": False, "skipped": True, "reason": "disabled"}

        _start_worker()
        selected_library_ids = _intro_detection_library_ids()
        batch_limit = max(1, min(int(limit or 50), 200))
        if mode == INTRO_TRIGGER_MODE_IMPORT:
            refs = _load_library_episode_refs(
                limit=batch_limit,
                selected_library_ids=selected_library_ids,
            )
            scope_label = "所选媒体库"
        else:
            refs = _load_active_watchlist_episode_refs(
                limit=batch_limit,
                selected_library_ids=selected_library_ids,
            )
            scope_label = "收藏范围"
        queued = 0
        for ref in refs:
            key = f"backfill:{ref.series_tmdb_id}:s{ref.season_number}"
            if not _claim_recent(key):
                continue
            try:
                _queue.put_nowait({
                    "kind": "backfill",
                    "episode": _episode_to_payload(ref),
                    "trigger": INTRO_JOB_TRIGGER_BACKFILL,
                    "queued_at": time.time(),
                })
                queued += 1
            except queue.Full:
                break

        logger.info("  ➜ [片头片尾提取] %s查漏入队完成：%s/%s", scope_label, queued, len(refs))
        return {
            "ok": True,
            "queued": queued > 0,
            "count": queued,
            "candidates": len(refs),
            "scope": "library" if mode == INTRO_TRIGGER_MODE_IMPORT else "favorite",
        }
    except Exception as e:
        logger.warning("  ➜ [片头片尾提取] 活跃追剧查漏入队失败: %s", e)
        return {"ok": False, "queued": False, "reason": "service_unavailable", "error": str(e)}


def get_trigger_mode() -> str:
    """Read the autonomous-intro policy from the smart-watch settings."""
    try:
        cfg = settings_db.get_setting("watchlist_config") or {}
        mode = str(cfg.get("intro_detection_trigger_mode") or "").strip().lower()
        if mode in INTRO_TRIGGER_MODES:
            return mode

        # Compatibility for instances that have not opened the new settings
        # page yet.  The settings route persists this migration on first read.
        legacy = settings_db.get_setting(
            getattr(settings_db, "SHARED_RESOURCE_CONFIG_KEY", "shared_resource_config")
        ) or {}
        return (
            INTRO_TRIGGER_MODE_IMPORT
            if legacy.get("p115_etk_intro_detection_enabled")
            else INTRO_TRIGGER_MODE_OFF
        )
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 读取触发策略失败，按关闭处理: %s", e)
        return INTRO_TRIGGER_MODE_OFF


def _enabled() -> bool:
    return get_trigger_mode() != INTRO_TRIGGER_MODE_OFF


def _credits_detection_enabled() -> bool:
    """Credits matching is opt-in because it needs an extra tail sample."""
    try:
        cfg = settings_db.get_setting("watchlist_config") or {}
        value = cfg.get("intro_detection_credits_enabled") if isinstance(cfg, dict) else False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)
    except Exception:
        return False


def _intro_detection_library_ids() -> List[str]:
    """Selected Emby library IDs for autonomous intro extraction.

    Empty means all libraries, preserving the existing default behavior.
    """
    try:
        cfg = settings_db.get_setting("watchlist_config") or {}
        raw_ids = cfg.get("intro_detection_library_ids") if isinstance(cfg, dict) else []
        if not isinstance(raw_ids, list):
            return []
        result: List[str] = []
        seen = set()
        for item in raw_ids:
            library_id = str(item or "").strip()
            if library_id and library_id not in seen:
                seen.add(library_id)
                result.append(library_id)
        return result
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 读取媒体库范围失败，按不限制处理: %s", e)
        return []


def _resolve_ref_library_id(ref: EpisodeRef) -> str:
    if ref.source_library_id:
        return str(ref.source_library_id or "").strip()
    try:
        from handler import emby

        base_url = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        user_id = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID)
        if not base_url or not api_key or not ref.item_id:
            return ""
        library = emby.get_library_root_for_item(
            ref.item_id,
            base_url,
            api_key,
            user_id,
            item_path=ref.path,
        )
        return str((library or {}).get("Id") or "").strip()
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 解析分集所属媒体库失败 Item=%s: %s", ref.item_id, e)
        return ""


def _is_intro_library_allowed(ref: EpisodeRef, selected_library_ids: Optional[Sequence[str]] = None) -> bool:
    library_ids = [str(x or "").strip() for x in (selected_library_ids if selected_library_ids is not None else _intro_detection_library_ids()) if str(x or "").strip()]
    if not library_ids:
        return True
    return _resolve_ref_library_id(ref) in set(library_ids)


def _normalize_job_trigger(value: Any) -> str:
    trigger = str(value or "").strip().lower()
    return trigger if trigger in {
        INTRO_JOB_TRIGGER_IMPORT,
        INTRO_JOB_TRIGGER_FAVORITE,
        INTRO_JOB_TRIGGER_PLAYBACK,
        INTRO_JOB_TRIGGER_BACKFILL,
    } else INTRO_JOB_TRIGGER_IMPORT


def _mode_accepts_job(mode: str, trigger: str) -> bool:
    if trigger == INTRO_JOB_TRIGGER_BACKFILL:
        return mode in {INTRO_TRIGGER_MODE_IMPORT, INTRO_TRIGGER_MODE_FAVORITE}
    if mode == INTRO_TRIGGER_MODE_IMPORT:
        return trigger == INTRO_JOB_TRIGGER_IMPORT
    if mode == INTRO_TRIGGER_MODE_FAVORITE:
        return trigger in {INTRO_JOB_TRIGGER_IMPORT, INTRO_JOB_TRIGGER_FAVORITE}
    if mode == INTRO_TRIGGER_MODE_PLAYBACK:
        return trigger == INTRO_JOB_TRIGGER_PLAYBACK
    return False


def _load_active_watchlist_series_by_episode_sha1(sha1: str) -> Optional[Dict[str, Any]]:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return None
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT p.tmdb_id, p.title, p.watching_status
                    FROM media_metadata e
                    JOIN media_metadata p
                      ON p.tmdb_id = e.parent_series_tmdb_id
                     AND p.item_type = 'Series'
                    WHERE e.item_type = 'Episode'
                      AND e.file_sha1_json ? %s
                      AND p.watching_status = ANY(%s)
                    LIMIT 1
                    """,
                    (sha1, ["Watching", "Paused", "Pending"]),
                )
                row = cur.fetchone()
                return dict(row) if row else None
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 按 SHA1 读取活跃追剧状态失败 %s: %s", sha1[:12], e)
        return None


def _is_active_watchlist_target(target: EpisodeRef) -> bool:
    """Keep import-mode detection limited to active smart-watch series."""
    if target.series_id:
        try:
            if watchlist_db.get_active_watchlist_series_by_emby_id(target.series_id):
                return True
        except Exception as e:
            logger.debug("  ➜ [片头片尾提取] 读取 Emby 剧集追剧状态失败: %s", e)
    return bool(_load_active_watchlist_series_by_episode_sha1(target.sha1))


def _load_favorite_scope_episode_refs(item_id: str, item_type: str) -> List[EpisodeRef]:
    """Return one resident Episode per selected Series/Season.

    A Series favorite starts every resident season.  A Season favorite only
    starts that Season.  The worker itself always compares within one season.
    """
    item_id = str(item_id or "").strip()
    item_type = str(item_type or "").strip().title()
    if not item_id or item_type not in {"Series", "Season"}:
        return []

    select_sql = """
        SELECT e.tmdb_id, e.title, e.parent_series_tmdb_id, e.season_number, e.episode_number,
               e.emby_item_ids_json, e.file_sha1_json, e.file_pickcode_json, e.asset_details_json,
               p.title AS series_title, p.emby_item_ids_json AS series_emby_ids
        FROM media_metadata e
        JOIN media_metadata p
          ON p.tmdb_id = e.parent_series_tmdb_id
         AND p.item_type = 'Series'
        WHERE e.item_type = 'Episode'
          AND e.in_library = TRUE
          AND jsonb_array_length(COALESCE(e.emby_item_ids_json, '[]'::jsonb)) > 0
          AND jsonb_array_length(COALESCE(e.file_sha1_json, '[]'::jsonb)) > 0
          AND jsonb_array_length(COALESCE(e.file_pickcode_json, '[]'::jsonb)) > 0
    """
    params: List[Any] = []
    if item_type == "Series":
        select_sql += " AND p.emby_item_ids_json ? %s"
        params.append(item_id)
    else:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT parent_series_tmdb_id, season_number
                    FROM media_metadata
                    WHERE item_type = 'Season' AND emby_item_ids_json ? %s
                    LIMIT 1
                    """,
                    (item_id,),
                )
                season = cur.fetchone()
        if not season:
            return []
        select_sql += " AND e.parent_series_tmdb_id = %s AND e.season_number = %s"
        params.extend([str(season.get("parent_series_tmdb_id") or ""), _to_int(season.get("season_number"))])

    select_sql += " ORDER BY e.season_number ASC, e.episode_number ASC"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(select_sql, tuple(params))
                rows = cur.fetchall() or []
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 读取收藏范围分集失败: %s", e)
        return []

    refs: List[EpisodeRef] = []
    seen_seasons = set()
    for row in rows:
        ref = _row_to_episode_ref(dict(row))
        key = (ref.series_tmdb_id, ref.season_number) if ref else None
        if not ref or not ref.item_id or not ref.sha1 or not ref.pick_code or key in seen_seasons:
            continue
        seen_seasons.add(key)
        refs.append(ref)
    return refs


def _favorite_scope_item_ids(target: EpisodeRef) -> List[str]:
    """Collect all Emby IDs that make this exact season a favorite target."""
    values = {target.item_id, target.series_id, target.season_id}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT emby_item_ids_json
                    FROM media_metadata
                    WHERE item_type = 'Episode'
                      AND parent_series_tmdb_id = %s
                      AND season_number = %s
                    UNION ALL
                    SELECT emby_item_ids_json
                    FROM media_metadata
                    WHERE item_type = 'Season'
                      AND parent_series_tmdb_id = %s
                      AND season_number = %s
                    UNION ALL
                    SELECT emby_item_ids_json
                    FROM media_metadata
                    WHERE item_type = 'Series' AND tmdb_id = %s
                    """,
                    (
                        target.series_tmdb_id,
                        target.season_number,
                        target.series_tmdb_id,
                        target.season_number,
                        target.series_tmdb_id,
                    ),
                )
                for row in cur.fetchall() or []:
                    values.update(str(x or "").strip() for x in _json_list(row.get("emby_item_ids_json")))
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 读取收藏范围 ItemID 失败: %s", e)
    return [value for value in values if value]


def _season_emby_ids(target: EpisodeRef) -> List[str]:
    values = {target.season_id}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT emby_item_ids_json
                    FROM media_metadata
                    WHERE item_type = 'Season'
                      AND parent_series_tmdb_id = %s
                      AND season_number = %s
                    """,
                    (target.series_tmdb_id, target.season_number),
                )
                for row in cur.fetchall() or []:
                    values.update(str(x or "").strip() for x in _json_list(row.get("emby_item_ids_json")))
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 读取季 ItemID 失败: %s", e)
    return [value for value in values if value]


def _is_favorited_season(target: EpisodeRef) -> bool:
    item_ids = _favorite_scope_item_ids(target)
    if user_db.has_any_emby_favorite(item_ids):
        return True
    return _load_remote_favorite_scope(target, item_ids)


def _load_remote_favorite_scope(target: EpisodeRef, item_ids: Sequence[str]) -> bool:
    """One-time Emby fallback for favorites that predate the bridge index."""
    try:
        from handler import emby

        base_url = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
        api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY)
        if not base_url or not api_key:
            return False
        users = emby.get_all_emby_users_from_server(base_url, api_key) or []
        if not users:
            return False

        # Series/Season favorites are cheap point lookups.  An Episode favorite
        # can be any Episode in this season, so use Emby's IsFavorite filter.
        season_ids = _season_emby_ids(target)
        direct_ids = list(dict.fromkeys([target.series_id, *season_ids]))
        for user in users:
            user_id = str((user or {}).get("Id") or "").strip()
            if not user_id:
                continue
            for item_id in direct_ids:
                if not item_id:
                    continue
                details = emby.get_emby_item_details(
                    item_id,
                    base_url,
                    api_key,
                    user_id,
                    fields="UserData",
                    silent_404=True,
                )
                if bool((details or {}).get("UserData", {}).get("IsFavorite")):
                    user_db.set_emby_favorite_item(user_id, item_id, True)
                    return True
            for season_id in season_ids:
                if not season_id:
                    continue
                response = emby.emby_client.get(
                    f"{base_url.rstrip('/')}/Users/{user_id}/Items",
                    params={
                        "api_key": api_key,
                        "ParentId": season_id,
                        "IncludeItemTypes": "Episode",
                        "Filters": "IsFavorite",
                        "Limit": 1,
                        "Fields": "UserData",
                    },
                )
                if response.status_code != 200:
                    continue
                favorite = next(iter((response.json() or {}).get("Items") or []), None)
                favorite_id = str((favorite or {}).get("Id") or "").strip()
                if favorite_id:
                    user_db.set_emby_favorite_item(user_id, favorite_id, True)
                    return True
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 回查 Emby 历史收藏失败: %s", e)
    return False


def _start_worker() -> None:
    global _worker_started
    if _worker_started:
        return
    with _worker_lock:
        if _worker_started:
            return
        t = threading.Thread(target=_worker_loop, name="etk-intro-detector", daemon=True)
        t.start()
        _worker_started = True


def _claim_recent(key: str) -> bool:
    now = time.time()
    with _recent_lock:
        expired = [k for k, deadline in _recent_jobs.items() if deadline <= now]
        for k in expired:
            _recent_jobs.pop(k, None)
        if _recent_jobs.get(key, 0) > now:
            return False
        _recent_jobs[key] = now + RECENT_TTL_SECONDS
        return True


def _worker_loop() -> None:
    while True:
        job = _queue.get()
        try:
            _process_job(job)
        except Exception as e:
            logger.warning("  ➜ [片头片尾提取] 后台任务失败: %s", e, exc_info=True)
        finally:
            _queue.task_done()


def _process_job(job: Dict[str, Any]) -> None:
    mode = get_trigger_mode()
    trigger = _normalize_job_trigger(job.get("trigger"))
    if mode == INTRO_TRIGGER_MODE_OFF:
        logger.debug("  ➜ [片头片尾提取] 开关已关闭，丢弃已入队任务。")
        return
    if not _mode_accepts_job(mode, trigger):
        logger.debug("  ➜ [片头片尾提取] 当前策略不处理 %s 触发，已跳过。", trigger)
        return
    target = _resolve_episode_ref(job)
    if not target:
        logger.debug("  ➜ [片头片尾提取] 跳过：未能定位分集记录。")
        return
    selected_library_ids = _intro_detection_library_ids()
    if not _is_intro_library_allowed(target, selected_library_ids):
        logger.debug("  ➜ [片头片尾提取] 《%s》不在已选择的媒体库范围内，跳过。", target.series_title)
        return
    if mode == INTRO_TRIGGER_MODE_FAVORITE and not _is_favorited_season(target):
        logger.debug("  ➜ [片头片尾提取] 《%s》第 %s 季不在收藏范围，跳过。", target.series_title, target.season_number)
        return
    if not target.sha1 or not target.pick_code:
        logger.debug("  ➜ [片头片尾提取] 跳过《%s》S%sE%s：缺少 SHA1/PC。", target.series_title, target.season_number, target.episode_number)
        return

    season_refs = [
        ref for ref in _load_same_season_episode_refs(target)
        if _is_intro_library_allowed(ref, selected_library_ids)
    ]
    if len(season_refs) < 2:
        logger.info(
            "  ➜ [片头片尾提取] 《%s》第 %s 季可比对分集不足 2 集，暂不提取。",
            target.series_title,
            target.season_number,
        )
        return
    chapter_refs = season_refs
    _restore_cached_chapters_to_emby(chapter_refs[:MAX_WRITE_EPISODES_PER_JOB])
    intro_refs = _pending_detection_refs(chapter_refs, FINGERPRINT_KIND_INTRO, limit=MAX_WRITE_EPISODES_PER_JOB)
    credits_refs = (
        _pending_detection_refs(chapter_refs, FINGERPRINT_KIND_CREDITS, limit=MAX_WRITE_EPISODES_PER_JOB)
        if _credits_detection_enabled()
        else []
    )
    if not intro_refs and not credits_refs:
        logger.debug(
            "  ➜ [片头片尾提取] 《%s》第 %s 季已完成或当前算法版本已终止，跳过。",
            target.series_title,
            target.season_number,
        )
        return
    direct_url_cache: Dict[str, Tuple[str, str]] = {}
    if intro_refs:
        _detect_and_write_intro(target, chapter_refs, direct_url_cache=direct_url_cache)
    if credits_refs:
        _detect_and_write_credits(target, chapter_refs, direct_url_cache=direct_url_cache)
    _schedule_next_batch_if_needed(target, chapter_refs, trigger)


def _detect_and_write_intro(
    target: EpisodeRef,
    season_refs: Sequence[EpisodeRef],
    *,
    direct_url_cache: Optional[Dict[str, Tuple[str, str]]] = None,
) -> None:
    if _has_detection_failure(_failure_key_for_season(target), FINGERPRINT_KIND_INTRO):
        return

    all_pending_refs = _pending_detection_refs(
        season_refs,
        FINGERPRINT_KIND_INTRO,
    )
    if not all_pending_refs:
        return

    uncached_pending_refs = [
        ref for ref in all_pending_refs
        if not _fingerprint_cache_ready(ref, FINGERPRINT_KIND_INTRO, SAMPLE_SECONDS)
    ]
    if not uncached_pending_refs:
        _detect_cached_intro_groups(target, all_pending_refs, season_refs)
        return

    pending_refs = uncached_pending_refs[:MAX_WRITE_EPISODES_PER_JOB]
    if not pending_refs:
        return

    sample_refs = _select_template_refs(
        target,
        season_refs,
        FINGERPRINT_KIND_INTRO,
        priority_refs=pending_refs,
    )
    if len(sample_refs) < 2:
        logger.info("  ➜ [片头片尾提取] 《%s》第 %s 季可比对分集不足 2 集，暂不提取片头。", target.series_title, target.season_number)
        return

    detected = None
    fps: List[FingerprintRef] = []
    selected_sample_seconds = 0
    for sample_seconds in INTRO_SAMPLE_STEPS:
        fps = []
        for ref in sample_refs:
            fp = _load_or_extract_fingerprint(
                ref,
                requested_sample_seconds=sample_seconds,
                direct_url_cache=direct_url_cache,
            )
            if fp and len(fp.values) >= 40:
                fps.append(fp)
        if len(fps) < 2:
            continue
        try:
            detected = _detect_common_intro(
                fps,
                max_start_seconds=min(MAX_INTRO_START_SECONDS, sample_seconds),
            )
        except _FingerprintMatchTimeout:
            logger.warning(
                "  [intro] %s season %s batch match timed out; keep fingerprints and continue later batches.",
                target.series_title,
                target.season_number,
            )
            return
        if detected:
            selected_sample_seconds = sample_seconds
            break
    if len(fps) < 2:
        logger.info("  ➜ [片头片尾提取] 《%s》第 %s 季有效片头指纹不足，暂不写入。", target.series_title, target.season_number)
        return
    if not detected:
        logger.info("  [intro] %s season %s batch has no common intro; keep fingerprints and continue later batches.", target.series_title, target.season_number)
        return

    base_fp, base_start, base_end, ranges_by_sha1, matched = detected
    updated = 0
    attempted = 0
    for ref in pending_refs:
        attempted += 1
        own_range = ranges_by_sha1.get(ref.sha1)
        had_fingerprint = bool(own_range)
        if not own_range:
            try:
                own_range, had_fingerprint = _match_intro_with_progressive_windows(
                    ref,
                    base_fp,
                    base_start,
                    base_end,
                    selected_sample_seconds,
                    direct_url_cache=direct_url_cache,
                )
            except _FingerprintMatchTimeout:
                _mark_detection_failure(
                    ref,
                    FINGERPRINT_KIND_INTRO,
                    scope='episode',
                    reason='intro_episode_match_timeout',
                    sample_count=len(fps),
                    episode_count=len(season_refs),
                )
                logger.warning(
                    "  ➜ [片头片尾提取] 《%s》S%02dE%02d 片头匹配超时，当前算法版本不再自动重试。",
                    ref.series_title,
                    ref.season_number,
                    ref.episode_number,
                )
                continue
        if not own_range:
            if had_fingerprint:
                logger.info(
                    "  [intro] %s S%02dE%02d did not match this batch template; keep fingerprint for season cross-match.",
                    ref.series_title,
                    ref.season_number,
                    ref.episode_number,
                )
            else:
                logger.info(
                    "  ➜ [片头片尾提取] 《%s》S%02dE%02d 片头指纹尚未提取成功，保留后续重试。",
                    ref.series_title,
                    ref.season_number,
                    ref.episode_number,
                )
            continue
        if _write_intro_for_episode(ref, own_range[0], own_range[1]):
            updated += 1
        time.sleep(0)
    logger.info(
        "  ➜ [片头片尾提取] 《%s》第 %s 季已识别片头 %.1fs-%.1fs，本轮逐集回写 %s/%s 集（模板 %s/%s 集，窗口 %s 秒，命中 %s 组）。",
        target.series_title,
        target.season_number,
        base_start,
        base_end,
        updated,
        attempted,
        len(fps),
        len(sample_refs),
        selected_sample_seconds,
        matched,
    )


def _detect_cached_intro_groups(
    target: EpisodeRef,
    pending_refs: Sequence[EpisodeRef],
    season_refs: Sequence[EpisodeRef],
) -> None:
    fps: List[FingerprintRef] = []
    for ref in pending_refs:
        fp = _load_cached_fingerprint_ref(ref, FINGERPRINT_KIND_INTRO, SAMPLE_SECONDS)
        if fp and len(fp.values) >= 40:
            fps.append(fp)

    if len(fps) < 2:
        for fp in fps:
            _mark_detection_failure(
                fp.episode,
                FINGERPRINT_KIND_INTRO,
                scope='episode',
                reason='no_pair_intro_match',
                sample_count=len(fps),
                episode_count=len(season_refs),
            )
        logger.info(
            "  [intro] %s season %s has fewer than 2 pending cached fingerprints; cross-match skipped.",
            target.series_title,
            target.season_number,
        )
        return

    remaining = list(fps)
    ranges_by_sha1: Dict[str, Tuple[int, int]] = {}
    template_count = 0

    while len(remaining) >= 2:
        detected = None
        for base_index, base_fp in enumerate(remaining):
            for other_fp in remaining[base_index + 1:]:
                try:
                    detected = _detect_common_intro_for_base(
                        base_fp,
                        [base_fp, other_fp],
                        max_start_seconds=MAX_INTRO_START_SECONDS,
                        deadline=_new_match_deadline(EPISODE_MATCH_TIMEOUT_SECONDS),
                    )
                except _FingerprintMatchTimeout:
                    detected = None
                if detected:
                    break
            if detected:
                break

        if not detected:
            break

        base_fp, base_start, base_end, group_ranges, _matched = detected
        template_count += 1
        for fp in list(remaining):
            if fp.episode.sha1 in group_ranges:
                continue
            try:
                own_range = _match_intro_against_template(
                    base_fp,
                    fp,
                    base_start,
                    base_end,
                    max_start_seconds=MAX_INTRO_START_SECONDS,
                    deadline=_new_match_deadline(EPISODE_MATCH_TIMEOUT_SECONDS),
                )
            except _FingerprintMatchTimeout:
                own_range = None
            if own_range:
                group_ranges[fp.episode.sha1] = own_range

        ranges_by_sha1.update(group_ranges)
        remaining = [fp for fp in remaining if fp.episode.sha1 not in group_ranges]
        time.sleep(0)

    updated = 0
    attempted = 0
    refs_by_sha1 = {ref.sha1: ref for ref in pending_refs}
    for sha1, own_range in ranges_by_sha1.items():
        ref = refs_by_sha1.get(sha1)
        if not ref:
            continue
        attempted += 1
        if _write_intro_for_episode(ref, own_range[0], own_range[1]):
            updated += 1
        time.sleep(0)

    for fp in remaining:
        _mark_detection_failure(
            fp.episode,
            FINGERPRINT_KIND_INTRO,
            scope='episode',
            reason='no_pair_intro_match',
            sample_count=len(fps),
            episode_count=len(season_refs),
        )

    logger.info(
        "  [intro] %s season %s cached cross-match complete: %s templates, wrote %s/%s episodes, unmatched %s.",
        target.series_title,
        target.season_number,
        template_count,
        updated,
        attempted,
        len(remaining),
    )


def _schedule_next_batch_if_needed(
    target: EpisodeRef,
    season_refs: Sequence[EpisodeRef],
    trigger: str,
) -> None:
    has_intro = bool(_pending_detection_refs(season_refs, FINGERPRINT_KIND_INTRO, limit=1))
    has_credits = _credits_detection_enabled() and bool(
        _pending_detection_refs(season_refs, FINGERPRINT_KIND_CREDITS, limit=1)
    )
    if not has_intro and not has_credits:
        return

    def enqueue_later() -> None:
        try:
            _queue.put_nowait({
                "kind": "episode",
                "episode": _episode_to_payload(target),
                "trigger": trigger,
                "queued_at": time.time(),
                "continuation": True,
            })
            logger.debug(
                "  ➜ [片头片尾提取] 《%s》第 %s 季仍有未处理分集，已排入下一批（延迟 %s 秒）。",
                target.series_title,
                target.season_number,
                BATCH_CONTINUATION_DELAY_SECONDS,
            )
        except queue.Full:
            logger.warning(
                "  ➜ [片头片尾提取] 《%s》第 %s 季下一批入队失败：队列已满。",
                target.series_title,
                target.season_number,
            )

    timer = threading.Timer(BATCH_CONTINUATION_DELAY_SECONDS, enqueue_later)
    timer.daemon = True
    timer.start()


def _resolve_episode_ref(job: Dict[str, Any]) -> Optional[EpisodeRef]:
    episode = job.get("episode") if isinstance(job.get("episode"), dict) else None
    if episode:
        return _episode_from_payload(episode)

    item = job.get("item") if isinstance(job.get("item"), dict) else {}
    item_id = str(item.get("Id") or "").strip()
    sha1 = _norm_sha1(job.get("sha1"))
    return _load_episode_ref_by_item_id(
        item_id,
        sha1=sha1,
        fallback_item=item,
    )


def _load_episode_ref_by_item_id(item_id: str, *, sha1: str = "", fallback_item: Optional[Dict[str, Any]] = None) -> Optional[EpisodeRef]:
    item_id = str(item_id or "").strip()
    fallback_item = fallback_item or {}
    if not item_id:
        return None

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.tmdb_id, e.title, e.parent_series_tmdb_id, e.season_number, e.episode_number,
                       e.emby_item_ids_json, e.file_sha1_json, e.file_pickcode_json, e.asset_details_json,
                       p.title AS series_title, p.emby_item_ids_json AS series_emby_ids
                FROM media_metadata e
                LEFT JOIN media_metadata p
                  ON p.tmdb_id = e.parent_series_tmdb_id
                 AND p.item_type = 'Series'
                WHERE e.item_type = 'Episode'
                  AND e.emby_item_ids_json ? %s
                LIMIT 1
                """,
                (item_id,),
            )
            row = cur.fetchone()
            if not row:
                series_id = str(fallback_item.get("SeriesId") or "").strip()
                season_number = _to_int(fallback_item.get("ParentIndexNumber"))
                episode_number = _to_int(fallback_item.get("IndexNumber"))
                if series_id and season_number is not None and episode_number is not None:
                    cur.execute(
                        """
                        SELECT e.tmdb_id, e.title, e.parent_series_tmdb_id, e.season_number, e.episode_number,
                               e.emby_item_ids_json, e.file_sha1_json, e.file_pickcode_json, e.asset_details_json,
                               p.title AS series_title, p.emby_item_ids_json AS series_emby_ids
                        FROM media_metadata e
                        JOIN media_metadata p
                          ON p.tmdb_id = e.parent_series_tmdb_id
                         AND p.item_type = 'Series'
                        WHERE e.item_type = 'Episode'
                          AND p.emby_item_ids_json ? %s
                          AND e.season_number = %s
                          AND e.episode_number = %s
                        LIMIT 1
                        """,
                        (series_id, season_number, episode_number),
                    )
                    row = cur.fetchone()
            if not row:
                return None
            return _row_to_episode_ref(dict(row), item_id_hint=item_id, sha1_hint=sha1, fallback_item=fallback_item)


def _load_same_season_episode_refs(target: EpisodeRef) -> List[EpisodeRef]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.tmdb_id, e.title, e.parent_series_tmdb_id, e.season_number, e.episode_number,
                       e.emby_item_ids_json, e.file_sha1_json, e.file_pickcode_json, e.asset_details_json,
                       p.title AS series_title, p.emby_item_ids_json AS series_emby_ids
                FROM media_metadata e
                LEFT JOIN media_metadata p
                  ON p.tmdb_id = e.parent_series_tmdb_id
                 AND p.item_type = 'Series'
                WHERE e.item_type = 'Episode'
                  AND e.parent_series_tmdb_id = %s
                  AND e.season_number = %s
                  AND e.in_library = TRUE
                  AND jsonb_array_length(COALESCE(e.file_sha1_json, '[]'::jsonb)) > 0
                  AND jsonb_array_length(COALESCE(e.file_pickcode_json, '[]'::jsonb)) > 0
                ORDER BY
                  CASE WHEN e.episode_number = %s THEN 0 ELSE 1 END,
                  e.episode_number ASC
                """,
                (
                    target.series_tmdb_id,
                    target.season_number,
                    target.episode_number,
                ),
            )
            refs: List[EpisodeRef] = []
            seen_sha1 = set()
            for row in cur.fetchall() or []:
                row_dict = dict(row)
                is_target_row = _to_int(row_dict.get("episode_number")) == target.episode_number
                ref = _row_to_episode_ref(
                    row_dict,
                    item_id_hint=target.item_id if is_target_row else "",
                    sha1_hint=target.sha1 if is_target_row else "",
                )
                if not ref or not ref.sha1 or not ref.pick_code or ref.sha1 in seen_sha1:
                    continue
                seen_sha1.add(ref.sha1)
                refs.append(ref)
            return refs


def _season_has_intro_detection_cache(target: EpisodeRef) -> bool:
    for ref in _load_same_season_episode_refs(target):
        if _cache_has_intro(ref.sha1) or _cache_has_credits(ref.sha1):
            return True
    return False


def _needs_intro_backfill(ref: EpisodeRef, include_credits: bool) -> bool:
    return _needs_kind_detection(ref, FINGERPRINT_KIND_INTRO) or (
        include_credits and _needs_kind_detection(ref, FINGERPRINT_KIND_CREDITS)
    )


def _needs_kind_detection(ref: EpisodeRef, kind: str) -> bool:
    if not ref.sha1 or not ref.pick_code:
        return False
    if _cache_has_kind(ref, kind):
        return False
    return not _is_detection_failed(ref, kind)


def _cache_has_kind(ref: EpisodeRef, kind: str) -> bool:
    return _cache_has_credits(ref.sha1) if kind == FINGERPRINT_KIND_CREDITS else _cache_has_intro(ref.sha1)


def _pending_detection_refs(
    refs: Sequence[EpisodeRef],
    kind: str,
    *,
    limit: int = 0,
) -> List[EpisodeRef]:
    pending: List[EpisodeRef] = []
    for ref in refs:
        if _needs_kind_detection(ref, kind):
            pending.append(ref)
            if limit and len(pending) >= limit:
                break
    return pending


def _select_template_refs(
    target: EpisodeRef,
    refs: Sequence[EpisodeRef],
    kind: str,
    *,
    priority_refs: Optional[Sequence[EpisodeRef]] = None,
) -> List[EpisodeRef]:
    selected: List[EpisodeRef] = []
    seen = set()

    def add(ref: Optional[EpisodeRef]) -> None:
        if not ref or not ref.sha1 or not ref.pick_code or ref.sha1 in seen:
            return
        if _has_detection_failure(_failure_key_for_episode(ref), kind):
            return
        selected.append(ref)
        seen.add(ref.sha1)

    for ref in priority_refs or []:
        add(ref)
        if len(selected) >= MAX_WORK_EPISODES_PER_SEASON:
            break
    if len(selected) < MAX_WORK_EPISODES_PER_SEASON:
        for ref in refs:
            if ref.sha1 == target.sha1 or ref.episode_number == target.episode_number:
                add(ref)
                break
    for ref in refs:
        add(ref)
        if len(selected) >= MAX_WORK_EPISODES_PER_SEASON:
            break
    return selected[:MAX_WORK_EPISODES_PER_SEASON]


def _is_detection_failed(ref: EpisodeRef, kind: str) -> bool:
    return _has_detection_failure(_failure_key_for_season(ref), kind) or _has_detection_failure(_failure_key_for_episode(ref), kind)


def _failure_key_for_season(ref: EpisodeRef) -> str:
    return f"season:{ref.series_tmdb_id}:{int(ref.season_number or 0)}"


def _failure_key_for_episode(ref: EpisodeRef) -> str:
    return f"episode:{ref.sha1}"


def _has_detection_failure(failure_key: str, kind: str) -> bool:
    failure_key = str(failure_key or '').strip()
    kind = str(kind or FINGERPRINT_KIND_INTRO).strip().lower()
    if not failure_key:
        return False
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM p115_intro_detection_failures
                    WHERE failure_key=%s
                      AND kind=%s
                      AND algorithm_version=%s
                    LIMIT 1
                    """,
                    (failure_key, kind, INTRO_DETECTION_ALGORITHM_VERSION),
                )
                return bool(cur.fetchone())
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 读取失败标记失败 %s/%s: %s", kind, failure_key, e)
        return False


def _mark_detection_failure(
    ref: EpisodeRef,
    kind: str,
    *,
    scope: str,
    reason: str,
    sample_count: int = 0,
    episode_count: int = 0,
) -> None:
    kind = str(kind or FINGERPRINT_KIND_INTRO).strip().lower()
    scope = 'episode' if scope == 'episode' else 'season'
    failure_key = _failure_key_for_episode(ref) if scope == 'episode' else _failure_key_for_season(ref)
    if not failure_key:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO p115_intro_detection_failures(
                        failure_key, kind, algorithm_version, scope,
                        series_tmdb_id, season_number, sha1, reason,
                        sample_count, episode_count, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (failure_key, kind, algorithm_version)
                    DO UPDATE SET
                        reason = EXCLUDED.reason,
                        sample_count = EXCLUDED.sample_count,
                        episode_count = EXCLUDED.episode_count,
                        updated_at = NOW()
                    """,
                    (
                        failure_key,
                        kind,
                        INTRO_DETECTION_ALGORITHM_VERSION,
                        scope,
                        ref.series_tmdb_id,
                        int(ref.season_number or 0),
                        ref.sha1 if scope == 'episode' else '',
                        str(reason or '')[:200],
                        int(sample_count or 0),
                        int(episode_count or 0),
                    ),
                )
            conn.commit()
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 写入失败标记失败 %s/%s: %s", kind, failure_key, e)


def _should_count_fingerprint_extract_failure(kind: str, sample_seconds: int) -> bool:
    kind = str(kind or FINGERPRINT_KIND_INTRO).strip().lower()
    steps = CREDITS_SAMPLE_STEPS if kind == FINGERPRINT_KIND_CREDITS else INTRO_SAMPLE_STEPS
    first_step = int(steps[0])
    return abs(int(sample_seconds or 0) - first_step) <= FINGERPRINT_CACHE_TOLERANCE_SECONDS


def _record_fingerprint_extract_failure(
    ref: EpisodeRef,
    kind: str,
    *,
    sample_seconds: int,
    reason: str,
) -> int:
    if not ref or not _should_count_fingerprint_extract_failure(kind, sample_seconds):
        return 0
    kind = str(kind or FINGERPRINT_KIND_INTRO).strip().lower()
    failure_key = _failure_key_for_episode(ref)
    if not failure_key:
        return 0
    attempts = 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO p115_intro_fingerprint_failures(
                        failure_key, kind, algorithm_version, attempts,
                        last_sample_seconds, last_reason,
                        series_tmdb_id, season_number, sha1, updated_at
                    ) VALUES (%s, %s, %s, 1, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (failure_key, kind, algorithm_version)
                    DO UPDATE SET
                        attempts = p115_intro_fingerprint_failures.attempts + 1,
                        last_sample_seconds = EXCLUDED.last_sample_seconds,
                        last_reason = EXCLUDED.last_reason,
                        series_tmdb_id = EXCLUDED.series_tmdb_id,
                        season_number = EXCLUDED.season_number,
                        sha1 = EXCLUDED.sha1,
                        updated_at = NOW()
                    RETURNING attempts
                    """,
                    (
                        failure_key,
                        kind,
                        INTRO_DETECTION_ALGORITHM_VERSION,
                        int(sample_seconds or 0),
                        str(reason or '')[:200],
                        ref.series_tmdb_id,
                        int(ref.season_number or 0),
                        ref.sha1,
                    ),
                )
                row = cur.fetchone()
                attempts = int(row.get('attempts') if isinstance(row, dict) else row[0])
            conn.commit()
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 记录指纹提取失败次数失败 %s/%s: %s", kind, failure_key, e)
        return 0

    logger.debug(
        "  ➜ [片头片尾提取] 《%s》S%02dE%02d %s音频指纹提取失败计数：%s。",
        ref.series_title,
        ref.season_number,
        ref.episode_number,
        "片尾" if kind == FINGERPRINT_KIND_CREDITS else "片头",
        attempts,
    )
    return attempts


def _clear_fingerprint_extract_failure(ref: EpisodeRef, kind: str) -> None:
    if not ref:
        return
    kind = str(kind or FINGERPRINT_KIND_INTRO).strip().lower()
    failure_key = _failure_key_for_episode(ref)
    if not failure_key:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM p115_intro_fingerprint_failures
                    WHERE failure_key=%s
                      AND kind=%s
                      AND algorithm_version=%s
                    """,
                    (failure_key, kind, INTRO_DETECTION_ALGORITHM_VERSION),
                )
            conn.commit()
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 清理指纹提取失败次数失败 %s/%s: %s", kind, failure_key, e)


def _load_active_watchlist_episode_refs(
    limit: int = 50,
    selected_library_ids: Optional[Sequence[str]] = None,
) -> List[EpisodeRef]:
    scan_limit = min(max(limit * 120, 800), 8000)
    include_credits = _credits_detection_enabled()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.tmdb_id, e.title, e.parent_series_tmdb_id, e.season_number, e.episode_number,
                       e.emby_item_ids_json, e.file_sha1_json, e.file_pickcode_json, e.asset_details_json,
                       p.title AS series_title, p.emby_item_ids_json AS series_emby_ids
                FROM media_metadata e
                JOIN media_metadata p
                  ON p.tmdb_id = e.parent_series_tmdb_id
                 AND p.item_type = 'Series'
                WHERE e.item_type = 'Episode'
                  AND e.in_library = TRUE
                  AND e.season_number > 0
                  AND p.watching_status = ANY(%s)
                  AND jsonb_array_length(COALESCE(e.emby_item_ids_json, '[]'::jsonb)) > 0
                  AND jsonb_array_length(COALESCE(e.file_sha1_json, '[]'::jsonb)) > 0
                  AND jsonb_array_length(COALESCE(e.file_pickcode_json, '[]'::jsonb)) > 0
                ORDER BY e.last_updated_at DESC NULLS LAST
                LIMIT %s
                """,
                (["Watching", "Paused", "Pending"], scan_limit),
            )
            refs = []
            seen_seasons = set()
            for row in cur.fetchall() or []:
                ref = _row_to_episode_ref(dict(row))
                season_key = (ref.series_tmdb_id, ref.season_number) if ref else None
                if not ref or not season_key or season_key in seen_seasons:
                    continue
                if not _is_intro_library_allowed(ref, selected_library_ids):
                    continue
                if not _needs_intro_backfill(ref, include_credits):
                    continue
                seen_seasons.add(season_key)
                refs.append(ref)
                if len(refs) >= limit:
                    break
            return refs


def _load_library_episode_refs(
    limit: int = 50,
    selected_library_ids: Optional[Sequence[str]] = None,
) -> List[EpisodeRef]:
    """Load missing seasons from all in-library episodes in the selected libraries."""
    page_size = min(max(limit * 20, 500), 4000)
    include_credits = _credits_detection_enabled()
    refs: List[EpisodeRef] = []
    seen_seasons = set()
    offset = 0

    with get_db_connection() as conn:
        while len(refs) < limit:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT e.tmdb_id, e.title, e.parent_series_tmdb_id, e.season_number, e.episode_number,
                           e.emby_item_ids_json, e.file_sha1_json, e.file_pickcode_json, e.asset_details_json,
                           p.title AS series_title, p.emby_item_ids_json AS series_emby_ids
                    FROM media_metadata e
                    JOIN media_metadata p
                      ON p.tmdb_id = e.parent_series_tmdb_id
                     AND p.item_type = 'Series'
                    WHERE e.item_type = 'Episode'
                      AND e.in_library = TRUE
                      AND e.season_number > 0
                      AND jsonb_array_length(COALESCE(e.emby_item_ids_json, '[]'::jsonb)) > 0
                      AND jsonb_array_length(COALESCE(e.file_sha1_json, '[]'::jsonb)) > 0
                      AND jsonb_array_length(COALESCE(e.file_pickcode_json, '[]'::jsonb)) > 0
                    ORDER BY e.last_updated_at DESC NULLS LAST, e.tmdb_id
                    LIMIT %s OFFSET %s
                    """,
                    (page_size, offset),
                )
                rows = cur.fetchall() or []
            if not rows:
                break
            offset += len(rows)
            for row in rows:
                ref = _row_to_episode_ref(dict(row))
                season_key = (ref.series_tmdb_id, ref.season_number) if ref else None
                if not ref or not season_key or season_key in seen_seasons:
                    continue
                if not _is_intro_library_allowed(ref, selected_library_ids):
                    continue
                if not _needs_intro_backfill(ref, include_credits):
                    continue
                seen_seasons.add(season_key)
                refs.append(ref)
                if len(refs) >= limit:
                    break
            if len(rows) < page_size:
                break
    return refs


def _row_to_episode_ref(
    row: Dict[str, Any],
    *,
    item_id_hint: str = "",
    sha1_hint: str = "",
    fallback_item: Optional[Dict[str, Any]] = None,
) -> Optional[EpisodeRef]:
    fallback_item = fallback_item or {}
    emby_ids = _json_list(row.get("emby_item_ids_json"))
    sha1s = [_norm_sha1(x) for x in _json_list(row.get("file_sha1_json"))]
    pick_codes = [str(x or "").strip() for x in _json_list(row.get("file_pickcode_json"))]
    idx = 0
    if item_id_hint and item_id_hint in [str(x or "").strip() for x in emby_ids]:
        idx = [str(x or "").strip() for x in emby_ids].index(item_id_hint)
    elif sha1_hint and sha1_hint in sha1s:
        idx = sha1s.index(sha1_hint)

    item_id = item_id_hint or _list_get(emby_ids, idx) or _list_get(emby_ids, 0)
    sha1 = sha1_hint or _list_get(sha1s, idx) or _list_get(sha1s, 0)
    pick_code = _list_get(pick_codes, idx) or _list_get(pick_codes, 0)
    assets = _json_list(row.get("asset_details_json"))
    asset = _list_get(assets, idx)
    if not isinstance(asset, dict):
        asset = _list_get(assets, 0)
    if not isinstance(asset, dict):
        asset = {}
    source_library_id = str(asset.get("source_library_id") or fallback_item.get("_SourceLibraryId") or "").strip()
    item_path = str(asset.get("path") or asset.get("file_path") or fallback_item.get("Path") or "").strip()
    series_ids = _json_list(row.get("series_emby_ids"))
    series_id = str(fallback_item.get("SeriesId") or _list_get(series_ids, 0) or "").strip()
    season_number = _to_int(row.get("season_number"))
    episode_number = _to_int(row.get("episode_number"))
    if season_number is None or episode_number is None:
        return None
    return EpisodeRef(
        item_id=str(item_id or "").strip(),
        series_id=series_id,
        season_id=str(fallback_item.get("SeasonId") or "").strip(),
        series_tmdb_id=str(row.get("parent_series_tmdb_id") or "").strip(),
        title=str(row.get("title") or fallback_item.get("Name") or "").strip(),
        series_title=str(row.get("series_title") or fallback_item.get("SeriesName") or row.get("parent_series_tmdb_id") or "").strip(),
        season_number=int(season_number),
        episode_number=int(episode_number),
        sha1=_norm_sha1(sha1),
        pick_code=str(pick_code or "").strip(),
        source_library_id=source_library_id,
        path=item_path,
    )


def _episode_to_payload(ref: EpisodeRef) -> Dict[str, Any]:
    return {
        "item_id": ref.item_id,
        "series_id": ref.series_id,
        "season_id": ref.season_id,
        "series_tmdb_id": ref.series_tmdb_id,
        "title": ref.title,
        "series_title": ref.series_title,
        "season_number": ref.season_number,
        "episode_number": ref.episode_number,
        "sha1": ref.sha1,
        "pick_code": ref.pick_code,
        "source_library_id": ref.source_library_id,
        "path": ref.path,
    }


def _episode_from_payload(payload: Dict[str, Any]) -> Optional[EpisodeRef]:
    season_number = _to_int(payload.get("season_number"))
    episode_number = _to_int(payload.get("episode_number"))
    if season_number is None or episode_number is None:
        return None
    return EpisodeRef(
        item_id=str(payload.get("item_id") or "").strip(),
        series_id=str(payload.get("series_id") or "").strip(),
        season_id=str(payload.get("season_id") or "").strip(),
        series_tmdb_id=str(payload.get("series_tmdb_id") or "").strip(),
        title=str(payload.get("title") or "").strip(),
        series_title=str(payload.get("series_title") or payload.get("series_tmdb_id") or "").strip(),
        season_number=int(season_number),
        episode_number=int(episode_number),
        sha1=_norm_sha1(payload.get("sha1")),
        pick_code=str(payload.get("pick_code") or "").strip(),
        source_library_id=str(payload.get("source_library_id") or "").strip(),
        path=str(payload.get("path") or "").strip(),
    )


def _fingerprint_cache_ready(
    ref: EpisodeRef,
    kind: str,
    minimum_sample_seconds: int = 0,
) -> bool:
    cached = _load_fingerprint_cache(
        ref.sha1,
        kind=kind,
        minimum_sample_seconds=minimum_sample_seconds,
    )
    if not cached:
        return False
    _values, cached_sample_seconds = cached
    cached_coverage = _normalize_fingerprint_coverage(cached_sample_seconds, kind)
    return cached_coverage + FINGERPRINT_CACHE_TOLERANCE_SECONDS >= int(minimum_sample_seconds or 0)


def _load_cached_fingerprint_ref(
    ref: EpisodeRef,
    kind: str,
    requested_sample_seconds: int,
) -> Optional[FingerprintRef]:
    window_start, sample_seconds = _fingerprint_window(ref, kind, requested_sample_seconds)
    if sample_seconds < MIN_INTRO_SECONDS:
        return None
    cached = _load_fingerprint_cache(
        ref.sha1,
        kind=kind,
        minimum_sample_seconds=sample_seconds,
    )
    if not cached:
        return None
    cached_values, cached_sample_seconds = cached
    cached_coverage = _normalize_fingerprint_coverage(cached_sample_seconds, kind)
    if cached_coverage + FINGERPRINT_CACHE_TOLERANCE_SECONDS < sample_seconds:
        return None
    values = _slice_cached_fingerprint(
        cached_values,
        cached_coverage,
        sample_seconds,
        kind,
    )
    return FingerprintRef(ref, values, sample_seconds, window_start, kind)


def _load_or_extract_fingerprint(
    ref: EpisodeRef,
    *,
    kind: str = FINGERPRINT_KIND_INTRO,
    requested_sample_seconds: int = 0,
    direct_url_cache: Optional[Dict[str, Tuple[str, str]]] = None,
) -> Optional[FingerprintRef]:
    window_start, sample_seconds = _fingerprint_window(ref, kind, requested_sample_seconds)
    if sample_seconds < MIN_INTRO_SECONDS:
        return None
    if _is_detection_failed(ref, kind):
        return None
    cached = _load_fingerprint_cache(ref.sha1, kind=kind)
    if cached:
        cached_values, cached_sample_seconds = cached
        cached_coverage = _normalize_fingerprint_coverage(cached_sample_seconds, kind)
        if cached_coverage + FINGERPRINT_CACHE_TOLERANCE_SECONDS >= sample_seconds:
            values = _slice_cached_fingerprint(
                cached_values,
                cached_coverage,
                sample_seconds,
                kind,
            )
            if cached_coverage != cached_sample_seconds:
                _save_fingerprint_cache(ref.sha1, cached_values, cached_coverage, kind=kind)
            return FingerprintRef(ref, values, sample_seconds, window_start, kind)

    values = _extract_fingerprint(
        ref,
        window_start=window_start,
        sample_seconds=sample_seconds,
        kind=kind,
        direct_url_cache=direct_url_cache,
    )
    if not values:
        return None
    _clear_fingerprint_extract_failure(ref, kind)
    _save_fingerprint_cache(ref.sha1, values, sample_seconds, kind=kind)
    return FingerprintRef(ref, values, sample_seconds, window_start, kind)


def _normalize_fingerprint_coverage(sample_seconds: int, kind: str) -> int:
    value = max(1, int(sample_seconds or 0))
    steps = CREDITS_SAMPLE_STEPS if kind == FINGERPRINT_KIND_CREDITS else INTRO_SAMPLE_STEPS
    for step in steps:
        if abs(value - step) <= FINGERPRINT_CACHE_TOLERANCE_SECONDS:
            return step
    return value


def _slice_cached_fingerprint(
    values: List[int],
    cached_sample_seconds: int,
    requested_sample_seconds: int,
    kind: str,
) -> List[int]:
    if not values or cached_sample_seconds <= requested_sample_seconds:
        return values
    keep_count = max(
        1,
        min(len(values), int(round(len(values) * requested_sample_seconds / cached_sample_seconds))),
    )
    return values[-keep_count:] if kind == FINGERPRINT_KIND_CREDITS else values[:keep_count]


def _fingerprint_cache_key(sha1: str, kind: str) -> str:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return ""
    return sha1 if kind == FINGERPRINT_KIND_INTRO else f"{sha1}:{kind}"


def _load_fingerprint_cache(
    sha1: str,
    *,
    kind: str = FINGERPRINT_KIND_INTRO,
    minimum_sample_seconds: int = 0,
    expected_sample_seconds: int = 0,
) -> Optional[Tuple[List[int], int]]:
    cache_key = _fingerprint_cache_key(sha1, kind)
    if not cache_key:
        return None
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT fingerprint_zlib, fingerprint_count, sample_seconds FROM p115_intro_fingerprint_cache WHERE sha1=%s",
                    (cache_key,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                blob = bytes(row.get("fingerprint_zlib") or b"")
                values = json.loads(zlib.decompress(blob).decode("utf-8"))
                if not isinstance(values, list) or not values:
                    return None
                cached_sample_seconds = int(row.get("sample_seconds") or SAMPLE_SECONDS)
                if cached_sample_seconds < max(0, int(minimum_sample_seconds or 0)):
                    return None
                if expected_sample_seconds and abs(cached_sample_seconds - int(expected_sample_seconds)) > 2:
                    return None
                return [int(x) for x in values], cached_sample_seconds
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 读取%s指纹缓存失败 %s: %s", "片尾" if kind == FINGERPRINT_KIND_CREDITS else "片头", cache_key[:12], e)
        return None


def _save_fingerprint_cache(
    sha1: str,
    values: List[int],
    sample_seconds: int,
    *,
    kind: str = FINGERPRINT_KIND_INTRO,
) -> None:
    cache_key = _fingerprint_cache_key(sha1, kind)
    if not cache_key or not values:
        return
    try:
        from psycopg2 import Binary

        blob = zlib.compress(json.dumps(values, separators=(",", ":")).encode("utf-8"))
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO p115_intro_fingerprint_cache
                      (sha1, fingerprint_zlib, fingerprint_count, sample_seconds, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (sha1)
                    DO UPDATE SET
                      fingerprint_zlib = EXCLUDED.fingerprint_zlib,
                      fingerprint_count = EXCLUDED.fingerprint_count,
                      sample_seconds = EXCLUDED.sample_seconds,
                      updated_at = NOW()
                    """,
                    (cache_key, Binary(blob), len(values), int(sample_seconds or SAMPLE_SECONDS)),
                )
            conn.commit()
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 写入%s指纹缓存失败 %s: %s", "片尾" if kind == FINGERPRINT_KIND_CREDITS else "片头", cache_key[:12], e)


def _fingerprint_window(
    ref: EpisodeRef,
    kind: str,
    requested_sample_seconds: int = 0,
) -> Tuple[float, int]:
    requested_sample_seconds = max(
        1,
        int(
            requested_sample_seconds
            or (CREDITS_SAMPLE_STEPS[-1] if kind == FINGERPRINT_KIND_CREDITS else INTRO_SAMPLE_STEPS[-1])
        ),
    )
    if kind != FINGERPRINT_KIND_CREDITS:
        return 0.0, requested_sample_seconds
    runtime_seconds = _cached_runtime_seconds(ref.sha1)
    if runtime_seconds <= 0:
        return 0.0, 0
    sample_seconds = min(requested_sample_seconds, max(1, int(runtime_seconds)))
    return max(0.0, runtime_seconds - sample_seconds), sample_seconds


def _extract_fingerprint(
    ref: EpisodeRef,
    *,
    window_start: float = 0.0,
    sample_seconds: int = SAMPLE_SECONDS,
    kind: str = FINGERPRINT_KIND_INTRO,
    direct_url_cache: Optional[Dict[str, Tuple[str, str]]] = None,
) -> List[int]:
    from handler.p115_service import P115Service

    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [片头片尾提取] 115 客户端未初始化，无法提取指纹。")
        return []
    cached_url = direct_url_cache.get(ref.pick_code) if direct_url_cache is not None else None
    reused_url = bool(cached_url)
    if cached_url:
        direct_url, backend = cached_url
    else:
        try:
            direct_url, backend = client.resolve_download_url(ref.pick_code, user_agent=UA, return_backend=True)
        except Exception as e:
            logger.warning("  ➜ [片头片尾提取] 获取直链失败《%s》S%sE%s: %s", ref.series_title, ref.season_number, ref.episode_number, e)
            return []
        if direct_url_cache is not None and direct_url:
            direct_url_cache[ref.pick_code] = (direct_url, str(backend or ""))
    if not direct_url:
        logger.warning("  ➜ [片头片尾提取] 获取直链为空《%s》S%sE%s。", ref.series_title, ref.season_number, ref.episode_number)
        return []

    logger.info(
        "  ➜ [片头片尾提取] 正在提取%s音频指纹：%s S%02dE%02d（%s）",
        "片尾" if kind == FINGERPRINT_KIND_CREDITS else "片头",
        ref.series_title,
        ref.season_number,
        ref.episode_number,
        backend or "115",
    )
    audio_stream_index = _fingerprint_audio_stream_index(ref.sha1)
    values = _run_ffmpeg_chromaprint(
        direct_url,
        window_start=window_start,
        sample_seconds=sample_seconds,
        audio_stream_index=audio_stream_index,
    )
    countable_extract_failure = not reused_url
    if values:
        time.sleep(3)
        return values
    if reused_url and direct_url_cache is not None:
        direct_url_cache.pop(ref.pick_code, None)
        try:
            direct_url, backend = client.resolve_download_url(ref.pick_code, user_agent=UA, return_backend=True)
        except Exception:
            direct_url = ""
        if direct_url:
            direct_url_cache[ref.pick_code] = (direct_url, str(backend or ""))
            values = _run_ffmpeg_chromaprint(
                direct_url,
                window_start=window_start,
                sample_seconds=sample_seconds,
                audio_stream_index=audio_stream_index,
            )
            if values:
                return values
            countable_extract_failure = True
    if countable_extract_failure:
        _record_fingerprint_extract_failure(
            ref,
            kind,
            sample_seconds=sample_seconds,
            reason='ffmpeg_chromaprint_empty',
        )
    logger.warning("  ➜ [片头片尾提取] %s音频指纹提取失败：%s S%02dE%02d", "片尾" if kind == FINGERPRINT_KIND_CREDITS else "片头", ref.series_title, ref.season_number, ref.episode_number)
    return []


def _run_ffmpeg_chromaprint(
    direct_url: str,
    *,
    window_start: float = 0.0,
    sample_seconds: int = SAMPLE_SECONDS,
    audio_stream_index: Optional[int] = None,
) -> List[int]:
    if not shutil.which("ffmpeg"):
        return []
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-reconnect",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "5",
        "-headers",
        f"User-Agent: {UA}\r\n",
    ]
    if window_start > 0:
        cmd.extend(["-ss", f"{window_start:.3f}"])
    cmd.extend([
        "-i",
        direct_url,
        "-map",
        f"0:{audio_stream_index}" if audio_stream_index is not None else "0:a:0",
        "-t",
        str(max(1, int(sample_seconds or SAMPLE_SECONDS))),
        "-ac",
        "1",
        "-acodec",
        "pcm_s16le",
        "-ar",
        "16000",
        "-vn",
        "-sn",
        "-f",
        "chromaprint",
        "-fp_format",
        "raw",
        "-",
    ])
    return _parse_raw_chromaprint(_run_command(cmd))


def _fingerprint_audio_stream_index(sha1: str) -> Optional[int]:
    root = _mediainfo_root(_cached_mediainfo(sha1))
    if not isinstance(root, dict):
        return None
    source = root.get("MediaSourceInfo") if isinstance(root.get("MediaSourceInfo"), dict) else root
    streams = source.get("MediaStreams") if isinstance(source, dict) else None
    if not isinstance(streams, list):
        return None

    valid_streams = [stream for stream in streams if _is_valid_fingerprint_audio_stream(stream)]
    if not valid_streams:
        return None
    selected = next((stream for stream in valid_streams if stream.get("IsDefault") is True), valid_streams[0])
    try:
        return int(selected.get("Index"))
    except Exception:
        return None


def _is_valid_fingerprint_audio_stream(stream: Any) -> bool:
    if not isinstance(stream, dict):
        return False
    if str(stream.get("Type") or "").strip().lower() != "audio":
        return False
    codec = str(stream.get("Codec") or "").strip().lower()
    if not codec or codec in {"unknown", "none", "null", "data"} or codec.isdigit():
        return False
    try:
        return int(stream.get("Index")) >= 0
    except Exception:
        return False


def _run_command(cmd: List[str]) -> bytes:
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=FFMPEG_TIMEOUT_SECONDS, check=False)
        if proc.returncode != 0:
            _log_subprocess_debug(cmd[0], proc.stderr)
            return b""
        return proc.stdout or b""
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] %s 执行失败: %s", cmd[0] if cmd else "command", e)
        return b""


def _log_subprocess_debug(name: str, stderr: bytes) -> None:
    text = (stderr or b"").decode("utf-8", errors="ignore").strip().splitlines()
    if text:
        logger.debug("  ➜ [片头片尾提取] %s 输出: %s", name, text[-1][:240])


def _parse_raw_chromaprint(output: bytes) -> List[int]:
    """解析 ffmpeg chromaprint 的 raw 小端 int32 输出。"""
    if not output or len(output) < 80:
        return []
    usable = len(output) - (len(output) % 4)
    if usable <= 0:
        return []
    try:
        return list(struct.unpack(f"<{usable // 4}i", output[:usable]))
    except struct.error:
        return []


def _detect_common_intro(
    fps: List[FingerprintRef],
    *,
    max_start_seconds: float = MAX_INTRO_START_SECONDS,
) -> Optional[Tuple[FingerprintRef, float, float, Dict[str, Tuple[int, int]], int]]:
    if not fps:
        return None
    # 选一条最长稳定指纹作为本批模板，避免所有分集两两互比造成 O(n^2) CPU 扫描。
    base = max(fps, key=lambda fp: len(fp.values))
    return _detect_common_intro_for_base(
        base,
        fps,
        max_start_seconds=max_start_seconds,
        deadline=_new_match_deadline(COMMON_MATCH_TIMEOUT_SECONDS),
    )


def _detect_common_intro_for_base(
    base: FingerprintRef,
    fps: List[FingerprintRef],
    *,
    max_start_seconds: float,
    deadline: Optional[float] = None,
) -> Optional[Tuple[FingerprintRef, float, float, Dict[str, Tuple[int, int]], int]]:
    segments: List[Tuple[FingerprintRef, float, float, float, float]] = []
    for index, fp in enumerate(fps):
        if index and index % CPU_YIELD_INTERVAL == 0:
            _yield_match_cpu(deadline)
        _check_match_deadline(deadline)
        if fp is base:
            continue
        seg = _find_best_common_segment(base, fp, max_start_seconds=max_start_seconds, deadline=deadline)
        if seg:
            base_start, base_end, own_start, own_end = seg
            segments.append((fp, base_start, base_end, own_start, own_end))
    if not segments:
        return None

    base_start_median = float(median([segment[1] for segment in segments]))
    base_end_median = float(median([segment[2] for segment in segments]))
    # 只保留在模板集位置一致的匹配，每集最终仍使用自己的匹配区间。
    consensus = [
        segment
        for segment in segments
        if abs(segment[1] - base_start_median) <= 20
        and abs(segment[2] - base_end_median) <= 30
    ]
    if not consensus:
        return None

    ranges: Dict[str, List[Tuple[float, float]]] = {base.episode.sha1: []}
    for fp, base_start, base_end, own_start, own_end in consensus:
        ranges[base.episode.sha1].append((base_start, base_end))
        ranges.setdefault(fp.episode.sha1, []).append((own_start, own_end))

    base_range = _normalize_intro_range(base_start_median, base_end_median)
    if not base_range:
        return None
    base_start_template = base_range[0] / INTRO_TICKS
    base_end_template = base_range[1] / INTRO_TICKS

    normalized: Dict[str, Tuple[int, int]] = {}
    for sha1, values in ranges.items():
        start_seconds = float(median([value[0] for value in values]))
        end_seconds = float(median([value[1] for value in values]))
        own_range = _normalize_intro_range(start_seconds, end_seconds)
        if own_range:
            normalized[sha1] = own_range
    return (base, base_start_template, base_end_template, normalized, len(consensus)) if normalized else None


def _detect_and_write_credits(
    target: EpisodeRef,
    season_refs: Sequence[EpisodeRef],
    *,
    direct_url_cache: Optional[Dict[str, Tuple[str, str]]] = None,
) -> None:
    if _has_detection_failure(_failure_key_for_season(target), FINGERPRINT_KIND_CREDITS):
        return

    pending_refs = _pending_detection_refs(
        season_refs,
        FINGERPRINT_KIND_CREDITS,
        limit=MAX_WRITE_EPISODES_PER_JOB,
    )
    if not pending_refs:
        return

    # ================== 【跨批次全局偷懒模式】 ==================
    tail_durations = []
    for ref in season_refs:
        if ref.sha1 and not _needs_kind_detection(ref, FINGERPRINT_KIND_CREDITS):
            _, credits_start = _cached_chapter_ticks(ref.sha1)
            if credits_start is not None:
                tail_duration = _credits_tail_duration_ticks(ref.sha1, credits_start)
                if tail_duration is not None:
                    tail_durations.append(tail_duration)

    if len(tail_durations) >= 2:
        global_tail_ticks = float(median(tail_durations))
        logger.info(
            "  ➜ [片头片尾提取] 《%s》第 %s 季已有 %s 集提取过片尾，触发全局反推，跳过模板下载。",
            target.series_title,
            target.season_number,
            len(tail_durations)
        )
        
        updated = 0
        attempted = 0
        for ref in pending_refs:
            attempted += 1
            if _cache_has_intro(ref.sha1):
                own_start = _infer_credits_start_from_tail(ref, global_tail_ticks)
                if own_start is not None and _write_credits_for_episode(ref, own_start):
                    updated += 1
                    logger.info(
                        "  ➜ [片头片尾提取] 《%s》S%02dE%02d 已成功提取片头，触发全局反推模式，按本季片尾时长反推起点。",
                        ref.series_title,
                        ref.season_number,
                        ref.episode_number,
                    )
            else:
                _handle_credits_waiting_for_intro(
                    ref,
                    sample_count=0,
                    episode_count=len(season_refs),
                )
            time.sleep(0)
            
        logger.info(
            "  ➜ [片头片尾提取] 《%s》第 %s 季全局反推模式完成，本轮逐集回写 %s/%s 集。",
            target.series_title,
            target.season_number,
            updated,
            attempted,
        )
        return
    # ==================================================================

    # --- 以下为第一批次（尚无历史数据）的常规处理逻辑 ---
    sample_refs = _select_template_refs(
        target,
        season_refs,
        FINGERPRINT_KIND_CREDITS,
        priority_refs=pending_refs,
    )
    if len(sample_refs) < 2:
        logger.info("  ➜ [片头片尾提取] 《%s》第 %s 季可比对分集不足 2 集，暂不提取片尾。", target.series_title, target.season_number)
        return

    detected = None
    fps: List[FingerprintRef] = []
    selected_sample_seconds = 0
    for sample_seconds in CREDITS_SAMPLE_STEPS:
        fps = []
        for ref in sample_refs:
            fp = _load_or_extract_fingerprint(
                ref,
                kind=FINGERPRINT_KIND_CREDITS,
                requested_sample_seconds=sample_seconds,
                direct_url_cache=direct_url_cache,
            )
            if fp and len(fp.values) >= 40:
                fps.append(fp)
        if len(fps) < 2:
            continue
        try:
            candidate = _detect_common_credits(fps, max_start_seconds=sample_seconds)
        except _FingerprintMatchTimeout:
            _mark_detection_failure(
                target,
                FINGERPRINT_KIND_CREDITS,
                scope='season',
                reason='credits_match_timeout',
                sample_count=len(fps),
                episode_count=len(season_refs),
            )
            logger.warning(
                "  ➜ [片头片尾提取] 《%s》第 %s 季片尾公共片段匹配超时，当前算法版本不再自动重试。",
                target.series_title,
                target.season_number,
            )
            return
        if candidate:
            detected = candidate
            selected_sample_seconds = sample_seconds
            if not _credits_detection_touches_window_start(candidate, fps):
                break
            if sample_seconds >= CREDITS_SAMPLE_STEPS[-1]:
                detected = None
                logger.info(
                    "  ➜ [片头片尾提取] 《%s》第 %s 季片尾起点仍贴近 %s 秒窗口边界，跳过不安全片尾章节。",
                    target.series_title,
                    target.season_number,
                    sample_seconds,
                )
                break
            logger.info(
                "  ➜ [片头片尾提取] 《%s》第 %s 季片尾贴近 %s 秒窗口起点，扩大窗口继续识别。",
                target.series_title,
                target.season_number,
                sample_seconds,
            )
    if len(fps) < 2:
        logger.info("  ➜ [片头片尾提取] 《%s》第 %s 季有效片尾指纹不足，暂不写入。", target.series_title, target.season_number)
        return
    if not detected:
        _mark_detection_failure(
            target,
            FINGERPRINT_KIND_CREDITS,
            scope='season',
            reason='no_common_credits',
            sample_count=len(fps),
            episode_count=len(season_refs),
        )
        logger.info("  ➜ [片头片尾提取] 《%s》第 %s 季未找到稳定公共片尾，当前算法版本不再重试。", target.series_title, target.season_number)
        return
        
    base_fp, base_start, base_end, starts_by_sha1, matched = detected
    target_start = int(round((base_fp.window_start_seconds + base_start) * INTRO_TICKS))

    tail_durations_batch = []
    for sha1, start_ticks in starts_by_sha1.items():
        tail_duration = _credits_tail_duration_ticks(sha1, start_ticks)
        if tail_duration is not None:
            tail_durations_batch.append(tail_duration)

    inferred_tail_ticks = float(median(tail_durations_batch)) if len(tail_durations_batch) >= 2 else 0

    updated = 0
    attempted = 0
    for ref in pending_refs:
        attempted += 1
        own_start = starts_by_sha1.get(ref.sha1)
        had_fingerprint = own_start is not None

        if own_start is None:
            if inferred_tail_ticks > 0:
                if _cache_has_intro(ref.sha1):
                    own_start = _infer_credits_start_from_tail(ref, inferred_tail_ticks)
                    if own_start is not None:
                        had_fingerprint = True
                        logger.info(
                            "  ➜ [片头片尾提取] 《%s》S%02dE%02d 已成功提取片头，触发反推模式，按本季片尾时长反推起点。",
                            ref.series_title,
                            ref.season_number,
                            ref.episode_number,
                        )
                else:
                    _handle_credits_waiting_for_intro(
                        ref,
                        sample_count=len(fps),
                        episode_count=len(season_refs),
                    )
                    continue

            # 只有在连平均片尾时长都没算出来的情况下（极罕见），才会走到这里去下载音频比对
            if own_start is None:
                try:
                    own_start, had_fingerprint = _match_credits_with_progressive_windows(
                        ref,
                        base_fp,
                        base_start,
                        base_end,
                        selected_sample_seconds,
                        direct_url_cache=direct_url_cache,
                    )
                except _FingerprintMatchTimeout:
                    _mark_detection_failure(
                        ref,
                        FINGERPRINT_KIND_CREDITS,
                        scope='episode',
                        reason='credits_episode_match_timeout',
                        sample_count=len(fps),
                        episode_count=len(season_refs),
                    )
                    logger.warning(
                        "  ➜ [片头片尾提取] 《%s》S%02dE%02d 片尾匹配超时，当前算法版本不再自动重试。",
                        ref.series_title,
                        ref.season_number,
                        ref.episode_number,
                    )
                    continue
                    
        if own_start is None:
            if had_fingerprint:
                _mark_detection_failure(
                    ref,
                    FINGERPRINT_KIND_CREDITS,
                    scope='episode',
                    reason='no_episode_credits_match',
                    sample_count=len(fps),
                    episode_count=len(season_refs),
                )
            else:
                logger.info(
                    "  ➜ [片头片尾提取] 《%s》S%02dE%02d 片尾指纹尚未提取成功，保留后续重试。",
                    ref.series_title,
                    ref.season_number,
                    ref.episode_number,
                )
            continue
            
        if _write_credits_for_episode(ref, own_start):
            updated += 1
        time.sleep(0)
        
    logger.info(
        "  ➜ [片头片尾提取] 《%s》第 %s 季已识别片尾 %.1fs，本轮逐集回写 %s/%s 集（模板 %s/%s 集，窗口 %s 秒，命中 %s 组）。",
        target.series_title,
        target.season_number,
        target_start / INTRO_TICKS,
        updated,
        attempted,
        len(fps),
        len(sample_refs),
        selected_sample_seconds,
        matched,
    )


def _detect_common_credits(
    fps: List[FingerprintRef],
    *,
    max_start_seconds: float,
) -> Optional[Tuple[FingerprintRef, float, float, Dict[str, int], int]]:
    if not fps:
        return None
    base = max(fps, key=lambda fp: len(fp.values))
    return _detect_common_credits_for_base(
        base,
        fps,
        max_start_seconds=max_start_seconds,
        deadline=_new_match_deadline(COMMON_MATCH_TIMEOUT_SECONDS),
    )


def _detect_common_credits_for_base(
    base: FingerprintRef,
    fps: List[FingerprintRef],
    *,
    max_start_seconds: float,
    deadline: Optional[float] = None,
) -> Optional[Tuple[FingerprintRef, float, float, Dict[str, int], int]]:
    segments: List[Tuple[FingerprintRef, float, float, float, float]] = []
    for index, fp in enumerate(fps):
        if index and index % CPU_YIELD_INTERVAL == 0:
            _yield_match_cpu(deadline)
        _check_match_deadline(deadline)
        if fp is base:
            continue
        seg = _find_best_common_segment(base, fp, max_start_seconds=max_start_seconds, deadline=deadline)
        if seg:
            base_start, base_end, own_start, own_end = seg
            duration = min(base_end - base_start, own_end - own_start)
            if MIN_CREDITS_SECONDS <= duration <= MAX_CREDITS_SECONDS:
                segments.append((fp, base_start, base_end, own_start, own_end))
    if not segments:
        return None

    base_start_median = float(median([segment[1] for segment in segments]))
    base_end_median = float(median([segment[2] for segment in segments]))
    consensus = [
        segment
        for segment in segments
        if abs(segment[1] - base_start_median) <= 30
        and abs(segment[2] - base_end_median) <= 45
    ]
    if not consensus:
        return None

    starts: Dict[str, List[float]] = {base.episode.sha1: []}
    for fp, base_start, _base_end, own_start, _own_end in consensus:
        starts[base.episode.sha1].append(base.window_start_seconds + base_start)
        starts.setdefault(fp.episode.sha1, []).append(fp.window_start_seconds + own_start)

    normalized: Dict[str, int] = {}
    for sha1, values in starts.items():
        start_seconds = float(median(values))
        start_ticks = _normalize_credits_start_by_sha1(sha1, start_seconds)
        if start_ticks is not None:
            normalized[sha1] = start_ticks
    return (base, base_start_median, base_end_median, normalized, len(consensus)) if normalized else None


def _credits_detection_touches_window_start(
    detected: Tuple[FingerprintRef, float, float, Dict[str, int], int],
    fps: Sequence[FingerprintRef],
) -> bool:
    starts_by_sha1 = detected[3]
    matched = 0
    touching = 0
    for fp in fps:
        start_ticks = starts_by_sha1.get(fp.episode.sha1)
        if start_ticks is None:
            continue
        matched += 1
        relative_start = start_ticks / INTRO_TICKS - fp.window_start_seconds
        if relative_start <= CREDITS_BOUNDARY_MARGIN_SECONDS:
            touching += 1
    return matched > 0 and touching * 2 >= matched


def _normalize_intro_range(start_seconds: float, end_seconds: float) -> Optional[Tuple[int, int]]:
    start_seconds = float(start_seconds)
    end_seconds = float(end_seconds)
    if start_seconds < 3:
        start_seconds = 0.0
    if end_seconds - start_seconds < MIN_INTRO_SECONDS:
        return None
    if end_seconds - start_seconds > MAX_INTRO_SECONDS:
        end_seconds = start_seconds + MAX_INTRO_SECONDS
    return (
        int(round(start_seconds * INTRO_TICKS)),
        int(round(end_seconds * INTRO_TICKS)),
    )


def _match_intro_against_template(
    base: FingerprintRef,
    other: FingerprintRef,
    base_start_template: float,
    base_end_template: float,
    *,
    max_start_seconds: float = MAX_INTRO_START_SECONDS,
    deadline: Optional[float] = None,
) -> Optional[Tuple[int, int]]:
    matched_range = _find_template_matched_range(
        base,
        other,
        base_start_template,
        base_end_template,
        max_start_seconds=max_start_seconds,
        min_seconds=MIN_INTRO_SECONDS,
        deadline=deadline,
    )
    if not matched_range:
        return None
    return _normalize_intro_range(*matched_range)


def _match_intro_with_progressive_windows(
    ref: EpisodeRef,
    base: FingerprintRef,
    base_start_template: float,
    base_end_template: float,
    initial_sample_seconds: int,
    *,
    direct_url_cache: Optional[Dict[str, Tuple[str, str]]] = None,
) -> Tuple[Optional[Tuple[int, int]], bool]:
    had_fingerprint = False
    for sample_seconds in INTRO_SAMPLE_STEPS:
        if sample_seconds < initial_sample_seconds:
            continue
        fp = _load_or_extract_fingerprint(
            ref,
            requested_sample_seconds=sample_seconds,
            direct_url_cache=direct_url_cache,
        )
        if not fp or len(fp.values) < 40:
            continue
        had_fingerprint = True
        deadline = _new_match_deadline(EPISODE_MATCH_TIMEOUT_SECONDS)
        own_range = _match_intro_against_template(
            base,
            fp,
            base_start_template,
            base_end_template,
            max_start_seconds=min(MAX_INTRO_START_SECONDS, sample_seconds),
            deadline=deadline,
        )
        if own_range:
            return own_range, True
    return None, had_fingerprint


def _normalize_credits_start_by_sha1(sha1: str, start_seconds: float) -> Optional[int]:
    start_seconds = float(start_seconds)
    runtime_seconds = _cached_runtime_seconds(sha1)
    if runtime_seconds > 0:
        tail_seconds = runtime_seconds - start_seconds
        if (
            start_seconds <= 0
            or tail_seconds < MIN_CREDITS_SECONDS
            or tail_seconds > MAX_CREDITS_SECONDS + CREDITS_BOUNDARY_MARGIN_SECONDS
        ):
            return None
    if start_seconds <= 0:
        return None
    return int(round(start_seconds * INTRO_TICKS))


def _credits_tail_duration_ticks(sha1: str, start_ticks: int) -> Optional[float]:
    runtime_seconds = _cached_runtime_seconds(sha1)
    if runtime_seconds <= 0:
        return None
    tail_ticks = (runtime_seconds * INTRO_TICKS) - int(start_ticks)
    min_tail = MIN_CREDITS_SECONDS * INTRO_TICKS
    max_tail = (MAX_CREDITS_SECONDS + CREDITS_BOUNDARY_MARGIN_SECONDS) * INTRO_TICKS
    if min_tail <= tail_ticks <= max_tail:
        return float(tail_ticks)
    return None


def _infer_credits_start_from_tail(ref: EpisodeRef, tail_ticks: float) -> Optional[int]:
    runtime_seconds = _cached_runtime_seconds(ref.sha1)
    if runtime_seconds <= 0:
        return None
    start_seconds = runtime_seconds - (float(tail_ticks) / INTRO_TICKS)
    return _normalize_credits_start_by_sha1(ref.sha1, start_seconds)


def _handle_credits_waiting_for_intro(
    ref: EpisodeRef,
    *,
    sample_count: int,
    episode_count: int,
) -> None:
    if _needs_kind_detection(ref, FINGERPRINT_KIND_INTRO):
        logger.info(
            "  ➜ [片头片尾提取] 《%s》S%02dE%02d 暂无片头，跳过本次片尾反推，等待片头提取成功后重试。",
            ref.series_title,
            ref.season_number,
            ref.episode_number,
        )
        return

    _mark_detection_failure(
        ref,
        FINGERPRINT_KIND_CREDITS,
        scope='episode',
        reason='credits_inference_no_intro',
        sample_count=sample_count,
        episode_count=episode_count,
    )
    logger.info(
        "  ➜ [片头片尾提取] 《%s》S%02dE%02d 片头已终止或不可用，片尾反推同步终止。",
        ref.series_title,
        ref.season_number,
        ref.episode_number,
    )


def _match_credits_against_template(
    base: FingerprintRef,
    other: FingerprintRef,
    base_start_template: float,
    base_end_template: float,
    *,
    max_start_seconds: float,
    deadline: Optional[float] = None,
) -> Optional[int]:
    matched_range = _find_template_matched_range(
        base,
        other,
        base_start_template,
        base_end_template,
        max_start_seconds=max_start_seconds,
        min_seconds=MIN_CREDITS_SECONDS,
        deadline=deadline,
    )
    if not matched_range:
        return None
    own_start, own_end = matched_range
    duration = min(base_end_template - base_start_template, own_end - own_start)
    if duration < MIN_CREDITS_SECONDS or duration > MAX_CREDITS_SECONDS:
        return None
    return _normalize_credits_start_by_sha1(other.episode.sha1, other.window_start_seconds + own_start)


def _match_credits_with_progressive_windows(
    ref: EpisodeRef,
    base: FingerprintRef,
    base_start_template: float,
    base_end_template: float,
    initial_sample_seconds: int,
    *,
    direct_url_cache: Optional[Dict[str, Tuple[str, str]]] = None,
) -> Tuple[Optional[int], bool]:
    had_fingerprint = False
    for sample_seconds in CREDITS_SAMPLE_STEPS:
        if sample_seconds < initial_sample_seconds:
            continue
        fp = _load_or_extract_fingerprint(
            ref,
            kind=FINGERPRINT_KIND_CREDITS,
            requested_sample_seconds=sample_seconds,
            direct_url_cache=direct_url_cache,
        )
        if not fp or len(fp.values) < 40:
            continue
        had_fingerprint = True
        deadline = _new_match_deadline(EPISODE_MATCH_TIMEOUT_SECONDS)
        own_start = _match_credits_against_template(
            base,
            fp,
            base_start_template,
            base_end_template,
            max_start_seconds=sample_seconds,
            deadline=deadline,
        )
        if own_start is not None:
            relative_start = own_start / INTRO_TICKS - fp.window_start_seconds
            if relative_start > CREDITS_BOUNDARY_MARGIN_SECONDS:
                return own_start, True
            if sample_seconds >= CREDITS_SAMPLE_STEPS[-1]:
                return None, True
    return None, had_fingerprint


def _find_template_matched_range(
    base: FingerprintRef,
    other: FingerprintRef,
    base_start_seconds: float,
    base_end_seconds: float,
    *,
    max_start_seconds: float,
    min_seconds: float,
    deadline: Optional[float] = None,
) -> Optional[Tuple[float, float]]:
    _check_match_deadline(deadline)
    a = base.values
    b = other.values
    if not a or not b:
        return None
    base_spv = base.seconds_per_value
    other_spv = other.seconds_per_value
    if base_spv <= 0 or other_spv <= 0:
        return None

    min_len = max(8, int(float(min_seconds) / max(base_spv, other_spv)))
    template_start = max(0, int(round(float(base_start_seconds) / base_spv)))
    template_end = min(len(a), int(round(float(base_end_seconds) / base_spv)))
    template_len = template_end - template_start
    if template_len < min_len:
        return None

    k = min(FINGERPRINT_ANCHOR_WIDTH, min_len)
    max_other_start = min(max(len(b) - k, 0), int(float(max_start_seconds) / other_spv))
    if max_other_start < 0:
        return None

    index: Dict[Tuple[int, Tuple[int, ...]], List[int]] = {}
    for j in range(max_other_start + 1):
        if j and j % CPU_YIELD_INTERVAL == 0:
            _yield_match_cpu(deadline)
        if j + k > len(b):
            break
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = (mask_index, tuple(value & mask for value in b[j:j + k]))
            index.setdefault(key, []).append(j)

    min_overlap = max(min_len, int(template_len * 0.75))
    tolerance = max(2, int(8.0 / base_spv))
    best: Optional[Tuple[int, int, int]] = None
    best_score: Tuple[int, int, int] = (0, 0, 0)
    for i in range(template_start, max(template_start, template_end - k) + 1):
        if i and i % CPU_YIELD_INTERVAL == 0:
            _yield_match_cpu(deadline)
        if i + k > len(a):
            break
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = (mask_index, tuple(value & mask for value in a[i:i + k]))
            hits = index.get(key)
            if not hits:
                continue
            for j in hits[:8]:
                _check_match_deadline(deadline)
                right_len, right_matches = _extend_fuzzy_match(a, b, i, j, 1, deadline=deadline)
                left_len, left_matches = _extend_fuzzy_match(a, b, i - 1, j - 1, -1, deadline=deadline)
                total = left_len + right_len
                matches = left_matches + right_matches
                if total < min_len or matches / total < FINGERPRINT_MIN_MATCH_RATIO:
                    continue
                base_match_start = i - left_len
                other_match_start = j - left_len
                base_match_end = base_match_start + total
                overlap = min(base_match_end, template_end) - max(base_match_start, template_start)
                if overlap < min_overlap:
                    continue
                if base_match_start > template_start + tolerance:
                    continue
                if base_match_end < template_end - tolerance:
                    continue
                score = (overlap, matches, total)
                if score > best_score:
                    best_score = score
                    best = (base_match_start, other_match_start, base_match_end)

    if not best:
        return None
    base_match_start, other_match_start, _base_match_end = best
    own_start_idx = other_match_start + (template_start - base_match_start)
    own_end_idx = other_match_start + (template_end - base_match_start)
    if own_start_idx < 0 or own_end_idx <= own_start_idx or own_end_idx > len(b):
        return None
    return own_start_idx * other_spv, own_end_idx * other_spv


def _find_best_common_segment(
    base: FingerprintRef,
    other: FingerprintRef,
    *,
    max_start_seconds: float = MAX_INTRO_START_SECONDS,
    deadline: Optional[float] = None,
) -> Optional[Tuple[float, float, float, float]]:
    _check_match_deadline(deadline)
    a = base.values
    b = other.values
    if not a or not b:
        return None
    sec_per = max(base.seconds_per_value, other.seconds_per_value)
    min_len = max(8, int(MIN_INTRO_SECONDS / sec_per))
    max_start = max(min(len(a), len(b)) - min_len, 0)
    max_start_idx = min(max_start, int(max_start_seconds / sec_per))
    if max_start_idx <= 0:
        return None
    k = min(FINGERPRINT_ANCHOR_WIDTH, min_len)
    index: Dict[Tuple[int, Tuple[int, ...]], List[int]] = {}
    upper_b = min(len(b) - k + 1, max_start_idx + 1)
    for j in range(max(0, upper_b)):
        if j and j % CPU_YIELD_INTERVAL == 0:
            _yield_match_cpu(deadline)
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = (mask_index, tuple(value & mask for value in b[j:j + k]))
            index.setdefault(key, []).append(j)

    best_i = best_j = best_len = 0
    upper_a = min(len(a) - k + 1, max_start_idx + 1)
    for i in range(max(0, upper_a)):
        if i and i % CPU_YIELD_INTERVAL == 0:
            _yield_match_cpu(deadline)
        for mask_index, mask in enumerate(FINGERPRINT_ANCHOR_MASKS):
            key = (mask_index, tuple(value & mask for value in a[i:i + k]))
            hits = index.get(key)
            if not hits:
                continue
            for j in hits[:8]:
                _check_match_deadline(deadline)
                right_len, right_matches = _extend_fuzzy_match(a, b, i, j, 1, deadline=deadline)
                left_len, left_matches = _extend_fuzzy_match(a, b, i - 1, j - 1, -1, deadline=deadline)
                total = left_len + right_len
                matches = left_matches + right_matches
                if total < min_len or matches / total < FINGERPRINT_MIN_MATCH_RATIO:
                    continue
                start = i - left_len
                if total > best_len:
                    best_i = start
                    best_j = j - left_len
                    best_len = total

    if best_len < min_len:
        return None
    start_seconds = best_i * base.seconds_per_value
    end_seconds = (best_i + best_len) * base.seconds_per_value
    if start_seconds > max_start_seconds:
        return None
    other_start_seconds = best_j * other.seconds_per_value
    other_end_seconds = (best_j + best_len) * other.seconds_per_value
    return start_seconds, end_seconds, other_start_seconds, other_end_seconds


def _extend_fuzzy_match(
    a: Sequence[int],
    b: Sequence[int],
    i: int,
    j: int,
    direction: int,
    *,
    deadline: Optional[float] = None,
) -> Tuple[int, int]:
    """扩展已对齐的声纹片段，并容忍编码造成的少量位差。"""
    total = matches = misses = 0
    last_good_total = last_good_matches = 0
    while 0 <= i < len(a) and 0 <= j < len(b):
        total += 1
        distance = ((int(a[i]) ^ int(b[j])) & 0xFFFFFFFF).bit_count()
        if distance <= FINGERPRINT_HAMMING_DISTANCE:
            matches += 1
            misses = 0
            last_good_total = total
            last_good_matches = matches
        else:
            misses += 1
            if misses > FINGERPRINT_MAX_CONSECUTIVE_MISSES:
                break
        if total and total % (CPU_YIELD_INTERVAL * 16) == 0:
            _yield_match_cpu(deadline)
        i += direction
        j += direction
    return last_good_total, last_good_matches


def _write_intro_for_episode(ref: EpisodeRef, start_ticks: int, end_ticks: int) -> bool:
    chapters = [
        {
            "StartPositionTicks": int(start_ticks),
            "Name": "片头",
            "MarkerType": "IntroStart",
            "ChapterIndex": 0,
        },
        {
            "StartPositionTicks": int(end_ticks),
            "Name": "片头结束",
            "MarkerType": "IntroEnd",
            "ChapterIndex": 1,
        },
    ]
    changed = _merge_intro_into_mediainfo_cache(ref.sha1, chapters)
    applied = _apply_intro_to_emby(ref, start_ticks, end_ticks)
    if not applied:
        logger.debug(
            "  ➜ [片头片尾提取] 片头已写入缓存但注入 Emby 失败：%s S%02dE%02d Item=%s",
            ref.series_title,
            ref.season_number,
            ref.episode_number,
            ref.item_id,
        )
    if changed:
        try:
            from handler.shared_intro_service import upload_intro_for_cache_sha1
            from handler.p115_service import P115CacheManager

            cached_text = P115CacheManager.get_mediainfo_cache_text(ref.sha1)
            if cached_text:
                upload_intro_for_cache_sha1(
                    ref.sha1,
                    json.loads(cached_text),
                    file_name=f"{ref.series_title} S{ref.season_number:02d}E{ref.episode_number:02d}",
                    reason="etk_intro_detection",
                )
        except Exception as e:
            logger.debug("  ➜ [片头片尾提取] 上传共享片头失败 %s: %s", ref.sha1[:12], e)
    return changed or applied


def _write_credits_for_episode(ref: EpisodeRef, start_ticks: int) -> bool:
    normalized_ticks = _normalize_credits_start_by_sha1(ref.sha1, int(start_ticks) / INTRO_TICKS)
    if normalized_ticks is None:
        logger.info(
            "  ➜ [片头片尾提取] 《%s》S%02dE%02d 片尾起点不安全，跳过写入。",
            ref.series_title,
            ref.season_number,
            ref.episode_number,
        )
        return False
    start_ticks = normalized_ticks
    chapter = {
        "StartPositionTicks": int(start_ticks),
        "Name": "片尾",
        "MarkerType": "CreditsStart",
        "ChapterIndex": 0,
    }
    changed = _merge_credits_into_mediainfo_cache(ref.sha1, chapter)
    applied = _apply_credits_to_emby(ref, start_ticks)
    if not applied:
        logger.debug(
            "  ➜ [片头片尾提取] 片尾已写入缓存但注入 Emby 失败：%s S%02dE%02d Item=%s",
            ref.series_title,
            ref.season_number,
            ref.episode_number,
            ref.item_id,
        )
    return changed or applied


def _restore_cached_chapters_to_emby(refs: Sequence[EpisodeRef]) -> None:
    item_ids = [ref.item_id for ref in refs if ref.item_id]
    if not item_ids:
        return
    try:
        from handler import emby

        statuses = emby.get_etk_chapter_status(
            item_ids,
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY),
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID),
        )
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 核对 Emby 章节状态失败: %s", e)
        return
    if statuses is None:
        return

    for ref in refs:
        status = statuses.get(ref.item_id) or {}
        intro_range, credits_start = _cached_chapter_ticks(ref.sha1)
        if intro_range and not status.get("intro"):
            _apply_intro_to_emby(ref, intro_range[0], intro_range[1])
        if credits_start is not None and not status.get("credits"):
            _apply_credits_to_emby(ref, credits_start)


def _cached_chapter_ticks(sha1: str) -> Tuple[Optional[Tuple[int, int]], Optional[int]]:
    data = _cached_mediainfo(sha1)
    root = _mediainfo_root(data)
    chapters = root.get("Chapters") if isinstance(root, dict) else []
    if not isinstance(chapters, list):
        return None, None

    intro_start = intro_end = credits_start = None
    for chapter in chapters:
        if not isinstance(chapter, dict):
            continue
        try:
            ticks = int(float(chapter.get("StartPositionTicks")))
        except (TypeError, ValueError):
            continue
        marker = str(chapter.get("MarkerType") or "")
        if marker == "IntroStart":
            intro_start = ticks
        elif marker == "IntroEnd":
            intro_end = ticks
        elif marker == "CreditsStart":
            credits_start = ticks
    intro_range = (
        (intro_start, intro_end)
        if intro_start is not None and intro_end is not None and intro_end > intro_start >= 0
        else None
    )
    if credits_start is not None and _normalize_credits_start_by_sha1(sha1, credits_start / INTRO_TICKS) is None:
        credits_start = None
    return intro_range, credits_start


def _merge_intro_into_mediainfo_cache(sha1: str, chapters: List[Dict[str, Any]]) -> bool:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return False
    try:
        from psycopg2.extras import Json
        from handler.shared_intro_service import extract_intro_chapters, merge_intro_chapters

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1=%s FOR UPDATE",
                    (sha1,),
                )
                row = cur.fetchone()
                if not row or not row.get("mediainfo_json"):
                    logger.debug("  ➜ [片头片尾提取] SHA1 %s 没有媒体信息缓存，跳过章节回写。", sha1[:12])
                    return False
                mediainfo = row.get("mediainfo_json")
                if isinstance(mediainfo, str):
                    mediainfo = json.loads(mediainfo)
                current = extract_intro_chapters(mediainfo)
                if current == chapters:
                    return False
                merge_intro_chapters(mediainfo, chapters)
                cur.execute(
                    "UPDATE p115_mediainfo_cache SET mediainfo_json=%s WHERE sha1=%s",
                    (Json(mediainfo, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), sha1),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("  ➜ [片头片尾提取] 回写媒体信息缓存失败 %s: %s", sha1[:12], e)
        return False


def _merge_credits_into_mediainfo_cache(sha1: str, chapter: Dict[str, Any]) -> bool:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return False
    try:
        from psycopg2.extras import Json

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1=%s FOR UPDATE",
                    (sha1,),
                )
                row = cur.fetchone()
                if not row or not row.get("mediainfo_json"):
                    logger.debug("  ➜ [片头片尾提取] SHA1 %s 没有媒体信息缓存，跳过片尾回写。", sha1[:12])
                    return False
                mediainfo = row.get("mediainfo_json")
                if isinstance(mediainfo, str):
                    mediainfo = json.loads(mediainfo)
                root = _mediainfo_root(mediainfo)
                if root is None:
                    return False
                existing = root.get("Chapters")
                if not isinstance(existing, list):
                    existing = []
                current = next(
                    (
                        item for item in existing
                        if isinstance(item, dict) and str(item.get("MarkerType") or "") == "CreditsStart"
                    ),
                    None,
                )
                if current and int(float(current.get("StartPositionTicks") or 0)) == int(chapter.get("StartPositionTicks") or 0):
                    return False
                kept = [
                    item for item in existing
                    if not (isinstance(item, dict) and str(item.get("MarkerType") or "") == "CreditsStart")
                ]
                root["Chapters"] = kept + [dict(chapter)]
                cur.execute(
                    "UPDATE p115_mediainfo_cache SET mediainfo_json=%s WHERE sha1=%s",
                    (Json(mediainfo, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), sha1),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("  ➜ [片头片尾提取] 回写片尾到媒体信息缓存失败 %s: %s", sha1[:12], e)
        return False


def _apply_intro_to_emby(ref: EpisodeRef, start_ticks: int, end_ticks: int) -> bool:
    if not ref.item_id:
        return False
    try:
        from handler import emby

        result = emby.apply_etk_intro_chapters(
            ref.item_id,
            start_ticks,
            end_ticks,
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY),
        )
        return result is not None
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 写入 Emby 章节失败 Item=%s: %s", ref.item_id, e)
        return False


def _apply_credits_to_emby(ref: EpisodeRef, start_ticks: int) -> bool:
    if not ref.item_id:
        return False
    try:
        from handler import emby

        result = emby.apply_etk_credits_chapter(
            ref.item_id,
            start_ticks,
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL),
            config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY),
        )
        return result is not None
    except Exception as e:
        logger.debug("  ➜ [片头片尾提取] 写入 Emby 片尾章节失败 Item=%s: %s", ref.item_id, e)
        return False


def _cache_has_intro(sha1: str) -> bool:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return False
    try:
        from handler.shared_intro_service import extract_intro_chapters

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1=%s", (sha1,))
                row = cur.fetchone()
                data = row.get("mediainfo_json") if row else None
        if isinstance(data, str):
            data = json.loads(data)
        return bool(extract_intro_chapters(data))
    except Exception:
        return False


def _cache_has_credits(sha1: str) -> bool:
    _, credits_start = _cached_chapter_ticks(sha1)
    return credits_start is not None


def _cached_runtime_seconds(sha1: str) -> float:
    root = _mediainfo_root(_cached_mediainfo(sha1))
    if not isinstance(root, dict):
        return 0.0
    source = root.get("MediaSourceInfo") if isinstance(root.get("MediaSourceInfo"), dict) else root
    for value in (source.get("RunTimeTicks"), root.get("RunTimeTicks")):
        try:
            ticks = float(value or 0)
            if ticks > 0:
                return ticks / INTRO_TICKS
        except Exception:
            pass
    fmt = root.get("format") if isinstance(root.get("format"), dict) else {}
    try:
        duration = float(fmt.get("duration") or 0)
        if duration > 0:
            return duration
    except Exception:
        pass
    return 0.0


def _cached_mediainfo(sha1: str) -> Any:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return None
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1=%s", (sha1,))
                row = cur.fetchone()
                data = row.get("mediainfo_json") if row else None
        return json.loads(data) if isinstance(data, str) else data
    except Exception:
        return None


def _mediainfo_root(mediainfo: Any) -> Optional[Dict[str, Any]]:
    if isinstance(mediainfo, list) and mediainfo and isinstance(mediainfo[0], dict):
        return mediainfo[0]
    if isinstance(mediainfo, dict):
        return mediainfo
    return None


def _json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _list_get(values: Sequence[Any], index: int) -> Any:
    try:
        return values[index]
    except Exception:
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _norm_sha1(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if re.fullmatch(r"[A-F0-9]{40}", text) else ""
