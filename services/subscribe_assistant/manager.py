import hashlib
import json
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from gevent import spawn

from database import connection
from database import media_db, settings_db, watchlist_db
import handler.moviepilot as moviepilot
import tasks.helpers as helpers
from tasks.helpers import process_subscription_items_and_update_db
from tasks.p115_fingerprint_helpers import p115_fp_is_virtual_strm_target, p115_fp_read_strm_target

from .config import AssistantConfig, from_watchlist_config
from .engine import (
    CompletionSignal,
    build_scope,
    check_airing_gap_pause,
    check_pre_air_pause,
    evaluate_completion,
    parse_date,
    should_enter_pending,
)
from . import store

logger = logging.getLogger(__name__)


STATE_SUBSCRIBES = "subscribes"
STATE_TORRENTS = "torrents"
STATE_HISTORY_RECONCILE = "history_reconcile"
STATE_SNAPSHOT_RESTORE_SUPPRESSIONS = "snapshot_restore_suppressions"
SOURCE_PENDING_JUDGE = "pending_judge"
SOURCE_GUARD_VETO = "guard_veto"
SOURCE_DOWNLOAD_PENDING = "download_pending"
SOURCE_PRE_AIR = "pre_air"
SOURCE_AIRING_GAP = "airing_gap"
SOURCE_NO_DOWNLOAD = "no_download"
SOURCE_MANUAL_MP = "manual_mp"
MANUAL_CHANGE_GRACE_SECONDS = 3600


def get_config() -> AssistantConfig:
    watchlist_config = settings_db.get_setting("watchlist_config") or {}
    mp_config = settings_db.get_setting("mp_config") or {}
    assistant = mp_config.get("subscribe_assistant")
    if not isinstance(assistant, dict):
        assistant = watchlist_config.get("subscribe_assistant")
    cfg_source = {
        "auto_pending": watchlist_config.get("auto_pending") if isinstance(watchlist_config, dict) else {},
        "auto_pause": watchlist_config.get("auto_pause") if isinstance(watchlist_config, dict) else 0,
        "subscribe_assistant": assistant if isinstance(assistant, dict) else {},
    }
    cfg = from_watchlist_config(cfg_source)
    strategy = settings_db.get_setting("subscription_strategy_config") or {}
    sources = strategy.get("subscription_sources")
    if not mp_config.get("moviepilot_url") or (isinstance(sources, list) and "mp" not in sources):
        cfg.enabled = False
    return cfg


class SubscribeAssistantManager:
    def __init__(self, app_config: Dict[str, Any] = None, assistant_config: AssistantConfig = None):
        self.app_config = app_config or {}
        self.cfg = assistant_config or get_config()
        self._title_cache: Dict[str, str] = {}

    def sync_movie(self, *, tmdb_id: str, movie_name: str = "") -> int:
        """电影入库后，按本地洗版等级收口对应的 MP 订阅。"""
        if not self.cfg.enabled:
            return 0

        tmdb_id = str(tmdb_id or "").strip()
        if not tmdb_id:
            return 0

        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT title, washing_level
                        FROM media_metadata
                        WHERE tmdb_id = %s
                          AND item_type = 'Movie'
                          AND in_library = TRUE
                        LIMIT 1
                        """,
                        (tmdb_id,),
                    )
                    movie = cursor.fetchone()
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 查询电影入库优先级失败：TMDb %s -> %s", tmdb_id, e)
            return 0

        if not movie:
            return 0

        title = movie_name or movie.get("title") or tmdb_id
        washing_level = _safe_int(movie.get("washing_level"))
        subscriptions = [
            sub
            for sub in moviepilot.find_subscriptions(tmdb_id, config=self.app_config)
            if self._mp_subscription_item_type(sub) == "Movie"
        ]

        deleted = 0
        for sub in subscriptions:
            washing_enabled = _safe_int(sub.get("best_version")) == 1
            if washing_enabled and washing_level != 1:
                logger.debug(
                    "  ➜ [订阅助手] 《%s》MP 洗版订阅继续保留，本地优先级=%s。",
                    title,
                    washing_level or "未知",
                )
                continue

            subscribe_id = _safe_int(sub.get("id"))
            if subscribe_id <= 0:
                logger.warning("  ➜ [订阅助手] 《%s》MP 电影订阅缺少 ID，无法删除。", title)
                continue
            if not moviepilot.delete_subscription_by_id(subscribe_id, self.app_config):
                logger.warning("  ➜ [订阅助手] 《%s》MP 电影订阅删除失败：ID=%s。", title, subscribe_id)
                continue

            reason = "电影已入库" if not washing_enabled else "电影已入库且优先级达到 1"
            self._remove_subscription_state(subscribe_id, sub, reason="movie_library_closeout")
            self._clear_torrents_for_subscription(subscribe_id, reason)
            logger.info("  ➜ [订阅助手] 《%s》%s，已删除 MP 订阅。", title, reason)
            deleted += 1
        return deleted

    def sync_series(
        self,
        *,
        tmdb_id: str,
        series_name: str,
        series_details: Dict[str, Any],
        final_status: str,
        old_status: str = None,
        all_tmdb_episodes: List[Dict[str, Any]] = None,
        real_next_episode: Dict[str, Any] = None,
        triggering_episode_ids: List[str] = None,
    ) -> None:
        if not self.cfg.enabled:
            logger.debug("  ➜ [订阅助手] 已关闭，跳过 MoviePilot 同步。")
            return

        all_tmdb_episodes = all_tmdb_episodes or []
        local_progress_seasons = self._local_progress_seasons(tmdb_id)
        if not local_progress_seasons:
            logger.debug("  ➜ [订阅助手] 《%s》没有任何在库分集，跳过 MP 同步。", series_name or tmdb_id)
            return
        valid_seasons = [
            s for s in (series_details.get("seasons") or [])
            if _safe_int(s.get("season_number")) in local_progress_seasons
        ]
        if not valid_seasons:
            logger.debug("  ➜ [订阅助手] 《%s》没有本地已有进度的季，跳过 MP 同步。", series_name or tmdb_id)
            return
        latest_season = max(valid_seasons, key=lambda s: _safe_int(s.get("season_number")))
        latest_season_num = _safe_int(latest_season.get("season_number"))

        existing = moviepilot.find_subscriptions(tmdb_id, config=self.app_config)
        existing_by_season = {
            _safe_int(sub.get("season")): sub
            for sub in existing
            if _safe_int(sub.get("season")) > 0
        }

        target_seasons = self._target_seasons_for_sync(
            valid_seasons=valid_seasons,
            existing_by_season=existing_by_season,
            latest_season_num=latest_season_num,
            final_status=final_status,
        )
        if not target_seasons:
            return

        for season_info in target_seasons:
            season_num = _safe_int(season_info.get("season_number"))
            season_episodes = [
                ep for ep in all_tmdb_episodes
                if _safe_int(ep.get("season_number")) == season_num
            ]
            signal = self._completion_signal(
                tmdb_id=tmdb_id,
                season=season_num,
                series_details=series_details,
                episodes=all_tmdb_episodes,
                season_info=season_info,
            )
            local_season_completed = self._season_local_completed(tmdb_id, season_num, season_info, signal)
            decision_status = "Completed" if local_season_completed else final_status
            decision = self._decide_subscription_state(
                final_status=decision_status,
                series_details=series_details,
                season=season_num,
                season_episodes=season_episodes,
                signal=signal,
                real_next_episode=real_next_episode,
            )
            sub = existing_by_season.get(season_num)
            created_subscription = False
            if not sub and season_num == latest_season_num and final_status in ("Watching", "Paused", "Pending") and not local_season_completed:
                if self._triggering_episodes_are_virtual(triggering_episode_ids) or self._season_has_virtual_import(tmdb_id, season_num):
                    logger.info(
                        "  ➜ [订阅助手] 《%s》第 %s 季 仍处于虚拟入库状态，跳过自动创建 MP 订阅，等待正式入库。",
                        series_name,
                        season_num,
                    )
                    continue
                if self._should_create_full_washing_for_partial_completed_season(
                    tmdb_id,
                    season_num,
                    season_info,
                    signal,
                ):
                    decision = dict(decision)
                    decision["completed_full_washing"] = True
                    logger.info(
                        "  ➜ [订阅助手] 《%s》第 %s 季 已确认完结但本地未集齐，自动补订改为全集洗版。",
                        series_name,
                        season_num,
                    )
                sub = self._create_subscription(tmdb_id, series_name, season_num, decision, consume_quota=True)
                if sub:
                    existing_by_season[season_num] = sub
                    created_subscription = True
            if not sub:
                continue

            subscribe_id = _safe_int(sub.get("id"))
            if _safe_int(sub.get("best_version_full")) == 1:
                self._mark_full_washing_started(
                    subscribe_id,
                    tmdb_id,
                    season_num,
                    sub,
                    reason="existing_full_washing_subscription",
                )
                self._set_season_active_washing(tmdb_id, season_num, True, "检测到现有全集洗版订阅。")
            if self._has_recent_manual_mp_change(subscribe_id, "state"):
                logger.info(
                    "  ➜ [订阅助手] 《%s》S%s 检测到 MP 近期人工改状态，本轮暂不覆盖。",
                    series_name,
                    season_num,
                )
                continue

            self._update_source_state(subscribe_id, decision)
            total = self._target_total(decision, season_info, signal)
            if created_subscription and decision["mp_state"] == "R":
                logger.info(
                    "  ➜ [订阅助手] 《%s》第 %s 季新订阅已保留 N 状态，等待 MP 新增订阅任务搜索。",
                    series_name,
                    season_num,
                )
            else:
                self._remember_expected_mp_update(
                    subscribe_id,
                    fields=["state", "total_episode"] if total else ["state"],
                    expected_state=decision["mp_state"],
                    expected_total=total,
                )
                if moviepilot.update_subscription_status(
                    int(tmdb_id),
                    season_num,
                    decision["mp_state"],
                    self.app_config,
                    total_episodes=total,
                ):
                    logger.info(
                        "  ➜ [订阅助手] 《%s》第 %s 季 已同步 MP 状态=%s，总集数=%s，原因=%s",
                        series_name,
                        season_num,
                        decision["mp_state"],
                        total or "不改",
                        decision.get("reason") or "状态同步",
                    )

            if decision.get("snapshot"):
                self._sync_completed_full_washing(
                    tmdb_id=tmdb_id,
                    series_name=series_name,
                    season=season_num,
                    subscribe=sub,
                    season_info=season_info,
                    signal=signal,
                )
                scope = build_scope(tmdb_id, season_num, all_tmdb_episodes)
                store.upsert_snapshot(
                    tmdb_id=str(tmdb_id),
                    season_number=season_num,
                    subscribe_id=subscribe_id or None,
                    scope_total=scope.total,
                    scope={
                        "season": season_num,
                        "total": scope.total,
                        "high_risk": scope.high_risk,
                        "signals": signal.signals,
                    },
                    subscribe=sub,
                )

    def run_periodic_checks(self, limit: int = 100) -> Dict[str, int]:
        stats = {
            "released_pending": 0,
            "download_checked": 0,
            "no_download_checked": 0,
            "mp_subscriptions_synced": 0,
            "washing_timeouts": 0,
            "history_reconciled": 0,
            "snapshots_checked": 0,
            "snapshots_cleaned": 0,
            "delete_records_cleaned": 0,
        }
        stats["delete_records_cleaned"] = store.cleanup_delete_records()
        stats["snapshots_cleaned"] = store.cleanup_snapshots(self.cfg.snapshot_retention_days)
        stats["washing_timeouts"] = self.run_full_washing_timeout_check()
        stats["history_reconciled"] = self.run_transfer_history_reconcile(limit=limit)
        stats["mp_subscriptions_synced"] = self.run_moviepilot_subscription_sync()
        stats["no_download_checked"] = self.run_no_download_check()
        if self.cfg.download_monitor_enabled:
            stats["download_checked"] = self.run_download_check()
        if self.cfg.verify_enabled:
            stats["snapshots_checked"] = self.run_snapshot_verify(limit=limit)
        return stats

    def run_transfer_history_reconcile(self, limit: int = 100) -> int:
        now = time.time()
        retry_ttl = 6 * 3600
        reconcile_state = store.read_state(STATE_HISTORY_RECONCILE)
        if not isinstance(reconcile_state, dict):
            reconcile_state = {}
        last_retry = reconcile_state.get("last_retry") if isinstance(reconcile_state.get("last_retry"), dict) else {}

        page_size = max(20, min(_safe_int(limit, 100), 200))
        failed_records = moviepilot.list_transfer_history(
            self.app_config,
            page_limit=10,
            page_size=page_size,
            status=False,
        )
        recent_records = moviepilot.list_transfer_history(
            self.app_config,
            page_limit=5,
            page_size=page_size,
        )
        records = []
        seen_history_ids = set()
        for rec in list(failed_records or []) + list(recent_records or []):
            history_id = _safe_int(rec.get("id"))
            dedupe_key = history_id or self._transfer_history_fingerprint(
                rec,
                _first_text(rec, "pickcode", "pick_code", "pc", "PickCode"),
                _first_text(rec, "fileid", "file_id", "fid", "FileId"),
            )
            if dedupe_key in seen_history_ids:
                continue
            seen_history_ids.add(dedupe_key)
            records.append(rec)
        if not records:
            return 0

        keys = []
        redo_history_ids = []
        seen = set()
        failed_seen = 0
        retry_skipped = 0
        for rec in records:
            history_id = _safe_int(rec.get("id"))
            pick_code = _first_text(rec, "pickcode", "pick_code", "pc", "PickCode")
            file_id = _first_text(rec, "fileid", "file_id", "fid", "FileId")
            title = _first_text(rec, "title", "name", "src", "path", "dest")
            status_value = rec.get("status")
            errmsg = _first_text(rec, "errmsg", "error", "message")
            fingerprint = self._transfer_history_fingerprint(rec, pick_code, file_id)
            is_failed = (
                status_value is False
                or str(status_value).strip().lower() in {"false", "failed", "fail", "error", "0"}
                or bool(errmsg)
            )
            if is_failed:
                failed_seen += 1
            if now - float(last_retry.get(fingerprint) or 0) < retry_ttl:
                if is_failed:
                    retry_skipped += 1
                continue
            if is_failed and history_id > 0:
                redo_history_ids.append(history_id)
                last_retry[fingerprint] = now
                logger.info(
                    "  ➜ [订阅助手对账] 发现 MP 失败整理历史，准备重整理：%s（ID=%s，原因=%s）",
                    title or pick_code or file_id or "-",
                    history_id,
                    errmsg or "-",
                )
                if len(redo_history_ids) >= max(1, min(_safe_int(limit, 100), 20)):
                    break
                continue
            if not pick_code and not file_id:
                continue
            key = (pick_code, file_id)
            if key in seen:
                continue
            seen.add(key)
            keys.append({
                "history_id": history_id,
                "pick_code": pick_code,
                "file_id": file_id,
                "fingerprint": fingerprint,
                "title": title,
                "status": str(status_value),
            })
            if len(keys) >= max(1, _safe_int(limit, 100)):
                break
        if not keys and not redo_history_ids:
            if failed_seen or retry_skipped:
                logger.info(
                    "  ➜ [订阅助手对账] MP 整理历史扫描：失败页 %s 条，最近页 %s 条，合并 %s 条，失败 %s 条，缓存跳过 %s 条，触发 0 条。",
                    len(failed_records or []),
                    len(recent_records or []),
                    len(records),
                    failed_seen,
                    retry_skipped,
                )
            return 0

        fixed = 0
        retry_needed = bool(redo_history_ids)
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for item in keys:
                        pick_code = item.get("pick_code")
                        file_id = item.get("file_id")
                        if pick_code:
                            cursor.execute(
                                """
                                SELECT id, file_id, pick_code, original_name, status, fail_reason
                                FROM p115_organize_records
                                WHERE pick_code = %s
                                ORDER BY processed_at DESC
                                LIMIT 1
                                """,
                                (pick_code,),
                            )
                        elif file_id:
                            cursor.execute(
                                """
                                SELECT id, file_id, pick_code, original_name, status, fail_reason
                                FROM p115_organize_records
                                WHERE file_id = %s
                                ORDER BY processed_at DESC
                                LIMIT 1
                                """,
                                (file_id,),
                            )
                        else:
                            continue

                        row = cursor.fetchone()
                        if row and str(row.get("status") or "").lower() == "success":
                            continue

                        if row:
                            cursor.execute("DELETE FROM p115_organize_records WHERE id = %s", (row.get("id"),))
                            fixed += 1
                            retry_needed = True
                            last_retry[item.get("fingerprint")] = now
                            history_id = _safe_int(item.get("history_id"))
                            if history_id > 0:
                                redo_history_ids.append(history_id)
                            logger.info(
                                "  ➜ [订阅助手对账] 已清理 ETK 异常整理记录，准备重整理：%s（状态=%s，原因=%s）",
                                row.get("original_name") or item.get("title") or pick_code or file_id,
                                row.get("status") or "-",
                                row.get("fail_reason") or "-",
                            )
                        else:
                            history_id = _safe_int(item.get("history_id"))
                            if history_id > 0:
                                redo_history_ids.append(history_id)
                                fixed += 1
                                retry_needed = True
                                last_retry[item.get("fingerprint")] = now
                                logger.info(
                                    "  ➜ [订阅助手对账] MP 已有整理历史但 ETK 缺少整理记录，准备重整理：%s（ID=%s）",
                                    item.get("title") or pick_code or file_id,
                                    history_id,
                                )
                    conn.commit()
        except Exception as e:
            logger.warning(f"  ➜ [订阅助手对账] 巡检失败：{e}", exc_info=True)
            return fixed

        if retry_needed:
            last_retry = {
                k: v for k, v in (last_retry or {}).items()
                if k and now - float(v or 0) < retry_ttl
            }
            store.write_state(STATE_HISTORY_RECONCILE, {"last_retry": last_retry})
            if redo_history_ids:
                redo_ids = []
                for history_id in redo_history_ids:
                    if history_id not in redo_ids:
                        redo_ids.append(history_id)
                if moviepilot.redo_transfer_history(self.app_config, redo_ids):
                    fixed += max(0, len(redo_ids) - fixed)
                else:
                    try:
                        import task_manager
                        task_manager.trigger_115_organize_task(reason="subscribe_assistant_reconcile")
                    except Exception as e:
                        logger.warning(f"  ➜ [订阅助手对账] 触发 115 整理重跑失败：{e}", exc_info=True)
                logger.info(
                    "  ➜ [订阅助手对账] MP 整理历史扫描：失败页 %s 条，最近页 %s 条，合并 %s 条，失败 %s 条，缓存跳过 %s 条，触发 %s 条。",
                    len(failed_records or []),
                    len(recent_records or []),
                    len(records),
                    failed_seen,
                    retry_skipped,
                    len(redo_ids),
                )
                return fixed
            try:
                import task_manager
                task_manager.trigger_115_organize_task(reason="subscribe_assistant_reconcile")
            except Exception as e:
                logger.warning(f"  ➜ [订阅助手对账] 触发 115 整理重跑失败：{e}", exc_info=True)
        logger.info(
            "  ➜ [订阅助手对账] MP 整理历史扫描：失败页 %s 条，最近页 %s 条，合并 %s 条，失败 %s 条，缓存跳过 %s 条，触发 %s 条。",
            len(failed_records or []),
            len(recent_records or []),
            len(records),
            failed_seen,
            retry_skipped,
            len(redo_history_ids),
        )
        return fixed

    def run_full_washing_timeout_check(self) -> int:
        timeout_hours = _safe_int(self.cfg.full_washing_timeout_hours)
        if timeout_hours <= 0:
            return 0
        data = store.read_state(STATE_SUBSCRIBES)
        if not isinstance(data, dict) or not data:
            return 0

        now = time.time()
        changed = 0
        timeout_seconds = timeout_hours * 3600
        for key, task in list(data.items()):
            if not isinstance(task, dict):
                continue
            full_washing = bool(task.get("full_washing"))
            started_at = float(task.get("full_washing_started_at") or task.get("washing_timeout_started_at") or 0)
            if started_at <= 0 or now - started_at < timeout_seconds:
                continue
            tmdb_id = str(task.get("tmdb_id") or "").strip()
            season = _safe_int(task.get("season"))
            if not tmdb_id or season <= 0:
                self._clear_full_washing_state(_safe_int(task.get("subscribe_id")))
                continue

            info = task.get("subscribe_info") if isinstance(task.get("subscribe_info"), dict) else {}
            title = self._series_title(tmdb_id, info)
            total = _safe_int(info.get("total_episode") or info.get("total") or info.get("total_episodes"))
            if full_washing and total > 0 and self._season_consistency_ok(tmdb_id, season, total, title):
                self._set_season_active_washing(tmdb_id, season, False, "洗版超时检查时一致性已通过，收口。")
                self._clear_full_washing_state(_safe_int(task.get("subscribe_id")))
                logger.info("  ➜ [订阅助手] 《%s》S%s 洗版超时检查：一致性已通过，清理待洗版状态。", title, season)
                changed += 1
                continue

            sub = moviepilot.find_subscriptions(tmdb_id, season, self.app_config)
            subscribe_id = _safe_int(task.get("subscribe_id") or ((sub[0] or {}).get("id") if sub else 0))
            if not subscribe_id:
                self._mark_snapshot_restore_suppressed(
                    tmdb_id,
                    season,
                    0,
                    f"洗版超时 {timeout_hours} 小时，MP 订阅已不存在",
                )
                self._set_season_active_washing(tmdb_id, season, False, "洗版超时且 MP 订阅已不存在，停止洗版。")
                self._clear_full_washing_state(_safe_int(task.get("subscribe_id")))
                logger.warning(
                    "  ➜ [订阅助手] 《%s》S%s 洗版已超时 %s 小时，MP 订阅已不存在，已清理本地洗版状态。",
                    title,
                    season,
                    timeout_hours,
                )
                changed += 1
                continue
            self._mark_snapshot_restore_suppressed(
                tmdb_id,
                season,
                subscribe_id,
                f"洗版超时 {timeout_hours} 小时，按配置删除 MP 订阅",
            )
            if subscribe_id and moviepilot.delete_subscription_by_id(subscribe_id, self.app_config):
                self._set_season_active_washing(tmdb_id, season, False, "洗版超时，停止全集洗版。")
                self._remove_subscription_state(subscribe_id, info, reason="full_washing_timeout")
                self._clear_torrents_for_subscription(subscribe_id, "洗版超时删除 MP 订阅")
                self._clear_full_washing_state(subscribe_id)
                logger.warning(
                    "  ➜ [订阅助手] 《%s》S%s 洗版已超时 %s 小时，已删除 MP 洗版订阅。",
                    title,
                    season,
                    timeout_hours,
                )
                changed += 1
            else:
                self._clear_snapshot_restore_suppression(tmdb_id, season, subscribe_id)
                logger.warning(
                    "  ➜ [订阅助手] 《%s》S%s 洗版已超时 %s 小时，但删除 MP 洗版订阅失败。",
                    title,
                    season,
                    timeout_hours,
                )
        return changed

    def run_no_download_check(self) -> int:
        wait_days = _safe_int(self.cfg.tv_no_download_days)
        actions = {str(x).strip().lower() for x in (self.cfg.no_download_actions or []) if str(x).strip()}
        if wait_days <= 0 or not actions:
            return 0
        data = store.read_state(STATE_SUBSCRIBES)
        if not isinstance(data, dict) or not data:
            return 0

        now = time.time()
        wait_seconds = wait_days * 86400
        changed = 0
        for key, task in list(data.items()):
            if not isinstance(task, dict) or task.get("deleted") or task.get("full_washing"):
                continue
            subscribe_id = _safe_int(task.get("subscribe_id") or key)
            tmdb_id = str(task.get("tmdb_id") or "").strip()
            season = _safe_int(task.get("season"))
            info = task.get("subscribe_info") if isinstance(task.get("subscribe_info"), dict) else {}
            state = str(task.get("mp_state") or info.get("state") or "").strip().upper()
            if state and state != "R":
                continue
            if float(task.get("download_started_at") or 0) > 0:
                continue
            started_at = float(task.get("created_at") or task.get("updated_at") or 0)
            if subscribe_id <= 0 or not tmdb_id or season <= 0 or started_at <= 0:
                continue
            if now - started_at < wait_seconds:
                continue
            if now - float(task.get("no_download_action_at") or 0) < 86400:
                continue

            title = self._series_title(tmdb_id, info)
            action_text = []
            if "search" in actions and moviepilot.search_subscription(subscribe_id, self.app_config):
                action_text.append("补搜")
            target_state = "P" if "pending" in actions else ("S" if "pause" in actions else "")
            if target_state:
                if moviepilot.update_subscription_status(int(tmdb_id), season, target_state, self.app_config):
                    action_text.append("转待定" if target_state == "P" else "暂停")
                    task["expected_mp_update"] = {
                        "fields": ["state"],
                        "state": target_state,
                        "total_episode": None,
                        "updated_at": now,
                    }
                    task["mp_state"] = target_state
                    info["state"] = target_state
                    task["subscribe_info"] = info
            if not action_text:
                continue

            sources = task.get("active_sources") if isinstance(task.get("active_sources"), dict) else {}
            sources[SOURCE_NO_DOWNLOAD] = f"{wait_days} 天无下载，已执行：{','.join(action_text)}"
            task["active_sources"] = sources
            task["no_download_action_at"] = now
            task["last_reason"] = sources[SOURCE_NO_DOWNLOAD]
            task["updated_at"] = now
            data[str(subscribe_id)] = task
            changed += 1
            logger.info(
                "  ➜ [订阅助手] 《%s》S%s 已等待 %s 天无下载，执行动作：%s。",
                title,
                season,
                wait_days,
                "、".join(action_text),
            )
        if changed:
            store.write_state(STATE_SUBSCRIBES, data)
        return changed

    def run_download_check(self) -> int:
        torrents = store.read_state(STATE_TORRENTS)
        live = moviepilot.get_download_tasks_for_monitoring(self.app_config)
        live_by_hash = {
            str(task.get("hash") or task.get("hashString") or task.get("id") or "").lower(): task
            for task in live or []
        }
        if not live_by_hash:
            if torrents:
                logger.warning("  ➜ [订阅助手] 未获取到下载器任务，保留下载监控记录，避免误判为手工删除。")
            return 0
        self._register_untracked_download_tasks(torrents, live)
        torrents = store.read_state(STATE_TORRENTS)
        if not torrents:
            return 0
        changed = 0
        now = time.time()

        def updater(data):
            nonlocal changed
            for torrent_hash, task in list(data.items()):
                info = live_by_hash.get(str(torrent_hash).lower())
                if not info:
                    if self.cfg.manual_delete_listen:
                        self._clear_download_pending(task.get("subscribe_id"), torrent_hash, "下载任务已不存在")
                        data.pop(torrent_hash, None)
                        changed += 1
                    continue
                if self._download_task_has_excluded_tag(info):
                    self._clear_download_pending(task.get("subscribe_id"), torrent_hash, "下载任务命中排除标签")
                    data.pop(torrent_hash, None)
                    changed += 1
                    logger.info("  ➜ [订阅助手] 下载任务 %s 命中排除标签，跳过下载巡检删种。", str(torrent_hash)[:8])
                    continue
                tracker_keyword = self._download_task_tracker_keyword(info) if self.cfg.tracker_response_listen else ""
                if tracker_keyword:
                    if self._delete_monitored_download_task(torrent_hash, task, f"Tracker响应命中：{tracker_keyword}"):
                        data.pop(torrent_hash, None)
                        changed += 1
                    continue
                progress = _progress_value(info)
                baseline = float(task.get("baseline_progress") or 0)
                baseline_at = float(task.get("baseline_at") or task.get("time") or now)
                if progress >= 100 or info.get("state") in ("已完成", "completed", "COMPLETE"):
                    self._clear_download_pending(task.get("subscribe_id"), torrent_hash, "下载已完成")
                    data.pop(torrent_hash, None)
                    changed += 1
                    continue
                if now - baseline_at < self.cfg.download_timeout_minutes * 60:
                    continue
                if progress - baseline >= self.cfg.download_progress_threshold:
                    task["baseline_progress"] = progress
                    task["baseline_at"] = now
                    task["retry_count"] = 0
                    data[torrent_hash] = task
                    continue
                retry_count = _safe_int(task.get("retry_count")) + 1
                task["retry_count"] = retry_count
                task["baseline_at"] = now
                data[torrent_hash] = task
                if retry_count >= self.cfg.download_retry_limit:
                    logger.warning("  ➜ [订阅助手] 下载任务 %s 连续停滞，已达到人工保护阈值。", str(torrent_hash)[:8])
                    continue
                if self._delete_monitored_download_task(torrent_hash, task, "下载超时停滞"):
                    data.pop(torrent_hash, None)
                    changed += 1
            return data

        store.update_state(STATE_TORRENTS, updater)
        return changed

    def _register_untracked_download_tasks(self, torrents: Dict[str, Any], live: List[Dict[str, Any]]) -> int:
        known_hashes = {str(value).lower() for value in (torrents or {})}
        candidates = []
        for info in live or []:
            torrent_hash = str(info.get("hash") or info.get("hashString") or info.get("id") or "").lower()
            media = info.get("media") if isinstance(info.get("media"), dict) else {}
            if info.get("_mp_active") and torrent_hash and torrent_hash not in known_hashes and media.get("tmdbid"):
                candidates.append((torrent_hash, info, media))
        if not candidates:
            return 0

        subscriptions = moviepilot.list_subscriptions(self.app_config) or []
        registered = 0
        for torrent_hash, info, media in candidates:
            tmdb_id = str(media.get("tmdbid") or "")
            season = _safe_int(media.get("season"))
            matches = [
                sub for sub in subscriptions
                if str(sub.get("tmdbid") or sub.get("tmdb_id") or "") == tmdb_id
                and (season <= 0 or _safe_int(sub.get("season")) == season)
            ]
            if len(matches) != 1:
                logger.warning(
                    "  ➜ [订阅助手] 未登记的 MP 下载任务 %s 无法唯一匹配订阅，跳过自动补登记。",
                    torrent_hash[:8],
                )
                continue

            episode = _safe_int(media.get("episode"))
            added_on = _safe_int(info.get("added_on")) or int(time.time())
            self.mark_download_started(
                _safe_int(matches[0].get("id")),
                torrent_hash,
                tmdb_id=tmdb_id,
                season=season or None,
                episodes=[episode] if episode > 0 else [],
                title=media.get("title") or info.get("title") or info.get("name") or "",
                progress=_progress_value(info),
                baseline_at=added_on,
                time=added_on,
                source="moviepilot_active_reconcile",
            )
            known_hashes.add(torrent_hash)
            registered += 1
            logger.info(
                "  ➜ [订阅助手] 已补登记 MP 下载任务：订阅 %s，%s，hash=%s。",
                matches[0].get("id"),
                media.get("title") or info.get("name") or "-",
                torrent_hash[:8],
            )
        return registered

    def run_moviepilot_subscription_sync(self) -> int:
        subscriptions = moviepilot.list_subscriptions(self.app_config) or []
        changed = 0
        scanned = 0
        for sub in subscriptions:
            if not isinstance(sub, dict):
                continue
            scanned += 1
            subscribe_id = _safe_int(sub.get("id"))
            if subscribe_id:
                self._remember_subscription(subscribe_id, sub, reason="mp.history_sync")
            if self._sync_mp_subscription_to_etk(sub, reason="history"):
                changed += 1
        if scanned or changed:
            logger.info("  ➜ [订阅助手] MP 历史订阅同步完成：扫描=%s，同步=%s。", scanned, changed)
        return changed

    def run_snapshot_verify(self, limit: int = 100) -> int:
        checked = 0
        for snapshot in store.get_snapshots_due(self.cfg.verify_interval_hours, limit=limit):
            tmdb_id = str(snapshot.get("tmdb_id") or "")
            season = _safe_int(snapshot.get("season_number"))
            old_total = _safe_int(snapshot.get("scope_total"))
            if not tmdb_id or season <= 0:
                store.mark_snapshot_checked(_safe_int(snapshot.get("id")))
                checked += 1
                continue

            locked = self._season_total_locked(tmdb_id, season)
            if locked:
                title = self._series_title(tmdb_id, snapshot.get("subscribe_json"))
                logger.info(
                    "  ➜ [订阅助手] 《%s》第 %s 季 已由豆瓣/手动锁定为 %s 集，自动纠错跳过 MP 恢复动作。",
                    title,
                    season,
                    locked.get("count") or "未知",
                )
                store.mark_snapshot_checked(_safe_int(snapshot.get("id")))
                checked += 1
                continue

            fixed = self._repair_snapshot_subscription(snapshot, tmdb_id, season, old_total)
            if fixed:
                logger.info(
                    "  ➜ [订阅助手] 已根据完成快照纠正《%s》第 %s 季的 MP 订阅。",
                    self._series_title(tmdb_id, snapshot.get("subscribe_json")),
                    season,
                )
            store.mark_snapshot_checked(_safe_int(snapshot.get("id")))
            checked += 1
        return checked

    def handle_moviepilot_event(self, event_type: str, payload: Dict[str, Any]) -> bool:
        if not self.cfg.enabled:
            return False
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        try:
            if event_type == "download.added":
                return self._handle_download_added(data)
            if event_type == "subscribe.added":
                return self._handle_subscribe_added(data)
            if event_type == "subscribe.modified":
                return self._handle_subscribe_modified(data)
            if event_type == "subscribe.deleted":
                return self._handle_subscribe_deleted(data)
            if event_type == "subscribe.complete":
                return self._handle_subscribe_complete(data)
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 处理 MP Webhook 事件失败：%s -> %s", event_type, e, exc_info=True)
            return False
        return False

    def _handle_download_added(self, data: Dict[str, Any]) -> bool:
        torrent_hash = str(data.get("hash") or "").lower().strip()
        if not torrent_hash:
            return False
        source_info = self._parse_subscribe_source(data.get("source"))
        context = data.get("context") if isinstance(data.get("context"), dict) else {}
        meta = context.get("meta_info") if isinstance(context.get("meta_info"), dict) else {}
        torrent = context.get("torrent_info") if isinstance(context.get("torrent_info"), dict) else {}
        media = context.get("media_info") if isinstance(context.get("media_info"), dict) else {}
        subscribe_id = _safe_int(source_info.get("id") or data.get("subscribe_id"))
        if subscribe_id <= 0:
            logger.debug("  ➜ [订阅助手] download.added 未携带订阅 ID，跳过下载状态登记：%s", torrent_hash[:8])
            return False
        season = _safe_int(source_info.get("season") or meta.get("begin_season") or media.get("season"))
        episodes = data.get("episodes") or meta.get("episode_list") or []
        if not isinstance(episodes, list):
            episodes = [episodes]
        metadata = {
            "tmdb_id": str(source_info.get("tmdbid") or media.get("tmdb_id") or ""),
            "season": season or None,
            "episodes": [_safe_int(x) for x in episodes if _safe_int(x) > 0],
            "title": torrent.get("title") or meta.get("title") or media.get("title") or source_info.get("name") or "",
            "page_url": torrent.get("page_url") or "",
            "enclosure": torrent.get("enclosure") or "",
            "site_name": torrent.get("site_name") or "",
            "size": torrent.get("size"),
            "username": data.get("username") or "",
            "source": "moviepilot_webhook",
        }
        self.mark_download_started(subscribe_id, torrent_hash, **metadata)
        self._remember_subscription(subscribe_id, source_info or {
            "id": subscribe_id,
            "tmdbid": metadata["tmdb_id"],
            "season": season,
            "name": metadata["title"],
        }, reason="download.added")
        logger.info(
            "  ➜ [订阅助手] 已接收 MP 下载事件：订阅 %s，%s 第 %s 季，hash=%s。",
            subscribe_id,
            self._series_title(metadata["tmdb_id"], source_info or metadata),
            season or "-",
            torrent_hash[:8],
        )
        return True

    def _handle_subscribe_added(self, data: Dict[str, Any]) -> bool:
        subscribe_id = _safe_int(data.get("subscribe_id"))
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        media = data.get("mediainfo") if isinstance(data.get("mediainfo"), dict) else {}
        if not info:
            info = {
                "id": subscribe_id,
                "name": media.get("title"),
                "type": media.get("type"),
                "tmdbid": media.get("tmdb_id"),
                "imdbid": media.get("imdb_id"),
                "season": media.get("season"),
                "year": media.get("year"),
            }
        if subscribe_id <= 0:
            subscribe_id = _safe_int(info.get("id"))
        if subscribe_id <= 0:
            return False
        info = self._enrich_subscribe_info(info, media, subscribe_id)
        self._clear_snapshot_restore_suppression(
            str(info.get("tmdbid") or info.get("tmdb_id") or ""),
            _safe_int(info.get("season")),
            subscribe_id,
        )
        self._remember_subscription(subscribe_id, info, reason="subscribe.added")
        self._sync_mp_subscription_to_etk(info, reason="subscribe.added")
        logger.info("  ➜ [订阅助手] 已接管 MP 新增订阅：%s。", self._format_subscribe_info(info))
        return True

    def _handle_subscribe_modified(self, data: Dict[str, Any]) -> bool:
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        subscribe_id = _safe_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id <= 0:
            return False
        old_info = data.get("old_subscribe_info") if isinstance(data.get("old_subscribe_info"), dict) else {}
        fields = data.get("fields") if isinstance(data.get("fields"), list) else []
        scene = str(data.get("scene") or "")
        if self._consume_expected_mp_update(subscribe_id, info, fields):
            self._remember_subscription(subscribe_id, info, reason="subscribe.modified.expected")
            logger.debug("  ➜ [订阅助手] 已确认 ETK 预期内的 MP 订阅修改：%s。", self._format_subscribe_info(info))
            return True

        if str(old_info.get("state") or "").upper() == "N" and str(info.get("state") or "").upper() == "R":
            self._remember_subscription(subscribe_id, info, reason="subscribe.modified.initial_search")
            self._sync_mp_subscription_to_etk(info, reason="subscribe.modified.initial_search")
            logger.debug("  ➜ [订阅助手] 已确认 MP 新增订阅首次搜索完成：%s。", self._format_subscribe_info(info))
            return True

        self._remember_subscription(
            subscribe_id,
            info,
            reason="subscribe.modified",
            extra={
                "last_manual_change": {
                    "scene": scene,
                    "fields": fields,
                    "old_state": old_info.get("state"),
                    "new_state": info.get("state"),
                    "updated_at": time.time(),
                }
            },
        )
        if fields:
            self._mark_active_source(subscribe_id, SOURCE_MANUAL_MP, f"MP 手动修改：{','.join(str(x) for x in fields)}")
        self._sync_mp_subscription_to_etk(info, reason="subscribe.modified")
        logger.debug(
            "  ➜ [订阅助手] 已记录 MP 订阅修改：%s，scene=%s，fields=%s。",
            self._format_subscribe_info(info),
            scene or "-",
            ",".join(str(x) for x in fields) or "-",
        )
        return True

    def _handle_subscribe_deleted(self, data: Dict[str, Any]) -> bool:
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        subscribe_id = _safe_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id <= 0:
            return False
        self._remove_subscription_state(subscribe_id, info, reason="subscribe.deleted")
        self._clear_torrents_for_subscription(subscribe_id, "订阅已删除")
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or "").strip()
        season = _safe_int(info.get("season"))
        if self._snapshot_restore_suppressed(tmdb_id, season, subscribe_id):
            logger.info("  ➜ [订阅助手] MP 订阅删除属于订阅助手主动收口，跳过完成快照恢复：%s。", self._format_subscribe_info(info))
            logger.info("  ➜ [订阅助手] 已记录 MP 订阅删除：%s。", self._format_subscribe_info(info))
            return True
        if self.cfg.verify_enabled and tmdb_id and season > 0:
            snapshot = store.get_latest_snapshot(
                tmdb_id=tmdb_id,
                season_number=season or None,
                subscribe_id=subscribe_id,
            ) or store.get_latest_snapshot(
                tmdb_id=tmdb_id,
                season_number=season or None,
            )
            if snapshot:
                fixed = self._repair_snapshot_subscription(snapshot, tmdb_id, season, _safe_int(snapshot.get("scope_total")))
                if fixed:
                    logger.info("  ➜ [订阅助手] MP 订阅删除已由完成快照实时纠正：%s。", self._format_subscribe_info(info))
        logger.info("  ➜ [订阅助手] 已记录 MP 订阅删除：%s。", self._format_subscribe_info(info))
        return True

    def _handle_subscribe_complete(self, data: Dict[str, Any]) -> bool:
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        media = data.get("mediainfo") if isinstance(data.get("mediainfo"), dict) else {}
        subscribe_id = _safe_int(data.get("subscribe_id") or info.get("id"))
        tmdb_id = str(info.get("tmdbid") or media.get("tmdb_id") or "").strip()
        season = _safe_int(info.get("season") or media.get("season"))
        if subscribe_id <= 0 or not tmdb_id:
            return False
        total = _safe_int(info.get("total_episode") or media.get("number_of_episodes"))
        if self._full_washing_completion_blocked(tmdb_id, season, subscribe_id, info, total):
            return True
        self._remember_subscription(subscribe_id, info, reason="subscribe.complete")
        store.upsert_snapshot(
            tmdb_id=tmdb_id,
            item_type="Series" if str(info.get("type") or media.get("type") or "") == "电视剧" else "Movie",
            season_number=season or None,
            subscribe_id=subscribe_id,
            scope_total=total,
            scope={
                "season": season,
                "total": total,
                "source": "subscribe.complete",
                "completed_at": time.time(),
            },
            subscribe=info,
        )
        self._clear_download_pending(subscribe_id, "", "订阅已完成")
        self._clear_torrents_for_subscription(subscribe_id, "订阅已完成")
        logger.info("  ➜ [订阅助手] 已根据 MP 完成事件写入快照：%s，总集数=%s。", self._format_subscribe_info(info), total or "-")
        return True

    def _full_washing_completion_blocked(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        info: Dict[str, Any],
        total: int,
    ) -> bool:
        if not self.cfg.best_version_full_consistency_check_enabled:
            return False
        if season <= 0 or _safe_int((info or {}).get("best_version_full")) != 1:
            return False
        title = self._series_title(tmdb_id, info)
        if self._has_download_pending(subscribe_id):
            self._set_season_active_washing(tmdb_id, season, True, "全集洗版下载中，等待整理入库后再做一致性收口。")
            self._mark_full_washing_started(subscribe_id, tmdb_id, season, info, reason="full_washing_downloading")
            logger.info(
                "  ➜ [订阅助手] 《%s》S%s 全集洗版已开始下载，暂不做一致性收口，等待入库回流。",
                title,
                season,
            )
            return True
        if total <= 0:
            logger.warning("  ➜ [订阅助手] 《%s》S%s 全集洗版完成事件缺少总集数，按一致性不通过处理。", title, season)
        elif self._season_consistency_ok(tmdb_id, season, total, title):
            self._set_season_active_washing(tmdb_id, season, False, "全集洗版完成且一致性通过，收口。")
            self._clear_full_washing_state(subscribe_id)
            return False

        self._set_season_active_washing(tmdb_id, season, True, "全集洗版完成但一致性不通过，继续保持洗版。")
        if self._keep_full_washing_subscription_active(tmdb_id, season, subscribe_id, info, total):
            logger.warning(
                "  ➜ [订阅助手] 《%s》S%s 全集洗版完成但一致性未通过，已阻止收口并恢复 MP 洗版订阅。",
                title,
                season,
            )
        else:
            logger.warning(
                "  ➜ [订阅助手] 《%s》S%s 全集洗版完成但一致性未通过，恢复 MP 洗版订阅失败。",
                title,
                season,
            )
        self._mark_full_washing_started(subscribe_id, tmdb_id, season, info, reason="consistency_failed_complete")
        return True

    def _trigger_subscription_cleanup_on_complete(self, tmdb_id: str, season: int, info: Dict[str, Any]) -> None:
        cleanup_type = str(self.cfg.subscription_cleanup_history_type or "none").strip().lower()
        scenes = {str(x).strip().lower() for x in (self.cfg.subscription_cleanup_history_scenes or []) if str(x).strip()}
        if cleanup_type in ("", "none") or "completed" not in scenes:
            logger.debug(
                "  ➜ [订阅清理] 《%s》S%s 配置为保留历史或未启用订阅完成场景，跳过。",
                self._series_title(tmdb_id, info),
                season or "-",
            )
            return
        if _safe_int(info.get("best_version_full")) != 1:
            logger.debug(
                "  ➜ [订阅清理] 《%s》S%s 不是分集转全集洗版订阅完成，跳过。",
                self._series_title(tmdb_id, info),
                season or "-",
            )
            return

        seasons = []
        if cleanup_type == "current":
            if season <= 0:
                logger.info("  ➜ [订阅清理] 《%s》订阅完成事件缺少季号，无法清理当前季。", self._series_title(tmdb_id, info))
                return
            seasons = [int(season)]
        elif cleanup_type == "tmdb":
            seasons = self._local_seasons_for_tmdb(tmdb_id)
            if not seasons and season > 0:
                seasons = [int(season)]
        else:
            logger.warning("  ➜ [订阅清理] 未识别的清理范围 %s，跳过。", cleanup_type)
            return

        title = self._series_title(tmdb_id, info)
        logger.info(
            "  ➜ [订阅清理] 《%s》订阅完成，按配置触发分集残留清理：范围=%s，季=%s。",
            title,
            cleanup_type,
            ",".join(f"S{s}" for s in seasons) if seasons else "全部",
        )
        spawn(
            moviepilot.smart_cleanup_mp_episode_residue,
            str(tmdb_id),
            seasons,
            title,
            self.app_config,
            True,
            True,
        )

    def _season_total_locked(self, tmdb_id: str, season: int) -> Optional[Dict[str, Any]]:
        try:
            lock_info = watchlist_db.get_series_seasons_lock_info(str(tmdb_id)).get(int(season)) or {}
            if lock_info.get("locked"):
                return lock_info
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 读取《%s》S%s 集数锁定状态失败，按未锁定处理: %s", self._series_title(tmdb_id), season, e)
        return None

    def _repair_snapshot_subscription(self, snapshot: Dict[str, Any], tmdb_id: str, season: int, snapshot_total: int) -> bool:
        if snapshot_total <= 0:
            return False
        if self._snapshot_restore_suppressed(tmdb_id, season, _safe_int(snapshot.get("subscribe_id"))):
            logger.debug(
                "  ➜ [订阅助手] 《%s》S%s 已按配置完成后删除订阅，跳过完成快照恢复。",
                self._series_title(tmdb_id, snapshot.get("subscribe_json")),
                season,
            )
            return False

        subscriptions = moviepilot.find_subscriptions(tmdb_id, season, self.app_config)
        sub = subscriptions[0] if subscriptions else None
        if not sub:
            snap_sub = snapshot.get("subscribe_json") or {}
            title = snap_sub.get("name") or snap_sub.get("title") or snap_sub.get("keyword") or tmdb_id
            decision = {"mp_state": "R", "sources": {}, "reason": "自动纠错恢复订阅"}
            if _safe_int(snap_sub.get("best_version_full")) == 1 or self._has_full_washing_state(
                tmdb_id,
                season,
                _safe_int(snapshot.get("subscribe_id")),
            ):
                decision["completed_full_washing"] = True
            logger.warning(
                "  ➜ [订阅助手] 完成快照对应的 MP 订阅已消失：《%s》S%s，正在按快照恢复订阅。",
                self._series_title(tmdb_id, snap_sub),
                season,
            )
            created = self._create_subscription(
                str(tmdb_id),
                str(title),
                season,
                decision,
            )
            if not created:
                return False
            sub = created

        current_total = _safe_int(sub.get("total_episode") or sub.get("total") or sub.get("total_episodes"))
        changed = False
        if current_total and current_total < snapshot_total:
            if moviepilot.update_subscription_status(
                int(tmdb_id),
                season,
                str(sub.get("state") or "R"),
                self.app_config,
                total_episodes=snapshot_total,
            ):
                logger.info(
                    "  ➜ [订阅助手] MP 订阅总集数低于快照，已修正：《%s》S%s %s -> %s。",
                    self._series_title(tmdb_id, sub),
                    season,
                    current_total,
                    snapshot_total,
                )
                changed = True

        subscribe_id = _safe_int(sub.get("id") or snapshot.get("subscribe_id"))
        if subscribe_id and self.cfg.auto_search_when_delete:
            if moviepilot.search_subscription(subscribe_id, self.app_config):
                changed = True
        return changed

    def mark_download_started(self, subscribe_id: int, torrent_hash: str, **metadata) -> None:
        if not subscribe_id or not torrent_hash:
            return
        now = time.time()

        def updater(data):
            data[str(torrent_hash).lower()] = {
                "hash": str(torrent_hash).lower(),
                "subscribe_id": subscribe_id,
                "baseline_progress": float(metadata.get("progress") or 0),
                "baseline_at": now,
                "retry_count": 0,
                "time": now,
                **metadata,
            }
            return data

        store.update_state(STATE_TORRENTS, updater)
        self._mark_download_observed(subscribe_id)
        self._mark_active_source(subscribe_id, SOURCE_DOWNLOAD_PENDING, "下载已发起，等待整理入库")

    def _remember_subscription(self, subscribe_id: int, info: Dict[str, Any], reason: str = "", extra: Dict[str, Any] = None) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            if not task.get("created_at"):
                task["created_at"] = time.time()
            task["subscribe_id"] = subscribe_id
            task["subscribe_info"] = info or {}
            task["tmdb_id"] = str((info or {}).get("tmdbid") or (info or {}).get("tmdb_id") or task.get("tmdb_id") or "")
            task["season"] = _safe_int((info or {}).get("season") or task.get("season")) or None
            task["mp_state"] = (info or {}).get("state", task.get("mp_state"))
            task["last_event"] = reason
            task["updated_at"] = time.time()
            if extra:
                task.update(extra)
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _mark_download_observed(self, subscribe_id: int) -> None:
        if not subscribe_id:
            return
        now = time.time()

        def updater(data):
            task = data.get(str(subscribe_id), {})
            if not task.get("created_at"):
                task["created_at"] = now
            task["subscribe_id"] = subscribe_id
            task["download_started_at"] = now
            task.pop("no_download_action_at", None)
            task["updated_at"] = now
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _mark_full_washing_started(
        self,
        subscribe_id: int,
        tmdb_id: str,
        season: int,
        info: Dict[str, Any],
        reason: str = "",
        reset_started_at: bool = False,
    ) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["subscribe_id"] = subscribe_id
            task["subscribe_info"] = info or task.get("subscribe_info") or {}
            task["tmdb_id"] = str(tmdb_id or task.get("tmdb_id") or "")
            task["season"] = _safe_int(season or task.get("season")) or None
            task["full_washing"] = True
            if reset_started_at:
                task["full_washing_started_at"] = time.time()
                task.pop("washing_timeout_started_at", None)
            else:
                task["full_washing_started_at"] = float(task.get("full_washing_started_at") or time.time())
            task["last_event"] = reason or task.get("last_event") or "full_washing"
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _clear_full_washing_state(self, subscribe_id: int) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            if isinstance(task, dict):
                task.pop("full_washing", None)
                task.pop("full_washing_started_at", None)
                task.pop("washing_timeout_started_at", None)
                task["updated_at"] = time.time()
                data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _has_full_washing_state(self, tmdb_id: str, season: int, subscribe_id: int = 0) -> bool:
        data = store.read_state(STATE_SUBSCRIBES)
        if not isinstance(data, dict):
            return False
        if subscribe_id:
            task = data.get(str(subscribe_id)) or {}
            if isinstance(task, dict) and bool(task.get("full_washing")):
                return True
        tmdb_id = str(tmdb_id or "").strip()
        season = _safe_int(season)
        for task in data.values():
            if not isinstance(task, dict) or not task.get("full_washing"):
                continue
            if str(task.get("tmdb_id") or "").strip() == tmdb_id and _safe_int(task.get("season")) == season:
                return True
        return False

    def _keep_full_washing_subscription_active(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        info: Dict[str, Any],
        total: int,
    ) -> bool:
        sub = None
        subscriptions = moviepilot.find_subscriptions(tmdb_id, season, self.app_config)
        if subscriptions:
            sub = subscriptions[0]

        if not sub:
            title = self._series_title(tmdb_id, info)
            created = self._create_subscription(
                str(tmdb_id),
                title,
                season,
                {"mp_state": "R", "sources": {}, "reason": "全集洗版一致性不通过，恢复订阅", "completed_full_washing": True},
            )
            return bool(created)

        payload = dict(sub)
        payload["tmdbid"] = int(tmdb_id)
        payload["season"] = int(season)
        payload["type"] = payload.get("type") or "电视剧"
        payload["state"] = "R"
        payload["best_version"] = 1
        payload["best_version_full"] = 1
        if total > 0:
            payload["total_episode"] = int(total)
        ok = moviepilot.update_subscription(payload, self.app_config)
        if ok:
            sid = _safe_int(payload.get("id") or subscribe_id)
            if sid:
                moviepilot.search_subscription(sid, self.app_config)
        return ok

    def _remove_subscription_state(self, subscribe_id: int, info: Dict[str, Any], reason: str = "") -> None:
        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["subscribe_id"] = subscribe_id
            task["subscribe_info"] = info or task.get("subscribe_info") or {}
            task["deleted"] = True
            task["last_event"] = reason
            task["active_sources"] = {}
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _snapshot_restore_suppression_keys(self, tmdb_id: str, season: int, subscribe_id: int = 0) -> List[str]:
        keys = []
        tmdb = str(tmdb_id or "").strip()
        if tmdb and _safe_int(season) > 0:
            keys.append(f"tmdb:{tmdb}:S{_safe_int(season)}")
        if _safe_int(subscribe_id) > 0:
            keys.append(f"sub:{_safe_int(subscribe_id)}")
        return keys

    def _mark_snapshot_restore_suppressed(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        reason: str,
    ) -> None:
        keys = self._snapshot_restore_suppression_keys(tmdb_id, season, subscribe_id)
        if not keys:
            return
        now = time.time()
        expires_at = now + max(1, _safe_int(self.cfg.snapshot_retention_days, 180)) * 86400

        def updater(data):
            for key in list(data.keys()):
                if float((data.get(key) or {}).get("expires_at") or 0) <= now:
                    data.pop(key, None)
            for key in keys:
                data[key] = {
                    "tmdb_id": str(tmdb_id or ""),
                    "season": _safe_int(season),
                    "subscribe_id": _safe_int(subscribe_id),
                    "reason": reason,
                    "created_at": now,
                    "expires_at": expires_at,
                }
            return data

        store.update_state(STATE_SNAPSHOT_RESTORE_SUPPRESSIONS, updater)

    def _clear_snapshot_restore_suppression(self, tmdb_id: str, season: int, subscribe_id: int = 0) -> None:
        keys = set(self._snapshot_restore_suppression_keys(tmdb_id, season, subscribe_id))
        if not keys:
            return

        def updater(data):
            for key in keys:
                data.pop(key, None)
            return data

        store.update_state(STATE_SNAPSHOT_RESTORE_SUPPRESSIONS, updater)

    def _snapshot_restore_suppressed(self, tmdb_id: str, season: int, subscribe_id: int = 0) -> bool:
        keys = set(self._snapshot_restore_suppression_keys(tmdb_id, season, subscribe_id))
        if not keys:
            return False
        now = time.time()
        state = store.read_state(STATE_SNAPSHOT_RESTORE_SUPPRESSIONS)
        changed = False
        suppressed = False
        if not isinstance(state, dict):
            return False
        for key, item in list(state.items()):
            expires_at = float((item or {}).get("expires_at") or 0)
            if expires_at <= now:
                state.pop(key, None)
                changed = True
                continue
            if key in keys:
                suppressed = True
        if changed:
            store.write_state(STATE_SNAPSHOT_RESTORE_SUPPRESSIONS, state)
        return suppressed

    def _has_recent_manual_mp_change(self, subscribe_id: int, field: str) -> bool:
        if not subscribe_id:
            return False
        data = store.read_state(STATE_SUBSCRIBES)
        task = data.get(str(subscribe_id)) if isinstance(data, dict) else {}
        change = task.get("last_manual_change") if isinstance(task, dict) else {}
        if not isinstance(change, dict):
            return False
        fields = change.get("fields") if isinstance(change.get("fields"), list) else []
        updated_at = float(change.get("updated_at") or 0)
        if field not in fields or updated_at <= 0:
            return False
        return time.time() - updated_at <= MANUAL_CHANGE_GRACE_SECONDS

    def _remember_expected_mp_update(
        self,
        subscribe_id: int,
        *,
        fields: List[str],
        expected_state: str = None,
        expected_total: int = None,
    ) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["expected_mp_update"] = {
                "fields": fields or [],
                "state": expected_state,
                "total_episode": expected_total,
                "updated_at": time.time(),
            }
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _consume_expected_mp_update(self, subscribe_id: int, info: Dict[str, Any], fields: List[str]) -> bool:
        data = store.read_state(STATE_SUBSCRIBES)
        task = data.get(str(subscribe_id)) if isinstance(data, dict) else {}
        expected = task.get("expected_mp_update") if isinstance(task, dict) else {}
        if not isinstance(expected, dict):
            return False
        updated_at = float(expected.get("updated_at") or 0)
        if updated_at <= 0 or time.time() - updated_at > 300:
            return False
        expected_fields = set(str(x) for x in (expected.get("fields") or []))
        changed_fields = set(str(x) for x in (fields or []))
        if changed_fields and expected_fields and not changed_fields.issubset(expected_fields):
            return False
        expected_state = expected.get("state")
        if expected_state and info.get("state") != expected_state:
            return False
        expected_total = _safe_int(expected.get("total_episode"))
        if expected_total and _safe_int(info.get("total_episode")) not in (0, expected_total):
            return False

        def updater(current):
            item = current.get(str(subscribe_id), {})
            item.pop("expected_mp_update", None)
            current[str(subscribe_id)] = item
            return current

        store.update_state(STATE_SUBSCRIBES, updater)
        return True

    def _clear_torrents_for_subscription(self, subscribe_id: int, reason: str) -> int:
        if not subscribe_id:
            return 0
        removed = 0

        def updater(data):
            nonlocal removed
            for torrent_hash, task in list(data.items()):
                if _safe_int((task or {}).get("subscribe_id")) == subscribe_id:
                    data.pop(torrent_hash, None)
                    removed += 1
            return data

        store.update_state(STATE_TORRENTS, updater)
        if removed:
            logger.info("  ➜ [订阅助手] 已清理订阅 %s 的下载监控：%s，数量=%s。", subscribe_id, reason, removed)
        return removed

    def _delete_monitored_download_task(self, torrent_hash: str, task: Dict[str, Any], reason: str) -> bool:
        fingerprint = self._delete_fingerprint(task)
        if self.cfg.skip_deletion and store.has_deleted_resource(fingerprint):
            self._clear_download_pending(task.get("subscribe_id"), torrent_hash, "近期已删除过同类任务，跳过重复删种")
            logger.info(
                "  ➜ [订阅助手] 下载任务 %s 近期已处理过同类删除，跳过重复删种。",
                str(torrent_hash)[:8],
            )
            return True
        if not moviepilot.delete_download_tasks("", self.app_config, hashes=[torrent_hash]):
            return False
        store.record_deleted_resource(
            fingerprint,
            tmdb_id=str(task.get("tmdb_id") or ""),
            season_number=task.get("season"),
            episodes=task.get("episodes") or [],
            reason=reason,
            retention_hours=self.cfg.delete_record_retention_hours,
        )
        if self.cfg.auto_search_when_delete and task.get("subscribe_id"):
            if moviepilot.search_subscription(_safe_int(task.get("subscribe_id")), self.app_config):
                repeated = any(
                    str(info.get("hash") or info.get("hashString") or info.get("id") or "").lower()
                    == str(torrent_hash).lower()
                    for info in moviepilot.get_downloading_tasks(self.app_config) or []
                )
                if repeated:
                    if not moviepilot.delete_download_tasks("", self.app_config, hashes=[torrent_hash]):
                        return False
                    logger.warning(
                        "  ➜ [订阅助手] 自动重搜再次命中相同下载任务 %s，已二次删除并停止本轮重搜。",
                        str(torrent_hash)[:8],
                    )
        self._clear_download_pending(task.get("subscribe_id"), torrent_hash, reason)
        return True

    def _download_task_has_excluded_tag(self, info: Dict[str, Any]) -> bool:
        tags = {x.lower() for x in self._download_task_tokens(info, ("tag", "tags", "label", "labels", "category", "categories"))}
        for tag in self.cfg.delete_exclude_tags or []:
            text = str(tag or "").strip().lower()
            if text and any(text == token or text in token for token in tags):
                return True
        return False

    def _download_task_tracker_keyword(self, info: Dict[str, Any]) -> str:
        blob = json.dumps(info or {}, ensure_ascii=False, default=str).lower()
        for keyword in self.cfg.tracker_keywords or []:
            text = str(keyword or "").strip().lower()
            if text and text in blob:
                return str(keyword)
        return ""

    def _download_task_tokens(self, data: Any, keys: tuple) -> List[str]:
        found = []
        if isinstance(data, dict):
            for key, value in data.items():
                if str(key).lower() in keys:
                    found.extend(self._download_task_tokens(value, keys))
                elif isinstance(value, (dict, list, tuple)):
                    found.extend(self._download_task_tokens(value, keys))
        elif isinstance(data, (list, tuple)):
            for item in data:
                found.extend(self._download_task_tokens(item, keys))
        elif data not in (None, ""):
            found.append(str(data).strip())
        return [x for x in found if x]

    def _sync_mp_subscription_to_etk(self, info: Dict[str, Any], reason: str = "") -> bool:
        if not isinstance(info, dict):
            return False
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or "").strip()
        if not tmdb_id:
            return False
        subscribe_id = _safe_int(info.get("id") or info.get("subscribe_id"))
        item_type = self._mp_subscription_item_type(info)
        if not item_type:
            return False

        source = {
            "type": "moviepilot",
            "subscribe_id": subscribe_id or None,
            "reason": reason or "sync",
        }
        history_sync = reason == "history"

        tmdb_items = []
        if item_type == "Movie":
            tmdb_items.append({"tmdb_id": tmdb_id, "media_type": "Movie"})
        elif item_type == "Season":
            season = _safe_int(info.get("season"))
            if season <= 0:
                return False
            tmdb_items.append({"tmdb_id": tmdb_id, "media_type": "Series", "season": season})
        else:
            logger.debug("  ➜ [订阅助手] 跳过无季号 MP 剧集订阅同步：%s。", self._format_subscribe_info(info))
            return False

        try:
            before_keys = self._subscription_sync_keys(tmdb_items)
            if self._mp_subscription_sync_current(tmdb_items, source):
                return False
            skip_statuses = {"PAUSED", "IGNORED", "PENDING_RELEASE"} if history_sync else set()
            process_subscription_items_and_update_db(
                tmdb_items=tmdb_items,
                tmdb_to_emby_item_map=media_db.get_tmdb_to_emby_map(),
                subscription_source=source,
                tmdb_api_key=self.app_config.get("tmdb_api_key") or "",
                target_status="SUBSCRIBED",
                skip_existing_statuses=skip_statuses,
                skip_in_library=False,
            )
            after_keys = self._subscription_sync_keys(tmdb_items)
            changed = after_keys != before_keys
        except Exception as e:
            logger.warning(
                "  ➜ [订阅助手] 同步 MP 订阅到 ETK 失败：%s -> %s",
                self._format_subscribe_info(info),
                e,
                exc_info=True,
            )
            return False

        if changed and not history_sync:
            logger.info(
                "  ➜ [订阅助手] 已同步 MP 订阅到 ETK：%s，来源=%s。",
                self._format_subscribe_info(info),
                reason or "sync",
            )
        return changed

    def _mp_subscription_item_type(self, info: Dict[str, Any]) -> str:
        type_text = str(info.get("type") or info.get("media_type") or "").strip().lower()
        season = _safe_int(info.get("season"))
        if "电影" in type_text or type_text in ("movie", "movies"):
            return "Movie"
        if "电视" in type_text or "剧" in type_text or type_text in ("tv", "series", "电视剧"):
            return "Season" if season > 0 else "Series"
        return "Season" if season > 0 else ""

    def _subscription_sync_keys(self, tmdb_items: List[Dict[str, Any]]) -> Dict[str, str]:
        try:
            expected = []
            for item in tmdb_items or []:
                tmdb_id = str((item or {}).get("tmdb_id") or "").strip()
                media_type = str((item or {}).get("media_type") or "").strip()
                season = _safe_int((item or {}).get("season"))
                if media_type == "Movie" and tmdb_id:
                    expected.append((tmdb_id, "Movie"))
                elif media_type == "Series" and tmdb_id:
                    expected.append((tmdb_id, "Series"))
                    if season > 0:
                        expected.append((tmdb_id, f"SeasonByParent:{season}"))
            if not expected:
                return {}
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    rows = []
                    for tmdb_id, item_type in expected:
                        if item_type.startswith("SeasonByParent:"):
                            season = _safe_int(item_type.split(":", 1)[1])
                            cursor.execute(
                                """
                                SELECT tmdb_id, item_type, subscription_status, subscription_sources_json::text AS sources
                                FROM media_metadata
                                WHERE parent_series_tmdb_id = %s
                                  AND item_type = 'Season'
                                  AND season_number = %s
                                """,
                                (tmdb_id, season),
                            )
                        else:
                            cursor.execute(
                                """
                                SELECT tmdb_id, item_type, subscription_status, subscription_sources_json::text AS sources
                                FROM media_metadata
                                WHERE tmdb_id = %s
                                  AND item_type = %s
                                """,
                                (tmdb_id, item_type),
                            )
                        rows.extend(cursor.fetchall() or [])
            return {
                f"{row.get('tmdb_id')}|{row.get('item_type')}": f"{row.get('subscription_status')}|{row.get('sources') or ''}"
                for row in rows
            }
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 读取同步前后状态失败：%s", e)
            return {}

    def _mp_subscription_sync_current(self, tmdb_items: List[Dict[str, Any]], source: Dict[str, Any]) -> bool:
        rows = self._subscription_sync_rows(tmdb_items, target_only=True)
        if not rows:
            return False
        subscribe_id = _safe_int((source or {}).get("subscribe_id"))
        for row in rows:
            if str(row.get("subscription_status") or "").upper() != "SUBSCRIBED":
                return False
            sources = row.get("sources") or []
            if isinstance(sources, str):
                try:
                    sources = json.loads(sources)
                except Exception:
                    sources = []
            if not isinstance(sources, list):
                return False
            matched = False
            for item in sources:
                if not isinstance(item, dict):
                    continue
                if str(item.get("type") or "") != "moviepilot":
                    continue
                if subscribe_id > 0 and _safe_int(item.get("subscribe_id")) != subscribe_id:
                    continue
                matched = True
                break
            if not matched:
                return False
        return True

    def _subscription_sync_rows(self, tmdb_items: List[Dict[str, Any]], target_only: bool = False) -> List[Dict[str, Any]]:
        expected = []
        for item in tmdb_items or []:
            tmdb_id = str((item or {}).get("tmdb_id") or "").strip()
            media_type = str((item or {}).get("media_type") or "").strip()
            season = _safe_int((item or {}).get("season"))
            if media_type == "Movie" and tmdb_id:
                expected.append((tmdb_id, "Movie"))
            elif media_type == "Series" and tmdb_id:
                if not target_only:
                    expected.append((tmdb_id, "Series"))
                if season > 0:
                    expected.append((tmdb_id, f"SeasonByParent:{season}"))
        if not expected:
            return []
        rows = []
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for tmdb_id, item_type in expected:
                        if item_type.startswith("SeasonByParent:"):
                            season = _safe_int(item_type.split(":", 1)[1])
                            cursor.execute(
                                """
                                SELECT tmdb_id, item_type, subscription_status, subscription_sources_json AS sources
                                FROM media_metadata
                                WHERE parent_series_tmdb_id = %s
                                  AND item_type = 'Season'
                                  AND season_number = %s
                                """,
                                (tmdb_id, season),
                            )
                        else:
                            cursor.execute(
                                """
                                SELECT tmdb_id, item_type, subscription_status, subscription_sources_json AS sources
                                FROM media_metadata
                                WHERE tmdb_id = %s
                                  AND item_type = %s
                                """,
                                (tmdb_id, item_type),
                            )
                        rows.extend(cursor.fetchall() or [])
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 读取 MP 订阅同步状态失败：%s", e)
            return []
        return rows

    def _enrich_subscribe_info(self, info: Dict[str, Any], media: Dict[str, Any], subscribe_id: int) -> Dict[str, Any]:
        enriched = dict(info or {})
        tmdb_id = str(enriched.get("tmdbid") or media.get("tmdb_id") or "").strip()
        if not tmdb_id:
            return enriched
        if _safe_int(enriched.get("season")) > 0 and enriched.get("state"):
            return enriched
        try:
            for sub in moviepilot.find_subscriptions(tmdb_id, config=self.app_config) or []:
                if _safe_int(sub.get("id")) != subscribe_id:
                    continue
                for key, value in sub.items():
                    if enriched.get(key) in (None, "", 0):
                        enriched[key] = value
                break
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 反查 MP 订阅详情失败：%s -> %s", subscribe_id, e)
        return enriched

    def _completion_signal(self, *, tmdb_id, season, series_details, episodes, season_info) -> CompletionSignal:
        return evaluate_completion(
            tmdb_id=tmdb_id,
            season=season,
            series_details=series_details,
            episodes=episodes,
            season_cooldown_days=self.cfg.season_cooldown_days,
            volatility_stable=True,
        )

    def _season_local_completed(self, tmdb_id: str, season: int, season_info: Dict[str, Any], signal: CompletionSignal) -> bool:
        expected_count = _safe_int((season_info or {}).get("episode_count")) or _safe_int(getattr(signal, "scope_total", 0))
        if expected_count <= 0:
            return False
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT COUNT(DISTINCT episode_number) AS count
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND item_type = 'Episode'
                          AND season_number = %s
                          AND in_library = TRUE
                          AND episode_number IS NOT NULL
                        """,
                        (str(tmdb_id), int(season)),
                    )
                    row = cursor.fetchone() or {}
            return _safe_int(row.get("count")) >= expected_count
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 判断本地季完结失败：tmdb=%s S%s，err=%s", tmdb_id, season, e)
            return False

    def _decide_subscription_state(
        self,
        *,
        final_status: str,
        series_details: Dict[str, Any],
        season: int,
        season_episodes: List[Dict[str, Any]],
        signal: CompletionSignal,
        real_next_episode: Dict[str, Any],
    ) -> Dict[str, Any]:
        decision = {
            "mp_state": "R",
            "sources": {},
            "reason": "",
            "snapshot": False,
            "best_version": None,
            "best_version_full": None,
        }
        if final_status == "Completed":
            guard_mode = str(self.cfg.guard_mode or "balanced").lower()
            low_confidence_needs_observe = (
                guard_mode == "strict"
                or (guard_mode == "balanced" and signal.scope_total <= 3)
            )
            if guard_mode != "off" and signal.completed and signal.confidence == "low" and low_confidence_needs_observe:
                decision["mp_state"] = "P"
                decision["sources"][SOURCE_GUARD_VETO] = "低置信完结，进入完成前观察"
                decision["reason"] = decision["sources"][SOURCE_GUARD_VETO]
            else:
                decision["snapshot"] = True
                decision["reason"] = "订阅目标已完成，保存完成快照"
            return decision

        if self.cfg.pause_enabled:
            paused, reason = check_pre_air_pause(
                series_details=series_details,
                season=season,
                episodes=season_episodes,
                tv_air_days=self.cfg.tv_air_pause_days,
            )
            if paused:
                decision["mp_state"] = "S"
                decision["sources"][SOURCE_PRE_AIR] = reason
                decision["reason"] = reason
                return decision

            paused, reason = check_airing_gap_pause(
                next_episode=real_next_episode,
                pause_days=self.cfg.airing_pause_days,
                signal=signal,
            )
            if paused:
                decision["mp_state"] = "S"
                decision["sources"][SOURCE_AIRING_GAP] = reason
                decision["reason"] = reason
                return decision

        if final_status == "Pending" or self.cfg.auto_pending_enabled:
            pending, reason = should_enter_pending(
                series_details=series_details,
                season=season,
                episodes=season_episodes,
                pending_days=self.cfg.auto_pending_days,
                pending_episodes=self.cfg.auto_pending_episodes,
                use_volatility=self.cfg.pending_use_volatility,
                signal=signal,
            )
            if final_status == "Pending" or pending:
                decision["mp_state"] = "P"
                decision["sources"][SOURCE_PENDING_JUDGE] = reason or "ETK 追剧状态为待定"
                decision["reason"] = decision["sources"][SOURCE_PENDING_JUDGE]
                return decision

        if final_status == "Paused":
            decision["mp_state"] = "S"
            decision["sources"][SOURCE_AIRING_GAP] = "ETK 追剧状态为暂停"
            decision["reason"] = "ETK 追剧状态为暂停"
            return decision

        decision["mp_state"] = "R"
        decision["reason"] = "订阅可运行"
        return decision

    def _target_seasons_for_sync(self, *, valid_seasons, existing_by_season, latest_season_num, final_status):
        seasons = []
        for season in valid_seasons:
            s_num = _safe_int(season.get("season_number"))
            if s_num in existing_by_season or s_num == latest_season_num:
                seasons.append(season)
        return seasons

    def _triggering_episodes_are_virtual(self, episode_ids: List[str] = None) -> bool:
        ids = [str(x or '').strip() for x in (episode_ids or []) if str(x or '').strip()]
        if not ids:
            return False
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT asset_details_json
                        FROM media_metadata
                        WHERE emby_item_ids_json ?| %s
                        """,
                        (ids,),
                    )
                    rows = cursor.fetchall() or []
            if not rows:
                return False
            virtual_count = 0
            for row in rows:
                assets = row.get("asset_details_json") or []
                if isinstance(assets, str):
                    assets = json.loads(assets or "[]")
                if isinstance(assets, dict):
                    assets = [assets]
                is_virtual = False
                for asset in assets or []:
                    if not isinstance(asset, dict):
                        continue
                    path = asset.get("path") or asset.get("Path")
                    if p115_fp_is_virtual_strm_target(path) or p115_fp_is_virtual_strm_target(p115_fp_read_strm_target(path)):
                        is_virtual = True
                        break
                if is_virtual:
                    virtual_count += 1
            return virtual_count > 0 and virtual_count == len(rows)
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 判断触发分集是否虚拟入库失败: %s", e)
            return False

    def _season_has_virtual_import(self, tmdb_id: str, season: int) -> bool:
        tmdb_id = str(tmdb_id or '').strip()
        season = _safe_int(season)
        if not tmdb_id or season <= 0:
            return False
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT 1
                        FROM shared_virtual_imports
                        WHERE status IN ('virtual', 'promoting')
                          AND LOWER(item_type) IN ('series', 'season', 'episode', 'tv')
                          AND (tmdb_id = %s OR parent_series_tmdb_id = %s)
                          AND (season_number = %s OR season_number IS NULL)
                        LIMIT 1
                        """,
                        (tmdb_id, tmdb_id, season),
                    )
                    if cursor.fetchone():
                        return True

                    cursor.execute(
                        """
                        SELECT asset_details_json
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND season_number = %s
                          AND item_type = 'Episode'
                          AND in_library = TRUE
                        """,
                        (tmdb_id, season),
                    )
                    rows = cursor.fetchall() or []
            for row in rows:
                assets = row.get("asset_details_json") or []
                if isinstance(assets, str):
                    try:
                        assets = json.loads(assets or "[]")
                    except Exception:
                        assets = []
                if isinstance(assets, dict):
                    assets = [assets]
                for asset in assets or []:
                    if not isinstance(asset, dict):
                        continue
                    path = asset.get("path") or asset.get("Path")
                    if p115_fp_is_virtual_strm_target(path) or p115_fp_is_virtual_strm_target(p115_fp_read_strm_target(path)):
                        return True
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 判断季是否仍为虚拟入库失败：tmdb=%s, season=%s, err=%s", tmdb_id, season, e)
        return False

    def _should_create_full_washing_for_partial_completed_season(
        self,
        tmdb_id: str,
        season: int,
        season_info: Dict[str, Any],
        signal: CompletionSignal,
    ) -> bool:
        if not signal or not signal.completed:
            return False
        expected_count = _safe_int((season_info or {}).get("episode_count")) or _safe_int(signal.scope_total)
        if expected_count <= 0:
            return False
        local_count = self._local_in_library_episode_count(tmdb_id, season)
        return 0 < local_count < expected_count

    def _local_in_library_episode_count(self, tmdb_id: str, season: int) -> int:
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT COUNT(DISTINCT episode_number) AS count
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND season_number = %s
                          AND item_type = 'Episode'
                          AND in_library = TRUE
                          AND episode_number IS NOT NULL
                        """,
                        (str(tmdb_id), int(season)),
                    )
                    row = cursor.fetchone() or {}
            return _safe_int(row.get("count"))
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 查询本地在库集数失败：tmdb=%s, season=%s, err=%s", tmdb_id, season, e)
            return 0

    def _local_in_library_episode_max(self, tmdb_id: str, season: int) -> int:
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT MAX(episode_number) AS episode_number
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND season_number = %s
                          AND item_type = 'Episode'
                          AND in_library = TRUE
                          AND episode_number IS NOT NULL
                        """,
                        (str(tmdb_id), int(season)),
                    )
                    row = cursor.fetchone() or {}
            return _safe_int(row.get("episode_number"))
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 查询本地已入库最大集号失败：tmdb=%s, season=%s, err=%s", tmdb_id, season, e)
            return 0

    def _create_subscription(
        self,
        tmdb_id: str,
        series_name: str,
        season: int,
        decision: Dict[str, Any],
        consume_quota: bool = False,
    ) -> Optional[dict]:
        payload_kwargs = self._subscription_wash_kwargs(decision)
        if _safe_int(payload_kwargs.get("best_version_full")) != 1:
            local_episode_max = self._local_in_library_episode_max(tmdb_id, season)
            if local_episode_max > 0:
                payload_kwargs["start_episode"] = local_episode_max + 1
        if not moviepilot.subscribe_series_to_moviepilot(
            {"title": series_name, "tmdb_id": tmdb_id},
            season,
            self.app_config,
            consume_quota=consume_quota,
            **payload_kwargs,
        ):
            logger.warning("  ➜ [订阅助手] 《%s》S%s 自动补订失败。", series_name, season)
            return None
        subscriptions = moviepilot.find_subscriptions(tmdb_id, season, self.app_config)
        sub = subscriptions[0] if subscriptions else {"tmdbid": tmdb_id, "season": season}
        if _safe_int(payload_kwargs.get("best_version_full")) == 1:
            self._mark_full_washing_started(
                _safe_int(sub.get("id")),
                tmdb_id,
                season,
                sub,
                reason="create_full_washing_subscription",
            )
            self._set_season_active_washing(tmdb_id, season, True, "全集洗版订阅已创建。")
        return sub

    def _local_progress_seasons(self, tmdb_id: str) -> set:
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT season_number
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND item_type = 'Episode'
                          AND in_library = TRUE
                          AND season_number > 0
                          AND episode_number IS NOT NULL
                        GROUP BY season_number
                        """,
                        (str(tmdb_id),),
                    )
                    rows = cursor.fetchall() or []
            return {
                _safe_int(row.get("season_number"))
                for row in rows
                if _safe_int(row.get("season_number")) > 0
            }
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 查询本地追剧进度失败：tmdb=%s, err=%s", tmdb_id, e)
            return set()

    def _subscription_wash_kwargs(self, decision: Dict[str, Any]) -> Dict[str, Optional[int]]:
        if decision.get("completed_full_washing"):
            return {"best_version": 1, "best_version_full": 1}
        if self.cfg.best_version_type in ("tv", "all"):
            return {"best_version": 1, "best_version_full": 1}
        if self.cfg.best_version_type == "tv_episode":
            return {"best_version": 1, "best_version_full": None}
        return {"best_version": None, "best_version_full": None}

    def _sync_completed_full_washing(
        self,
        *,
        tmdb_id: str,
        series_name: str,
        season: int,
        subscribe: Dict[str, Any],
        season_info: Dict[str, Any],
        signal: CompletionSignal,
    ) -> None:
        wash_mode = str(self.cfg.best_version_type or "no").strip().lower()
        if wash_mode not in ("tv_episode", "completed_full"):
            return

        expected_count = _safe_int(season_info.get("episode_count")) or _safe_int(signal.scope_total)
        if expected_count <= 0:
            logger.info("  ➜ [订阅助手] 《%s》S%s 总集数未知，跳过全集洗版门禁。", series_name, season)
            return

        full_washing_priority = None
        if wash_mode == "tv_episode" and self.cfg.best_version_full_consistency_check_enabled:
            if self._season_consistency_ok(tmdb_id, season, expected_count, series_name):
                if self.cfg.best_version_episode_to_full:
                    priority_result = self._backfill_mp_priority_for_full_washing(tmdb_id, season, series_name)
                    full_washing_priority = priority_result.get("mp_priority")
                    self._set_season_active_washing(tmdb_id, season, True, "一致性通过，转全集洗版并等待合集包。")
                    logger.info(
                        "  ➜ [订阅助手] 《%s》第 %s 季 一致性已通过，按配置转全集洗版。",
                        series_name,
                        season,
                    )
                else:
                    self._set_season_active_washing(tmdb_id, season, False, "一致性通过，不提交全集洗版。")
                    self._delete_subscription_after_episode_washing(tmdb_id, season, subscribe, series_name)
                    return
            else:
                self._set_season_active_washing(tmdb_id, season, True, "一致性不通过，提交全集洗版并等待收口。")
        elif wash_mode == "completed_full":
            self._set_season_active_washing(tmdb_id, season, True, "完结洗版模式，提交全集洗版并等待收口。")

        if _safe_int((subscribe or {}).get("best_version_full")) == 1:
            self._mark_full_washing_started(
                _safe_int((subscribe or {}).get("id")),
                tmdb_id,
                season,
                subscribe,
                reason="full_washing_already_active",
            )
            logger.debug("  ➜ [订阅助手] 《%s》S%s 已是全集洗版订阅，跳过重复更新。", series_name, season)
            return

        payload = dict(subscribe or {})
        if not payload.get("id"):
            logger.debug("  ➜ [订阅助手] 《%s》S%s 未找到可更新的 MP 订阅，跳过全集洗版。", series_name, season)
            return
        payload["tmdbid"] = int(tmdb_id)
        payload["season"] = int(season)
        payload["name"] = payload.get("name") or series_name
        payload["type"] = payload.get("type") or "电视剧"
        payload["best_version"] = 1
        payload["best_version_full"] = 1
        payload["include"] = ""
        if full_washing_priority is not None:
            payload["current_priority"] = int(full_washing_priority)

        if moviepilot.update_subscription(payload, self.app_config):
            if full_washing_priority is not None:
                logger.info(
                    "  ➜ [订阅助手] 《%s》S%s 全集洗版开启后已回填 MP 当前优先级=%s。",
                    series_name,
                    season,
                    full_washing_priority,
                )
            self._mark_full_washing_started(
                _safe_int(payload.get("id")),
                tmdb_id,
                season,
                payload,
                reason="submit_full_washing",
                reset_started_at=True,
            )
            if wash_mode == "completed_full":
                logger.info("  ➜ [订阅助手] 《%s》S%s 已提交完结全集洗版订阅。", series_name, season)
            else:
                self._trigger_subscription_cleanup_on_complete(tmdb_id, season, payload)
                logger.info("  ➜ [订阅助手] 《%s》S%s 已提交分集转全集洗版订阅。", series_name, season)
        else:
            logger.warning("  ➜ [订阅助手] 《%s》S%s 全集洗版订阅更新失败。", series_name, season)

    def _backfill_mp_priority_for_full_washing(self, tmdb_id: str, season: int, series_name: str) -> Dict[str, Any]:
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT episode_number, washing_level
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND item_type = 'Episode'
                          AND season_number = %s
                          AND in_library = TRUE
                          AND episode_number IS NOT NULL
                          AND washing_level IS NOT NULL
                        ORDER BY episode_number ASC
                        """,
                        (str(tmdb_id), int(season)),
                    )
                    rows = cursor.fetchall() or []
            levels = []
            for row in rows:
                episode = _safe_int(row.get("episode_number"))
                level = _safe_int(row.get("washing_level"))
                if episode <= 0 or level <= 0:
                    continue
                levels.append(level)
            level = min(levels or [])
            if level <= 0:
                logger.info("  ➜ [订阅助手] 《%s》S%s 未找到 ETK 优先级，跳过 MP 优先级回填。", series_name, season)
                return {}
            mp_priority = min(max(100 - level, 0), 99)
            return {"mp_priority": mp_priority}
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 《%s》S%s 回填 MP 优先级失败：%s", series_name, season, e)
        return {}

    def _delete_subscription_after_episode_washing(
        self,
        tmdb_id: str,
        season: int,
        subscribe: Dict[str, Any],
        series_name: str,
    ) -> None:
        subscribe_id = _safe_int((subscribe or {}).get("id"))
        if not subscribe_id:
            subscriptions = moviepilot.find_subscriptions(tmdb_id, season, self.app_config)
            subscribe_id = _safe_int((subscriptions[0] or {}).get("id") if subscriptions else 0)
        self._mark_snapshot_restore_suppressed(
            tmdb_id,
            season,
            subscribe_id,
            "分集洗版一致性通过，按配置删除 MP 订阅",
        )
        if subscribe_id and moviepilot.delete_subscription_by_id(subscribe_id, self.app_config):
            self._remove_subscription_state(subscribe_id, subscribe or {}, reason="episode_washing_consistency_ok")
            self._clear_torrents_for_subscription(subscribe_id, "分集洗版一致性通过，删除 MP 订阅")
            logger.info("  ➜ [订阅助手] 《%s》S%s 分集洗版一致性通过，已按配置删除 MP 订阅。", series_name, season)
        else:
            self._clear_snapshot_restore_suppression(tmdb_id, season, subscribe_id)
            logger.warning("  ➜ [订阅助手] 《%s》S%s 分集洗版一致性通过，但删除 MP 订阅失败。", series_name, season)

    def _season_consistency_ok(self, tmdb_id: str, season: int, expected_count: int, series_name: str) -> bool:
        try:
            result = helpers.check_season_consistency(
                tmdb_id=str(tmdb_id),
                season_number=int(season),
                expected_episode_count=int(expected_count),
                series_name=series_name,
            )
            return bool(result.get("ok"))
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 《%s》S%s 一致性校验失败，按不通过处理：%s", series_name, season, e)
            return False

    def _set_season_active_washing(self, tmdb_id: str, season: int, enabled: bool, reason: str = "") -> None:
        title = self._series_title(tmdb_id)
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE media_metadata
                        SET active_washing = %s
                        WHERE parent_series_tmdb_id = %s
                          AND season_number = %s
                          AND item_type = 'Season'
                        """,
                        (bool(enabled), str(tmdb_id), int(season)),
                    )
                    conn.commit()
            action = "开启" if enabled else "清理"
            logger.info("  ➜ [订阅助手] 已%s《%s》S%s active_washing：%s", action, title, season, reason or "-")
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 设置 active_washing 失败：《%s》S%s -> %s", title, season, e)

    def _local_seasons_for_tmdb(self, tmdb_id: str) -> List[int]:
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT DISTINCT season_number
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND season_number IS NOT NULL
                          AND season_number > 0
                          AND item_type IN ('Season', 'Episode')
                        ORDER BY season_number ASC
                        """,
                        (str(tmdb_id),),
                    )
                    rows = cursor.fetchall() or []
            return [int(row.get("season_number")) for row in rows if _safe_int(row.get("season_number")) > 0]
        except Exception as e:
            logger.warning("  ➜ [订阅清理] 查询《%s》本地季号失败：%s", self._series_title(tmdb_id), e)
            return []

    def _target_total(self, decision: Dict[str, Any], season_info: Dict[str, Any], signal: CompletionSignal) -> Optional[int]:
        if decision["mp_state"] == "P":
            return self.cfg.pending_fake_total_episodes
        total = _safe_int(season_info.get("episode_count"))
        if total > 0:
            return total
        if signal.scope_total > 0:
            return signal.scope_total
        return None

    def _update_source_state(self, subscribe_id: int, decision: Dict[str, Any]) -> None:
        if not subscribe_id:
            return
        active_sources = decision.get("sources") or {}

        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["active_sources"] = active_sources
            task["last_reason"] = decision.get("reason")
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _mark_washing_timeout_started(
        self,
        subscribe_id: int,
        tmdb_id: str,
        season: int,
        info: Dict[str, Any],
        reason: str = "",
    ) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["subscribe_id"] = subscribe_id
            task["subscribe_info"] = info or task.get("subscribe_info") or {}
            task["tmdb_id"] = str(tmdb_id or task.get("tmdb_id") or "")
            task["season"] = _safe_int(season or task.get("season")) or None
            task["washing_timeout_started_at"] = float(task.get("washing_timeout_started_at") or time.time())
            task["last_event"] = reason or task.get("last_event") or "washing_timeout"
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _mark_active_source(self, subscribe_id: int, source: str, reason: str) -> None:
        def updater(data):
            task = data.get(str(subscribe_id), {})
            sources = task.get("active_sources") or {}
            sources[source] = reason
            task["active_sources"] = sources
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _clear_download_pending(self, subscribe_id: int, torrent_hash: str, reason: str) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            sources = task.get("active_sources") or {}
            sources.pop(SOURCE_DOWNLOAD_PENDING, None)
            task["active_sources"] = sources
            task["last_reason"] = reason
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _has_download_pending(self, subscribe_id: int) -> bool:
        if not subscribe_id:
            return False
        data = store.read_state(STATE_SUBSCRIBES)
        task = data.get(str(subscribe_id), {}) if isinstance(data, dict) else {}
        sources = task.get("active_sources") or {} if isinstance(task, dict) else {}
        return SOURCE_DOWNLOAD_PENDING in sources

    def _parse_subscribe_source(self, source: Any) -> Dict[str, Any]:
        text = str(source or "")
        if "|" not in text:
            return {}
        prefix, raw = text.split("|", 1)
        if prefix != "Subscribe":
            return {}
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    def _format_subscribe_info(self, info: Dict[str, Any]) -> str:
        if not isinstance(info, dict):
            return "-"
        tmdb_id = info.get("tmdbid") or info.get("tmdb_id") or "-"
        title = self._series_title(str(tmdb_id), info)
        season = info.get("season")
        state = info.get("state")
        season_text = f"S{season}" if season not in (None, "", 0) else "全局"
        state_text = f"，状态={state}" if state else ""
        return f"{title}({tmdb_id}) {season_text}{state_text}"

    def _series_title(self, tmdb_id: Any, info: Dict[str, Any] = None) -> str:
        info = info if isinstance(info, dict) else {}
        tmdb_id = str(tmdb_id or info.get("tmdbid") or info.get("tmdb_id") or "").strip()
        for key in ("name", "title", "keyword"):
            title = str(info.get(key) or "").strip()
            if title and title.lower() not in ("none", "null") and title != tmdb_id:
                return title

        if not tmdb_id:
            return "-"
        if tmdb_id in self._title_cache:
            return self._title_cache[tmdb_id] or tmdb_id

        title = ""
        try:
            title = watchlist_db.get_watchlist_item_name(tmdb_id) or ""
        except Exception:
            title = ""
        if not title:
            try:
                with connection.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT title
                            FROM media_metadata
                            WHERE tmdb_id = %s AND item_type IN ('Series', 'Movie')
                            ORDER BY CASE WHEN item_type = 'Series' THEN 0 ELSE 1 END
                            LIMIT 1
                            """,
                            (tmdb_id,),
                        )
                        row = cursor.fetchone() or {}
                        title = str(row.get("title") or "").strip()
            except Exception:
                title = ""

        self._title_cache[tmdb_id] = title or tmdb_id
        return self._title_cache[tmdb_id]

    def _delete_fingerprint(self, task: Dict[str, Any]) -> str:
        raw = "|".join([
            str(task.get("tmdb_id") or ""),
            str(task.get("season") or ""),
            ",".join(str(x) for x in (task.get("episodes") or [])),
            str(task.get("title") or ""),
            str(task.get("enclosure") or ""),
            str(task.get("page_url") or ""),
        ])
        return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()

    def _transfer_history_fingerprint(self, rec: Dict[str, Any], pick_code: str, file_id: str) -> str:
        raw = "|".join([
            str(rec.get("id") or ""),
            str(pick_code or ""),
            str(file_id or ""),
            _first_text(rec, "title", "name", "src", "path"),
            _first_text(rec, "date", "time", "created_at", "updated_at"),
        ])
        return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _progress_value(info: Dict[str, Any]) -> float:
    for key in ("progress", "percent", "completed"):
        value = info.get(key)
        try:
            number = float(value)
            return number * 100 if 0 <= number <= 1 else number
        except (TypeError, ValueError):
            pass
    return 0.0


def _first_text(data: Dict[str, Any], *keys: str) -> str:
    if not isinstance(data, dict):
        return ""
    for key in keys:
        value = data.get(key)
        if value not in (None, "", [], {}):
            return str(value).strip()
    return ""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
