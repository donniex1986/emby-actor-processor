# routes/webhook.py

import collections
import threading
import time
import os
import re
import json
import requests
from datetime import datetime, timezone
from flask import Blueprint, Response, jsonify, request, stream_with_context
from typing import Optional, List
from gevent import spawn_later, spawn
from gevent.event import Event

import task_manager
import handler.emby as emby
import handler.tmdb as tmdb
from handler.p115_copy_play import cleanup_for_playback_stop
from handler import p115_play_pool
import config_manager
import constants
import handler.telegram as telegram
import extensions
from extensions import SYSTEM_UPDATE_MARKERS, SYSTEM_UPDATE_LOCK, RECURSION_SUPPRESSION_WINDOW, DELETING_COLLECTIONS, UPDATING_IMAGES, UPDATING_METADATA
from core_processor import MediaProcessor
from tasks.watchlist import task_process_watchlist
from tasks.users import task_auto_sync_template_on_policy_change
from tasks.media import task_sync_all_metadata
from handler.custom_collection import RecommendationEngine
from handler import tmdb_collections as collections_handler
from services.cover_generator import CoverGeneratorService
from database import custom_collection_db, tmdb_collection_db, settings_db, user_db, media_db, queries_db, watchlist_db
from database.connection import get_db_connection
from database.log_db import LogDBManager
from handler.p115_service import P115Service, SmartOrganizer, get_config
from services.subscribe_assistant.manager import SubscribeAssistantManager
from tasks.p115_fingerprint_helpers import p115_fp_is_virtual_strm_target, p115_fp_read_strm_target
try:
    from p115client import P115Client
except ImportError:
    P115Client = None
import logging
logger = logging.getLogger(__name__)

# 创建一个新的蓝图
webhook_bp = Blueprint('webhook_bp', __name__)

# --- 模块级变量 ---
WEBHOOK_REQUEUE_DELAY = 5
WEBHOOK_PENDING_TASKS = collections.deque()
WEBHOOK_PENDING_TASKS_LOCK = threading.Lock()
WEBHOOK_PENDING_TASKS_DRAINER = None
EMBY_METADATA_BACKFILL_DEBOUNCE_SECONDS = 120
EMBY_METADATA_BACKFILL_RECENT = {}
EMBY_METADATA_BACKFILL_RECENT_LOCK = threading.Lock()

UPDATE_DEBOUNCE_TIMERS = {}
UPDATE_DEBOUNCE_LOCK = threading.Lock()
UPDATE_DEBOUNCE_TIME = 15
# --- MP 单文件上传智能合并缓冲池 ---
MP_BATCH_QUEUE = {}
MP_BATCH_LOCK = threading.Lock()
ACTIVE_SERIES_NOTIFICATION_QUEUE = {}
ACTIVE_SERIES_NOTIFICATION_TIMERS = {}
ACTIVE_SERIES_NOTIFICATION_LOCK = threading.Lock()
ACTIVE_SERIES_NOTIFICATION_DEBOUNCE_TIME = 30
ACTIVE_EMBY_DISPATCH_QUEUE = {}
ACTIVE_EMBY_DISPATCH_TIMERS = {}
ACTIVE_EMBY_DISPATCH_SEEN = {}
ACTIVE_EMBY_DISPATCH_LOCK = threading.Lock()
ACTIVE_EMBY_DISPATCH_DEBOUNCE_TIME = 10
ACTIVE_EMBY_DISPATCH_SEEN_TTL = 120


def _flush_active_series_notification(series_key: str):
    with ACTIVE_SERIES_NOTIFICATION_LOCK:
        pending = ACTIVE_SERIES_NOTIFICATION_QUEUE.pop(series_key, None)
        ACTIVE_SERIES_NOTIFICATION_TIMERS.pop(series_key, None)
    if not pending:
        return
    try:
        telegram.send_media_notification(
            item_details=pending["item_details"],
            notification_type=pending["notification_type"],
            new_episode_ids=pending["new_episode_ids"] or None,
        )
    except Exception as e:
        logger.error(f"触发主动入库聚合通知失败: {e}")


def _enqueue_active_series_notification(item_details: dict, notification_type: str, new_episode_ids: List[str]):
    provider_ids = item_details.get("ProviderIds") if isinstance(item_details.get("ProviderIds"), dict) else {}
    series_key = str(item_details.get("Id") or provider_ids.get("Tmdb") or item_details.get("Name") or "").strip()
    if not series_key:
        telegram.send_media_notification(
            item_details=item_details,
            notification_type=notification_type,
            new_episode_ids=new_episode_ids or None,
        )
        return

    with ACTIVE_SERIES_NOTIFICATION_LOCK:
        pending = ACTIVE_SERIES_NOTIFICATION_QUEUE.setdefault(series_key, {
            "item_details": dict(item_details),
            "notification_type": notification_type,
            "new_episode_ids": [],
        })
        pending["item_details"] = dict(item_details)
        if notification_type == "new":
            pending["notification_type"] = "new"
        for episode_id in new_episode_ids or []:
            episode_id = str(episode_id or "").strip()
            if episode_id and episode_id not in pending["new_episode_ids"]:
                pending["new_episode_ids"].append(episode_id)

        old_timer = ACTIVE_SERIES_NOTIFICATION_TIMERS.get(series_key)
        if old_timer is not None:
            old_timer.kill()
        ACTIVE_SERIES_NOTIFICATION_TIMERS[series_key] = spawn_later(
            ACTIVE_SERIES_NOTIFICATION_DEBOUNCE_TIME,
            _flush_active_series_notification,
            series_key,
        )

    logger.info(
        "  ➜ [主动入库通知] 《%s》已聚合 %s 个单集通知，%s 秒内有新集将继续合并。",
        item_details.get("Name") or series_key,
        len(pending["new_episode_ids"]),
        ACTIVE_SERIES_NOTIFICATION_DEBOUNCE_TIME,
    )


def _should_skip_non_etk_strm_webhook(item_type: str, item_name: str, item_path: str) -> bool:
    """Emby 事件只处理 ETK 自己生成的 STRM，避免第三方 STRM 进入整理/刮削链路。"""
    if str(item_type or '') not in {'Movie', 'Episode'}:
        return False
    path = str(item_path or '').strip()
    if not path.lower().endswith('.strm'):
        return False
    try:
        from monitor_service import _is_etk_standard_strm
        if _is_etk_standard_strm(path):
            return False
    except Exception as e:
        logger.warning(f"  ➜ [Emby事件] STRM 标准校验失败，已跳过：{item_name or os.path.basename(path)}，原因：{e}")
        return True
    logger.warning(f"  ➜ [Emby事件] 非 ETK 标准 STRM，已跳过：{item_name or os.path.basename(path)}")
    return True


def _extract_virtual_id_from_webhook(data):
    texts = []
    for key in ('Path', 'MediaSourceId'):
        value = (data.get('Item') or {}).get(key)
        if value:
            texts.append(str(value))
    for key in ('Path', 'Url', 'MediaSourceId'):
        value = (data.get('PlaybackInfo') or {}).get(key)
        if value:
            texts.append(str(value))
    try:
        texts.append(json.dumps(data, ensure_ascii=False, default=str))
    except Exception:
        pass
    for text in texts:
        match = re.search(r'/api/p115/virtual-play/(\d+)', text)
        if match:
            return int(match.group(1))
    return 0


def _is_virtual_import_emby_id(emby_id: str) -> bool:
    emby_id = str(emby_id or '').strip()
    if not emby_id:
        return False
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT asset_details_json
                    FROM media_metadata
                    WHERE emby_item_ids_json ? %s
                    LIMIT 1
                    """,
                    (emby_id,),
                )
                row = cursor.fetchone()
        if not row:
            return False
        assets = row.get('asset_details_json') or []
        if isinstance(assets, str):
            assets = json.loads(assets or '[]')
        if isinstance(assets, dict):
            assets = [assets]
        for asset in assets or []:
            if not isinstance(asset, dict):
                continue
            path = asset.get('path') or asset.get('Path')
            if p115_fp_is_virtual_strm_target(path):
                return True
            target = p115_fp_read_strm_target(path)
            if p115_fp_is_virtual_strm_target(target):
                return True
    except Exception as e:
        logger.debug(f"  ➜ [虚拟入库] 判断 EmbyID={emby_id} 是否虚拟入库失败: {e}")
    return False


def _filter_real_episode_ids(episode_ids: Optional[List[str]]) -> List[str]:
    real_ids = []
    skipped = 0
    for eid in episode_ids or []:
        eid = str(eid or '').strip()
        if not eid:
            continue
        if _is_virtual_import_emby_id(eid):
            skipped += 1
            continue
        real_ids.append(eid)
    if skipped:
        logger.info(f"  ➜ [虚拟入库] 已跳过 {skipped} 个虚拟分集触发的 MP/追剧新集联动，等待正式入库。")
    return real_ids


def _maybe_promote_virtual_import_from_playback(data):
    virtual_id = _extract_virtual_id_from_webhook(data or {})
    if not virtual_id:
        return
    try:
        from database import shared_virtual_db
        row = shared_virtual_db.get_virtual_import(virtual_id)
        if not row or row.get('status') != 'virtual':
            return
        cfg = settings_db.get_shared_resource_config() or {}
        item_type = str(row.get('item_type') or '').lower()
        playback = data.get('PlaybackInfo') or {}
        percent = 0.0
        for key in ('PlayedPercentage', 'PlayedPercent'):
            if playback.get(key) not in (None, ''):
                percent = float(playback.get(key) or 0)
                break
        if not percent:
            pos = float(playback.get('PositionTicks') or 0)
            runtime = float((data.get('Item') or {}).get('RunTimeTicks') or playback.get('RunTimeTicks') or 0)
            if pos > 0 and runtime > 0:
                percent = min(100.0, pos * 100.0 / runtime)
        row = shared_virtual_db.record_virtual_play(virtual_id, percent=percent)
        episode_threshold = int(cfg.get('p115_shared_virtual_auto_promote_episodes') or 0)
        movie_threshold = int(cfg.get('p115_shared_virtual_auto_promote_movie_percent') or 0)
        should_promote = False
        if item_type == 'movie' and movie_threshold > 0 and float(row.get('played_percent') or 0) >= movie_threshold:
            should_promote = True
        elif item_type != 'movie' and episode_threshold > 0 and int(row.get('watched_count') or 0) >= episode_threshold:
            should_promote = True
        if not should_promote:
            logger.debug(
                "  ➜ [虚拟入库] 播放阈值未触发自动转正：virtual_id=%s, item_type=%s, watched=%s/%s, percent=%.2f/%s",
                virtual_id,
                row.get('item_type') or item_type,
                int(row.get('watched_count') or 0),
                episode_threshold,
                float(row.get('played_percent') or 0),
                movie_threshold,
            )
            return
        from tasks.shared_resource_tasks import consume_device_event_with_transfer_gate
        source = row.get('source_payload_json') if isinstance(row.get('source_payload_json'), dict) else {}
        event = {
            'event_id': '',
            'source_kind': source.get('source_kind') or row.get('source_kind') or '',
            'source_ref_id': source.get('source_id') or source.get('source_ref_id') or row.get('source_id') or '',
            'payload_json': {**source, '_virtual_auto_promote': True, '_virtual_id': virtual_id},
        }
        marked = shared_virtual_db.mark_active_washing_for_virtual_import(virtual_id, True)
        logger.info(f"  ➜ [虚拟入库] 自动转正已开启 active_washing 特权：virtual_id={virtual_id}, rows={marked}")
        result = consume_device_event_with_transfer_gate(event, ack=False)
        if result.get('ok'):
            shared_virtual_db.update_virtual_import(virtual_id, status='promoting', promoted_at='NOW()')
            logger.info(f"  ➜ [虚拟入库] 播放阈值触发自动转正，保留虚拟 STRM 等待正式入库完成：virtual_id={virtual_id}")
    except Exception as e:
        logger.warning(f"  ➜ [虚拟入库] 播放阈值自动转正失败：virtual_id={virtual_id}, err={e}")


def _cleanup_promoting_virtual_imports_after_library_ready(item_details: dict, item_type: str, tmdb_id: str) -> dict:
    tmdb_id = str(tmdb_id or '').strip()
    if not tmdb_id:
        return {'records': 0, 'cache': 0}

    try:
        from database import shared_virtual_db
        item_type_text = str(item_type or '').strip()
        season_number = None
        if item_type_text in {'Season', 'Episode'}:
            season_number = item_details.get('IndexNumber') if item_type_text == 'Season' else item_details.get('ParentIndexNumber')
        data = shared_virtual_db.list_virtual_imports(status='promoting', page=1, page_size=200)
        candidates = []
        for row in data.get('items') or []:
            row_item_type = str(row.get('item_type') or '').strip().lower()
            row_tmdb = str(
                row.get('parent_series_tmdb_id')
                or row.get('tmdb_id')
                or ''
            ).strip()
            if item_type_text == 'Movie':
                if row_item_type == 'movie' and row_tmdb == tmdb_id:
                    candidates.append(row)
            elif item_type_text in {'Series', 'Season', 'Episode'}:
                if row_item_type in {'series', 'season', 'episode', 'tv'} and row_tmdb == tmdb_id:
                    if season_number in (None, '') or row.get('season_number') in (None, season_number):
                        candidates.append(row)

        stats = {'records': 0, 'cache': 0}
        for row in candidates:
            virtual_id = int(row.get('id') or 0)
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "DELETE FROM p115_filesystem_cache WHERE parent_id = %s OR id LIKE %s",
                            (f"virtual:{virtual_id}", f"virtual:{virtual_id}:%"),
                        )
                        stats['cache'] += cursor.rowcount or 0
                    conn.commit()
            except Exception as e:
                logger.debug(f"  ➜ [虚拟入库] 正式入库后清理虚拟缓存失败: virtual_id={virtual_id} -> {e}")
            try:
                cleared = shared_virtual_db.mark_active_washing_for_virtual_import(virtual_id, False)
                if cleared:
                    logger.info(f"  ➜ [虚拟入库] 正式入库完成后收回 active_washing 临时特权：virtual_id={virtual_id}, rows={cleared}")
            except Exception as e:
                logger.debug(f"  ➜ [虚拟入库] 正式入库后收回 active_washing 失败: virtual_id={virtual_id} -> {e}")
            if shared_virtual_db.delete_virtual_import(virtual_id):
                stats['records'] += 1
        if stats['records']:
            logger.info(
                "  ➜ [虚拟入库] 正式入库完成后清理转正中虚拟记录：records=%s, cache=%s",
                stats['records'], stats['cache'],
            )
        return stats
    except Exception as e:
        logger.warning(f"  ➜ [虚拟入库] 正式入库后清理转正中虚拟项失败: {e}")
        return {'records': 0, 'cache': 0, 'error': str(e)}


def _submit_webhook_media_task(
    task_name,
    *,
    task_function=None,
    processor_type='media',
    from_pending_queue=False,
    **kwargs,
):
    task_function = task_function or _handle_full_processing_flow
    task_payload = {
        "task_name": task_name,
        "task_function": task_function,
        "processor_type": processor_type,
        "kwargs": dict(kwargs),
    }
    submitted = task_manager.submit_task(
        task_function,
        task_name=task_name,
        processor_type=processor_type,
        **kwargs,
    )
    if submitted:
        if from_pending_queue:
            logger.info(f"  ➜ [Emby事件队列] 任务 '{task_name}' 已从待提交队列成功分派。")
        return True

    if from_pending_queue:
        logger.debug(f"  ➜ [Emby事件队列] 任务 '{task_name}' 分派时媒体任务仍繁忙，稍后继续尝试。")
        return False

    logger.info(f"  ➜ [Emby事件队列] 任务 '{task_name}' 因媒体任务繁忙，已加入待提交队列。")
    _enqueue_pending_webhook_task(task_payload)
    return False


def _is_same_pending_webhook_task(existing_task, new_task):
    return (
        existing_task.get("task_name") == new_task.get("task_name")
        and existing_task.get("processor_type") == new_task.get("processor_type")
        and existing_task.get("task_function") == new_task.get("task_function")
        and existing_task.get("kwargs") == new_task.get("kwargs")
    )


def _merge_pending_webhook_task(existing_task, new_task):
    if (
        existing_task.get("processor_type") != new_task.get("processor_type")
        or existing_task.get("task_function") != new_task.get("task_function")
    ):
        return False

    existing_kwargs = existing_task.get("kwargs") or {}
    new_kwargs = new_task.get("kwargs") or {}
    if str(existing_kwargs.get("item_id") or "") != str(new_kwargs.get("item_id") or ""):
        return False

    existing_episode_ids = existing_kwargs.get("new_episode_ids")
    new_episode_ids = new_kwargs.get("new_episode_ids")
    if not existing_episode_ids or not new_episode_ids:
        return False

    merged_episode_ids = sorted({
        str(episode_id)
        for episode_id in list(existing_episode_ids) + list(new_episode_ids)
        if str(episode_id or "").strip()
    })
    existing_kwargs["new_episode_ids"] = merged_episode_ids
    existing_kwargs["is_new_item"] = bool(
        existing_kwargs.get("is_new_item") or new_kwargs.get("is_new_item")
    )
    existing_kwargs["force_full_update"] = bool(
        existing_kwargs.get("force_full_update") or new_kwargs.get("force_full_update")
    )
    existing_kwargs["aggregate_notification"] = bool(merged_episode_ids)
    if existing_kwargs["is_new_item"] and existing_task.get("task_name", "").startswith("主动追更:"):
        existing_task["task_name"] = existing_task["task_name"].replace("主动追更:", "主动入库:", 1)
    return True


def _schedule_pending_webhook_drain(delay=WEBHOOK_REQUEUE_DELAY):
    global WEBHOOK_PENDING_TASKS_DRAINER
    with WEBHOOK_PENDING_TASKS_LOCK:
        if WEBHOOK_PENDING_TASKS_DRAINER is not None:
            return
        WEBHOOK_PENDING_TASKS_DRAINER = spawn_later(delay, _drain_pending_webhook_tasks)


def _enqueue_pending_webhook_task(task_payload):
    with WEBHOOK_PENDING_TASKS_LOCK:
        for pending_task in WEBHOOK_PENDING_TASKS:
            if _merge_pending_webhook_task(pending_task, task_payload):
                logger.info(
                    "  ➜ [Emby事件队列] 已合并同剧任务 '%s'，当前包含 %s 个分集。",
                    pending_task["task_name"],
                    len(pending_task["kwargs"].get("new_episode_ids") or []),
                )
                break
            if _is_same_pending_webhook_task(pending_task, task_payload):
                logger.debug(f"  ➜ [Emby事件队列] 任务 '{task_payload['task_name']}' 已在待提交队列中，跳过重复入队。")
                break
        else:
            WEBHOOK_PENDING_TASKS.append(task_payload)
            logger.info(
                f"  ➜ [Emby事件队列] 当前待提交任务数: {len(WEBHOOK_PENDING_TASKS)} "
                f"(最新: {task_payload['task_name']})"
            )

    _schedule_pending_webhook_drain()


def _drain_pending_webhook_tasks():
    global WEBHOOK_PENDING_TASKS_DRAINER
    try:
        while True:
            with WEBHOOK_PENDING_TASKS_LOCK:
                if not WEBHOOK_PENDING_TASKS:
                    return
                task_payload = WEBHOOK_PENDING_TASKS[0]

            submitted = _submit_webhook_media_task(
                task_payload["task_name"],
                task_function=task_payload["task_function"],
                processor_type=task_payload["processor_type"],
                from_pending_queue=True,
                **task_payload["kwargs"],
            )
            if not submitted:
                return

            with WEBHOOK_PENDING_TASKS_LOCK:
                if WEBHOOK_PENDING_TASKS and _is_same_pending_webhook_task(WEBHOOK_PENDING_TASKS[0], task_payload):
                    WEBHOOK_PENDING_TASKS.popleft()
    finally:
        with WEBHOOK_PENDING_TASKS_LOCK:
            WEBHOOK_PENDING_TASKS_DRAINER = None
            has_pending_tasks = bool(WEBHOOK_PENDING_TASKS)
        if has_pending_tasks:
            _schedule_pending_webhook_drain()


def _first_mp_detail_value(data, *keys):
    for key in keys:
        value = data.get(key)
        if value not in (None, '', [], {}):
            return value
    return None


def _normalize_mp_detail_size(value):
    if value in (None, '', [], {}):
        return None
    try:
        if isinstance(value, (int, float)):
            size = int(value)
            return size if size > 0 else None
        text = str(value).strip().replace(',', '')
        if not text:
            return None
        if re.fullmatch(r'\d+(?:\.\d+)?', text):
            size = int(float(text))
            return size if size > 0 else None
    except Exception:
        return None
    return None


def _refresh_mp_file_info_from_115(client, file_info):
    """Use 115 file detail as the authority for MP webhook file identity."""
    try:
        file_id = file_info.get('file_id')
        if not file_id:
            return

        info_res = client.fs_get_info(file_id)
        if info_res and info_res.get('state') and info_res.get('data'):
            data = info_res['data']
            if isinstance(data, list):
                data = data[0] if data else {}
            if not isinstance(data, dict):
                return

            detail_size = _normalize_mp_detail_size(
                _first_mp_detail_value(data, 'size_byte', 'fs', 'file_size', 'size', 's')
            )
            if detail_size:
                old_size = _normalize_mp_detail_size(file_info.get('size') or file_info.get('fs'))
                file_info['size'] = detail_size
                file_info['fs'] = detail_size
                if old_size and old_size != detail_size:
                    logger.warning(
                        f"  ➜ [MP上传] 已用 115 实时详情修正文件大小: "
                        f"fid={file_id}, {old_size} -> {detail_size} | {file_info.get('name')}"
                    )
                else:
                    logger.debug(
                        f"  ➜ [MP上传] 已采用 115 实时详情文件大小: "
                        f"fid={file_id}, size={detail_size} | {file_info.get('name')}"
                    )

            detail_sha1 = _first_mp_detail_value(data, 'sha1', 'sha', 'file_sha1')
            if detail_sha1:
                file_info['sha1'] = str(detail_sha1).strip().upper()

            detail_pickcode = _first_mp_detail_value(data, 'pc', 'pick_code', 'pickcode')
            if detail_pickcode:
                file_info['pickcode'] = str(detail_pickcode).strip()

            detail_name = _first_mp_detail_value(data, 'fn', 'n', 'file_name', 'name')
            if detail_name and not file_info.get('name'):
                file_info['name'] = str(detail_name)

            real_parent_id = data.get('parent_id') or data.get('pid') or data.get('cid')
            if not real_parent_id and 'paths' in data and isinstance(data['paths'], list) and len(data['paths']) > 0:
                last_path_node = data['paths'][-1]
                real_parent_id = last_path_node.get('file_id') or last_path_node.get('cid')
            
            if real_parent_id and str(real_parent_id) != str(file_info.get('parent_id')):
                old_parent_id = file_info.get('parent_id')
                file_info['parent_id'] = str(real_parent_id)
                logger.info(
                    f"  ➜ [MP上传] 父目录已修正: "
                    f"{old_parent_id} -> {real_parent_id} | {file_info.get('name')}"
                )
    except Exception as e:
        logger.warning(f"  ➜ [MP上传] 查询 115 文件详情失败，沿用 MP 通知字段: {file_info.get('name')} -> {e}")

def _flush_mp_batch(key):
    """缓冲结束，将收集到的同集视频和字幕打包送入核心处理"""
    with MP_BATCH_LOCK:
        if key not in MP_BATCH_QUEUE:
            return
        task = MP_BATCH_QUEUE.pop(key)

    files = task.get('files') or []
    if not files:
        return

    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [MP合并整理] 115 客户端未初始化，任务取消。")
        return

    tmdb_id, media_type, season_num, episode_num = key
    title = files[0].get('title') or ''

    video_text = "包含视频" if task.get('has_video', False) else "仅字幕或附属文件"
    logger.info(
        f"  ➜ [MP合并整理] 缓冲结束，开始处理 {len(files)} 个文件，{video_text}，TMDb：{tmdb_id}"
    )

    try:
        organizer = SmartOrganizer(client, tmdb_id, media_type, title)

        if season_num is not None and str(season_num).isdigit():
            organizer.forced_season = int(season_num)

        file_nodes = []
        for f in files:
            _refresh_mp_file_info_from_115(client, f)
            file_nodes.append({
                'fid': f.get('file_id'),
                'file_id': f.get('file_id'),
                'fn': f.get('name'),
                'file_name': f.get('name'),
                'fc': '1',
                'type': '1',
                'pid': f.get('parent_id'),
                'parent_id': f.get('parent_id'),
                'pc': f.get('pickcode'),
                'pick_code': f.get('pickcode'),
                'sha1': f.get('sha1'),
                'size': f.get('size'),
                'fs': f.get('fs') or f.get('size'),
                '115_path': f.get('115_path'), # ★ 核心新增：将 115 物理路径传递给底层
                '_forced_season': f.get('season_num'),
                '_forced_episode': f.get('episode_num'),
                '_skip_gc': True,   
                '_from_mp': True    
            })

        config = get_config()
        mp_classify_enabled = bool(config.get(constants.CONFIG_OPTION_115_MP_CLASSIFY, False))

        if mp_classify_enabled:
            logger.info("  ➜ [MP直出] MP分类已开启：跳过整理/归类/重命名，生成 STRM 并缓存媒体信息。")
            ok = organizer.execute_mp_passthrough(file_nodes)
            if not ok:
                logger.warning("  ➜ [MP直出] 直出处理未完全成功。")
        else:
            target_cid = organizer.get_target_cid(
                season_num=organizer.forced_season if hasattr(organizer, 'forced_season') else None
            )

            if target_cid:
                organizer.execute(file_nodes, target_cid, skip_gc=True)
            else:
                logger.info("  ➜ [MP合并整理] 未命中分类规则，保持原样。")

    except Exception as e:
        logger.error(f"  ➜ [MP合并整理] 失败: {e}", exc_info=True)

def _process_mp_passthrough_immediate(file_info):
    """MP直出模式：跳过缓冲，直接处理单文件"""
    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [MP直出] 115 客户端未初始化，任务取消。")
        return

    tmdb_id = file_info.get('tmdb_id')
    media_type = file_info.get('media_type')
    title = file_info.get('title') or ''
    file_name = file_info.get('name')

    _refresh_mp_file_info_from_115(client, file_info)
    file_name = file_info.get('name') or file_name

    logger.info(f"  ➜ [MP直出] 开始处理单文件：{file_name}。")
    logger.debug(f"  ➜ [MP直出] 单文件处理详情：TMDb={tmdb_id}, 类型={media_type}")

    try:
        organizer = SmartOrganizer(client, tmdb_id, media_type, title)
        season_num = file_info.get('season_num')
        if season_num is not None and str(season_num).isdigit():
            organizer.forced_season = int(season_num)

        file_nodes = [{
            'fid': file_info.get('file_id'),
            'file_id': file_info.get('file_id'),
            'fn': file_name,
            'file_name': file_name,
            'fc': '1',
            'type': '1',
            'pid': file_info.get('parent_id'),
            'parent_id': file_info.get('parent_id'),
            'pc': file_info.get('pickcode'),
            'pick_code': file_info.get('pickcode'),
            'sha1': file_info.get('sha1'),
            'size': file_info.get('size'),
            'fs': file_info.get('fs') or file_info.get('size'),
            '115_path': file_info.get('115_path'),
            '_forced_season': season_num,
            '_forced_episode': file_info.get('episode_num'),
            '_skip_gc': True,   
            '_from_mp': True    
        }]

        ok = organizer.execute_mp_passthrough(file_nodes)
        if not ok:
            logger.warning("  ➜ [MP直出] 直出处理未完全成功。")

    except Exception as e:
        logger.error(f"  ➜ [MP直出] 失败: {e}", exc_info=True)

def _enqueue_mp_file(file_info):
    """将 MP 上传的文件加入缓冲池 (视频叫醒字幕机制)"""
    with MP_BATCH_LOCK:
        # 以 TMDB ID + 季号 + 集号 作为唯一批次 Key
        key = (file_info['tmdb_id'], file_info['media_type'], file_info.get('season_num'), file_info.get('episode_num'))
        
        if key not in MP_BATCH_QUEUE:
            MP_BATCH_QUEUE[key] = {
                'files': [],
                'timer': None,
                'has_video': False
            }
        
        task = MP_BATCH_QUEUE[key]
        task['files'].append(file_info)
        
        file_name = file_info['name']
        ext = file_name.split('.')[-1].lower() if '.' in file_name else ''
        is_video = ext in ['mp4', 'mkv', 'avi', 'ts', 'iso', 'rmvb', 'wmv', 'mov', 'm2ts', 'flv', 'mpg']
        
        if is_video:
            task['has_video'] = True
        
        # 只要有新文件进来，就重置计时器
        if task['timer'] is not None:
            task['timer'].kill()
        
        # ★ 核心机制：如果视频到了，只等 5 秒(防并发)就发车；如果只有字幕，最多等 2 小时！
        delay = 5.0 if task['has_video'] else 7200.0
        
        logger.info(f"  ➜ [MP缓冲] 文件 '{file_name}' 加入队列。当前批次 {len(task['files'])} 个文件。最多等待 {delay} 秒后合并执行...")
        task['timer'] = spawn_later(delay, _flush_mp_batch, key)


def _shared_resource_auto_share_enabled() -> bool:
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        value = cfg.get('p115_shared_resource_enabled', False)
        if isinstance(value, str):
            return value.strip().lower() in ('1', 'true', 'yes', 'on', '启用', '开启')
        return bool(value)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 读取共享资源总开关失败，跳过自动登记: {e}")
        return False


def _run_shared_auto_share_batch_detached(task_name: str, register_items: List[dict]):
    """共享供给侧登记必须脱离 task_manager 单线程队列。

    Emby 事件本身已经运行在 task_manager 的单 worker + task_lock 中。
    如果这里再 submit_task，会在同一线程内二次获取 task_lock，导致事件任务假死。

    Rapid v2 不再判断“是否有人需要”。只要共享资源开关已启用，
    媒体入库完成并补齐指纹后，就立即把本机秒传源登记到中心。
    """
    items = [dict(x or {}) for x in (register_items or []) if isinstance(x, dict)]
    if not items:
        return

    if not _shared_resource_auto_share_enabled():
        logger.debug(f"  ➜ [共享资源] 共享资源未启用，跳过自动登记: {task_name}")
        return

    def _runner():
        created_total = 0
        failed_total = 0
        try:
            from tasks.shared_resource_tasks import (
                trigger_shared_rapid_register_batch_for_library_items,
                trigger_shared_rapid_register_for_library_item,
            )

            logger.info(f"  ➜ [共享资源] 入库后自动登记共享源：{task_name}，共 {len(items)} 个文件。")
            if len(items) > 1:
                batch_result = trigger_shared_rapid_register_batch_for_library_items(None, items) or {}
                try:
                    created_total += int(batch_result.get('created', 0) or 0)
                except Exception:
                    pass
                failed_total += int(batch_result.get('failed', 0) or 0)
                raw_result = batch_result.get('raw_batch_result') or {}
                logger.info(
                    "  ➜ [共享资源] 媒体信息预上传完成：%s，本次上传 %s 个，中心已有 %s 个，可用于秒传校验 %s 个。",
                    task_name,
                    raw_result.get('uploaded_count') or 0,
                    raw_result.get('skipped_existing') or 0,
                    raw_result.get('count') or 0,
                )
                logger.info(
                    "  ➜ [共享资源] 入库共享源登记完成：%s，共 %s 个文件，成功 %s 个，失败 %s 个。",
                    task_name,
                    len(items),
                    created_total,
                    failed_total,
                )
                return
            for item in items:
                try:
                    result = trigger_shared_rapid_register_for_library_item(None, **item) or {}
                    try:
                        created_total += int(result.get('created', 0) or result.get('registered_count', 0) or 0)
                    except Exception:
                        pass
                    if not result.get('ok'):
                        failed_total += 1
                        logger.debug(
                            "  ➜ [共享资源] 入库登记未成功: %s，message=%s",
                            item.get('emby_item_id') or item.get('tmdb_id') or '-',
                            result.get('message', '') if isinstance(result, dict) else '',
                        )
                except Exception as item_err:
                    failed_total += 1
                    logger.warning(
                        "  ➜ [共享资源] 入库登记单项失败: %s -> %s",
                        item.get('emby_item_id') or item.get('tmdb_id') or '-',
                        item_err,
                        exc_info=True,
                    )

            logger.info(
                "  ➜ [共享资源] 入库共享源登记完成：%s，共 %s 个文件，成功 %s 个，失败 %s 个。",
                task_name,
                len(items),
                created_total,
                failed_total,
            )
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 自动登记失败: {task_name} -> {e}", exc_info=True)

    threading.Thread(
        target=_runner,
        name=f"shared-rapid-register-{str(task_name)[:40]}",
        daemon=True,
    ).start()


def _run_shared_auto_share_detached(task_name: str, **kwargs):
    _run_shared_auto_share_batch_detached(task_name, [kwargs])


def _submit_shared_auto_share_after_library_ready(
    item_details: dict,
    item_id: str,
    item_type: str,
    tmdb_id: str,
    *,
    new_episode_ids: Optional[List[str]] = None,
):
    """媒体入库完成后，异步登记 Rapid v2 共享源。

    新逻辑下客户端不再负责季包一致性和完结季成包判断：
    - Movie：入库完成即登记电影源；
    - Series + new_episode_ids：入库完成即登记本轮新增分集源；
    - 中心端根据单集资产池自行归类、凑整季、判定 pool_complete。
    """
    try:
        if not tmdb_id:
            return

        title = item_details.get('Name') or ''
        year = item_details.get('ProductionYear') or ''

        if item_type == 'Movie':
            _run_shared_auto_share_batch_detached(
                f"Rapid电影共享源登记: {title or tmdb_id}",
                [{
                    'item_type': 'Movie',
                    'tmdb_id': str(tmdb_id),
                    'emby_item_id': str(item_id),
                    'title': title,
                    'year': year,
                }],
            )
            return

        if item_type == 'Series':
            precise_episode_ids = []
            for eid in new_episode_ids or []:
                eid = str(eid or '').strip()
                if eid and eid not in precise_episode_ids:
                    precise_episode_ids.append(eid)
            if not precise_episode_ids:
                return
            _run_shared_auto_share_batch_detached(
                f"Rapid分集共享源登记: {title or tmdb_id}",
                [{
                    'item_type': 'Episode',
                    'emby_item_id': eid,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'title': title,
                    'year': year,
                } for eid in precise_episode_ids],
            )
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 提交 webhook Rapid 共享源登记失败: {e}", exc_info=True)

def _get_processor_local_strm_root(processor) -> str:
    """从 MediaProcessor / 配置中提取本地 STRM 根目录，用于补齐 p115_filesystem_cache.local_path。"""
    candidates = []

    if processor:
        for attr in (
            'local_strm_root',
            'p115_local_strm_root',
            'strm_root',
            'p115_strm_root',
        ):
            try:
                value = getattr(processor, attr, None)
                if value:
                    candidates.append(value)
            except Exception:
                pass

    try:
        nb_config = get_config() or {}
        for key in (
            'local_strm_root',
            'p115_local_strm_root',
            'strm_root',
            'p115_strm_root',
        ):
            value = nb_config.get(key)
            if value:
                candidates.append(value)
    except Exception:
        pass

    try:
        app_config = config_manager.APP_CONFIG or {}
        for key in (
            'local_strm_root',
            'p115_local_strm_root',
            'strm_root',
            'p115_strm_root',
        ):
            value = app_config.get(key)
            if value:
                candidates.append(value)
    except Exception:
        pass

    for value in candidates:
        text = str(value).strip().replace('\\', '/').rstrip('/')
        if text:
            return text

    return ''


def _repair_webhook_p115_fingerprints_for_emby_ids(
    processor,
    item_name_for_log: str,
    emby_item_ids,
    *,
    expected_item_type: Optional[str] = None,
    log_prefix: str = "Emby事件指纹补齐",
) -> int:
    """Emby 事件入库后按 Item ID 找 media_metadata 行，并执行 115 指纹体检补齐。"""
    ids = [str(x).strip() for x in (emby_item_ids or []) if str(x or '').strip()]
    if not ids:
        return 0

    try:
        rows_to_repair = []
        seen_keys = set()

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for emby_id in ids:
                    if expected_item_type:
                        cursor.execute(
                            """
                            SELECT *
                            FROM media_metadata
                            WHERE item_type = %s
                              AND emby_item_ids_json::text LIKE %s
                            """,
                            (expected_item_type, f'%"{emby_id}"%')
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT *
                            FROM media_metadata
                            WHERE emby_item_ids_json::text LIKE %s
                            """,
                            (f'%"{emby_id}"%',)
                        )

                    for row in cursor.fetchall() or []:
                        row_dict = dict(row)
                        dedupe_key = (
                            row_dict.get("id"),
                            row_dict.get("tmdb_id"),
                            row_dict.get("parent_series_tmdb_id"),
                            row_dict.get("item_type"),
                            row_dict.get("season_number"),
                            row_dict.get("episode_number"),
                        )
                        if dedupe_key in seen_keys:
                            continue

                        seen_keys.add(dedupe_key)
                        rows_to_repair.append(row_dict)

        if not rows_to_repair:
            logger.debug(
                f"  ➜ [指纹补齐] 未在 media_metadata 中找到可体检记录: "
                f"{item_name_for_log} ids={ids} type={expected_item_type or 'Any'}"
            )
            return 0

        logger.info(
            f"  ➜ [指纹补齐] 正在为《{item_name_for_log}》执行 115 指纹体检，"
            f"共 {len(rows_to_repair)} 条记录。"
        )

        from tasks.p115_fingerprint_helpers import repair_p115_fingerprints_for_rows

        local_root = _get_processor_local_strm_root(processor)
        if not local_root:
            logger.warning(
                "  ➜ [指纹补齐] 未获取到 local_strm_root，本次只能补齐 PC/SHA1/115 缓存基础字段，无法可靠写入 local_path。"
            )

        repair_p115_fingerprints_for_rows(
            processor=processor,
            rows=rows_to_repair,
            local_root=local_root,
            update_db=True,
            allow_api_fetch=True,
            log_prefix=log_prefix,
        )

        return len(rows_to_repair)

    except Exception as e:
        logger.warning(f"  ➜ [指纹补齐] 执行失败: {e}", exc_info=True)
        return 0

def _handle_full_processing_flow(processor: 'MediaProcessor', item_id: str, force_full_update: bool, new_episode_ids: Optional[List[str]] = None, is_new_item: bool = True, aggregate_notification: bool = False):
    """
    【Emby 事件统一入口】
    统一处理 新入库(New) 和 追更(Update) 两种情况。
    """
    if not processor:
        logger.error(f"  ➜ 完整处理流程中止：核心处理器 (MediaProcessor) 未初始化。")
        return

    item_details = emby.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
    if not item_details:
        logger.error(f"  ➜ 无法获取项目 {item_id} 的详情，任务中止。")
        return
    
    item_name_for_log = item_details.get("Name", f"ID:{item_id}")
    item_type = item_details.get("Type")
    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")

    # 1. 核心调用：优先执行元数据处理 (process_single_item)
    processed_successfully = processor.process_single_item(
        item_id, 
        force_full_update=force_full_update,
        specific_episode_ids=new_episode_ids 
    )
    
    if not processed_successfully:
        logger.warning(f"  ➜ 项目 '{item_name_for_log}' 的元数据处理未成功完成，跳过后续步骤。")
        return

    # 2. 媒体入库后先做 115 指纹体检，再登记 Rapid 共享源。
    # Rapid 登记依赖 PC/SHA1/FID/缓存字段，体检要放在登记前。
    precise_new_episode_ids = _filter_real_episode_ids(new_episode_ids)
    if item_type == "Movie":
        _repair_webhook_p115_fingerprints_for_emby_ids(
            processor,
            item_name_for_log,
            [item_id],
            expected_item_type="Movie",
            log_prefix="Emby事件电影指纹补齐",
        )
        try:
            SubscribeAssistantManager(processor.config).sync_movie(
                tmdb_id=tmdb_id,
                movie_name=item_name_for_log,
            )
        except Exception as e:
            logger.warning(f"  ➜ [订阅助手] 电影入库收口失败：《{item_name_for_log}》 -> {e}", exc_info=True)
    elif item_type == "Series" and precise_new_episode_ids:
        _repair_webhook_p115_fingerprints_for_emby_ids(
            processor,
            item_name_for_log,
            precise_new_episode_ids,
            expected_item_type="Episode",
            log_prefix="Emby事件新集指纹补齐",
        )

    _cleanup_promoting_virtual_imports_after_library_ready(item_details, item_type, tmdb_id)

    # 3. 共享资源供给侧实时触发：电影/本轮新增分集均在 Emby 事件入库完成后登记；中心端负责后续整季归类。
    _submit_shared_auto_share_after_library_ready(
        item_details,
        item_id,
        item_type,
        tmdb_id,
        new_episode_ids=precise_new_episode_ids,
    )

    # 新剧仍要先登记纳管；实际状态判定和 MP 订阅统一交给后面的 watchlist 任务。
    if is_new_item and item_type == "Series":
        processor.check_and_add_to_watchlist(item_details, process_immediately=False)

    # 3. 后续处理
    if is_new_item:
        try:
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            item_name = item_details.get("Name", f"ID:{item_id}")
            
            # --- 匹配 List (榜单) 类型的合集 (保持不变) ---
            # 榜单类合集是静态的，需要将新入库的项目加入到 Emby 实体合集中
            if tmdb_id:
                updated_list_collections = custom_collection_db.match_and_update_list_collections_on_item_add(
                    new_item_tmdb_id=tmdb_id,
                    new_item_emby_id=item_id,
                    new_item_name=item_name
                )
                
                if updated_list_collections:
                    logger.info(f"  ➜ 《{item_name}》匹配到 {len(updated_list_collections)} 个榜单类合集，正在追加...")
                    for collection_info in updated_list_collections:
                        emby.append_item_to_collection(
                            collection_id=collection_info['emby_collection_id'],
                            item_emby_id=item_id,
                            base_url=processor.emby_url,
                            api_key=processor.emby_api_key,
                            user_id=processor.emby_user_id
                        )

            # ★★★ 移除 Filter 类合集的匹配逻辑 ★★★
            # Filter 类合集现在是基于 SQL 实时查询的，不需要在入库时做任何操作。
            # 只要 media_metadata 表更新了（process_single_item 已完成），SQL 查询自然能查到它。

        except Exception as e:
            logger.error(f"  ➜ 为新入库项目 '{item_name_for_log}' 匹配榜单合集时发生意外错误: {e}", exc_info=True)

        # --- 封面生成逻辑 (保持不变) ---
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}

            if cover_config.get("enabled") and cover_config.get("transfer_monitor"):
                # ... (获取 library_info 的逻辑) ...
                library_info = emby.get_library_root_for_item(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                
                if library_info:
                    library_id = library_info.get("Id")
                    library_name = library_info.get("Name", library_id)
                    
                    if library_info.get('CollectionType') in ['movies', 'tvshows', 'boxsets', 'mixed', 'music']:
                        server_id = 'main_emby'
                        library_unique_id = f"{server_id}-{library_id}"
                        if library_unique_id not in cover_config.get("exclude_libraries", []):
                            # ... (获取 item_count) ...
                            TYPE_MAP = {'movies': 'Movie', 'tvshows': 'Series', 'music': 'MusicAlbum', 'boxsets': 'BoxSet', 'mixed': 'Movie,Series'}
                            collection_type = library_info.get('CollectionType')
                            item_type_to_query = TYPE_MAP.get(collection_type)
                            item_count = 0
                            if library_id and item_type_to_query:
                                item_count = emby.get_item_count(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, parent_id=library_id, item_type=item_type_to_query) or 0

                            logger.info(f"  ➜ 正在为媒体库 '{library_name}' 生成封面 (当前实时数量: {item_count}) ---")
                            cover_service = CoverGeneratorService(config=cover_config)
                            cover_service.generate_for_library(emby_server_id=server_id, library=library_info, item_count=item_count)

            # ★★★ 移除 update_user_caches_on_item_add 调用 ★★★
            # 权限现在是实时的，不需要补票了。

        except Exception as e:
            logger.error(f"  ➜ 在新入库后执行封面生成时发生错误: {e}", exc_info=True)

    logger.trace(f"  ➜ Emby 事件任务及所有后续流程完成: '{item_name_for_log}'")

    # 4. ★★★ 通知分流 ★★★
    try:
        # 如果提供了 new_episode_ids，说明是追更通知
        # 如果 is_new_item 为 True，说明是新入库通知
        notif_type = 'update' if (precise_new_episode_ids and not is_new_item) else 'new'
        
        if aggregate_notification and item_type == "Series" and precise_new_episode_ids:
            _enqueue_active_series_notification(item_details, notif_type, precise_new_episode_ids)
        else:
            telegram.send_media_notification(
                item_details=item_details,
                notification_type=notif_type,
                new_episode_ids=precise_new_episode_ids or None
            )
    except Exception as e:
        logger.error(f"触发通知失败: {e}")

    logger.trace(f"  ➜ Emby 事件任务及所有后续流程完成: '{item_name_for_log}'")

    # 打标
    if is_new_item: 
        try:
            # 1. 从数据库获取最新记录
            db_record = media_db.get_media_details(str(tmdb_id), item_type)
            
            if db_record:
                # 2. 提取 Library ID
                # asset_details_json 是一个列表，取第一个即可
                assets = db_record.get('asset_details_json')
                lib_id = None
                if assets and isinstance(assets, list) and len(assets) > 0:
                    lib_id = assets[0].get('source_library_id')
                
                # 3. 提取修正后的分级 (US)
                # official_rating_json: {"US": "XXX", "DE": "18"}
                ratings = db_record.get('official_rating_json')
                us_rating = None
                if ratings and isinstance(ratings, dict):
                    us_rating = ratings.get('US')
                
                if lib_id:
                    # 既然数据都在手里了，不需要延迟，直接干！
                    logger.info(f"  ➜ [自动打标] 基于数据库最新元数据执行自动打标，分级：{us_rating}。")
                    logger.debug(f"  ➜ [自动打标] 媒体库 ID：{lib_id}")
                    # 这里的 lib_name 传个占位符即可，不影响逻辑，只影响日志
                    _handle_immediate_tagging_with_lib(item_id, item_name_for_log, lib_id, "DB_Source", known_rating=us_rating)
                else:
                    logger.warning(f"  ➜ [自动打标] 数据库记录中未找到 来源库，跳过打标。")
            else:
                logger.warning(f"  ➜ [自动打标] 无法从数据库读取刚写入的记录，跳过打标。")

        except Exception as e:
            logger.warning(f"  ➜ [自动打标] 触发打标失败: {e}")

    # 刷新智能追剧状态 
    if item_type == "Series" and tmdb_id:
        def _async_trigger_watchlist():
            try:
                watching_ids = watchlist_db.get_watching_tmdb_ids()
                
                # ★★★ 核心破局点：打破“不见兔子不撒鹰”的死锁 ★★★
                is_watching = str(tmdb_id) in watching_ids
                has_new_episodes = bool(precise_new_episode_ids) # 明确有新集物理文件入库
                has_virtual_episodes = bool(new_episode_ids and not precise_new_episode_ids)
                
                # 如果既不在追剧列表中，又没有新集入库，才真正跳过
                if has_virtual_episodes:
                    logger.info(
                        f"  ➜ [虚拟入库] 《{item_name_for_log}》本次仅包含虚拟分集，"
                        "不作为新集范围触发联动，但仍刷新整剧追剧状态。"
                    )

                if not is_watching and not has_new_episodes and not has_virtual_episodes:
                    logger.debug(f"  ➜ [智能追剧] 剧集 《{item_name_for_log}》 当前不在追剧列表中，且无新集触发，跳过刷新。")
                    return
                
                # 如果不在追剧列表中，但是有新集入库 -> 新季复活，强制唤醒！
                if not is_watching and has_new_episodes:
                    logger.info(f"  ➜ [智能追剧]  《{item_name_for_log}》 检测到有新集入库，重新开始追剧！")

                # =======================================================

                # 新集指纹体检与共享源登记均已在 Emby 事件链中完成；watchlist_processor 只负责追剧状态刷新。
                refresh_scope_text = (
                    f"本次只刷新 {len(precise_new_episode_ids)} 个新增分集。"
                    if precise_new_episode_ids
                    else "本次刷新整部剧集。"
                )
                logger.info(
                    f"  ➜ [智能追剧] 触发单项刷新：{refresh_scope_text}"
                )
                task_manager.submit_task(
                    task_process_watchlist,
                    task_name=f"刷新智能追剧: 《{item_name_for_log}》",
                    processor_type='watchlist', 
                    tmdb_id=str(tmdb_id),
                    new_episode_ids=precise_new_episode_ids or None,
                    subscription_triggering_episode_ids=new_episode_ids or None,
                )
            except Exception as e:
                logger.error(f"  ➜ 触发智能追剧任务失败: {e}")

        # 启动协程，不等待结果，直接让当前 Emby 事件任务结束
        spawn(_async_trigger_watchlist)

def _handle_immediate_tagging_with_lib(item_id, item_name, lib_id, lib_name, known_rating=None):
    """
    自动打标 (支持分级过滤)。
    增加 known_rating 参数：如果调用方已经知道确切分级（如从数据库查到的），直接使用，不再查询 Emby。
    """
    try:
        processor = extensions.media_processor_instance
        tagging_config = settings_db.get_setting('auto_tagging_rules') or []
        
        # 只有当没有传入 known_rating 时，才需要去 Emby 查
        item_details = None 
        
        for rule in tagging_config:
            target_libs = rule.get('library_ids', [])
            if not target_libs or lib_id in target_libs:
                tags = rule.get('tags', [])
                rating_filters = rule.get('rating_filters', [])
                
                if tags:
                    # ★★★ 核心修改：分级匹配逻辑 ★★★
                    if rating_filters:
                        # 1. 优先使用传入的已知分级 (数据库里的真理)
                        current_rating = known_rating
                        
                        # 2. 如果没传，且还没查过 Emby，则去查 (兜底逻辑)
                        if not current_rating and item_details is None:
                            item_details = emby.get_emby_item_details(
                                item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
                                fields="OfficialRating"
                            )
                            if item_details:
                                current_rating = item_details.get('OfficialRating')
                        
                        # 3. 执行匹配
                        if not current_rating:
                            continue # 拿不到分级，跳过
                            
                        target_codes = queries_db._expand_rating_labels(rating_filters)
                        
                        # 兼容 "US: XXX" 和 "XXX" 两种格式
                        rating_code = current_rating.split(':')[-1].strip()
                        
                        if rating_code not in target_codes:
                            logger.debug(f"  ➜ 媒体项 '{item_name}' 分级 '{current_rating}' 不满足规则限制 {rating_filters}，跳过打标。")
                            continue

                    if rating_filters:
                        rule_desc = f"分级 '{','.join(rating_filters)}'"
                    else:
                        rule_desc = f"库 '{lib_name}'"

                    logger.info(f"  ➜ 媒体项 '{item_name}' 命中 {rule_desc} 规则，追加标签: {tags}")
                    emby.add_tags_to_item(item_id, tags, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                
                break 
    except Exception as e:
        logger.error(f"  ➜ [自动打标] 失败: {e}")

def _claim_active_emby_item(item_id: str) -> bool:
    now = time.monotonic()
    with ACTIVE_EMBY_DISPATCH_LOCK:
        expired = [key for key, deadline in ACTIVE_EMBY_DISPATCH_SEEN.items() if deadline <= now]
        for key in expired:
            ACTIVE_EMBY_DISPATCH_SEEN.pop(key, None)
        if ACTIVE_EMBY_DISPATCH_SEEN.get(item_id, 0) > now:
            return False
        ACTIVE_EMBY_DISPATCH_SEEN[item_id] = now + ACTIVE_EMBY_DISPATCH_SEEN_TTL
        return True


def _release_active_emby_item(item_id: str):
    with ACTIVE_EMBY_DISPATCH_LOCK:
        ACTIVE_EMBY_DISPATCH_SEEN.pop(item_id, None)


def _flush_active_emby_series_dispatch(series_id: str):
    with ACTIVE_EMBY_DISPATCH_LOCK:
        pending = ACTIVE_EMBY_DISPATCH_QUEUE.pop(series_id, None)
        ACTIVE_EMBY_DISPATCH_TIMERS.pop(series_id, None)
    if not pending:
        return

    processor = extensions.media_processor_instance
    if not processor:
        return

    episode_ids = sorted(pending["episode_ids"])
    series_name = pending.get("name") or ""
    if not series_name:
        series = emby.get_emby_item_details(
            series_id,
            processor.emby_url,
            processor.emby_api_key,
            processor.emby_user_id,
            fields="Name",
        )
        series_name = (series or {}).get("Name") or f"ID:{series_id}"

    already_processed = (
        series_id in processor.processed_items_cache
        and media_db.is_emby_id_in_library(series_id)
    )
    task_prefix = "主动追更" if already_processed else "主动入库"
    logger.info(
        "  ➜ [主动入库] 剧集 ItemID 聚合完成，分派 %s: %s (分集数: %s)",
        task_prefix,
        series_name,
        len(episode_ids),
    )
    _submit_webhook_media_task(
        f"{task_prefix}: {series_name}",
        item_id=series_id,
        force_full_update=False,
        new_episode_ids=episode_ids,
        is_new_item=not already_processed,
        aggregate_notification=bool(episode_ids),
    )


def _enqueue_active_emby_series_dispatch(series_id: str, series_name: str, episode_ids):
    with ACTIVE_EMBY_DISPATCH_LOCK:
        pending = ACTIVE_EMBY_DISPATCH_QUEUE.setdefault(
            series_id,
            {"name": "", "episode_ids": set()},
        )
        if series_name:
            pending["name"] = series_name
        pending["episode_ids"].update(str(x) for x in episode_ids if str(x or "").strip())

        old_timer = ACTIVE_EMBY_DISPATCH_TIMERS.get(series_id)
        if old_timer is not None:
            old_timer.kill()
        ACTIVE_EMBY_DISPATCH_TIMERS[series_id] = spawn_later(
            ACTIVE_EMBY_DISPATCH_DEBOUNCE_TIME,
            _flush_active_emby_series_dispatch,
            series_id,
        )
        pending_count = len(pending["episode_ids"])

    logger.info(
        "  ➜ [主动入库] 剧集 ItemID 等待聚合: SeriesId=%s，当前 %s 集。",
        series_id,
        pending_count,
    )


# --- 辅助函数 ---
def dispatch_active_emby_items(items):
    """Dispatch actively discovered Movie/Episode IDs through the existing flow."""
    movie_items = []
    series_items = collections.defaultdict(lambda: {"name": "", "episode_ids": set()})
    processor = extensions.media_processor_instance
    if not processor:
        return

    for item in items or []:
        item_id = str(item.get("Id") or "").strip()
        item_type = item.get("Type")
        item_name = item.get("Name") or f"ID:{item_id}"
        if not item_id:
            continue
        if not _claim_active_emby_item(item_id):
            logger.debug("  ➜ [主动入库] Item %s 已进入分派流程，跳过重复回调。", item_id)
            continue
        if item_type == "Movie":
            movie_items.append((item_id, item_name))
        elif item_type == "Episode":
            series_id = str(item.get("SeriesId") or "").strip()
            if not series_id:
                series_id = emby.get_series_id_from_child_id(
                    item_id, processor.emby_url, processor.emby_api_key,
                    processor.emby_user_id, item_name=item_name
                )
            if not series_id:
                logger.warning("  ➜ [主动入库] 分集 '%s' 未取到 SeriesId，已跳过。", item_name)
                _release_active_emby_item(item_id)
                continue
            series_items[series_id]["episode_ids"].add(item_id)
            if item.get("SeriesName"):
                series_items[series_id]["name"] = item.get("SeriesName")
        else:
            _release_active_emby_item(item_id)

    for item_id, item_name in movie_items:
        already_processed = (
            item_id in processor.processed_items_cache
            and media_db.is_emby_id_in_library(item_id)
        )
        task_prefix = "主动入库"
        logger.info(
            "  ➜ [主动入库] 已获取 Emby ID，分派 %s: %s (分集数: %s)",
            task_prefix,
            item_name,
            0,
        )
        _submit_webhook_media_task(
            f"{task_prefix}: {item_name}",
            item_id=item_id,
            force_full_update=False,
            new_episode_ids=None,
            is_new_item=not already_processed,
            aggregate_notification=False,
        )

    for series_id, info in series_items.items():
        _enqueue_active_emby_series_dispatch(
            series_id,
            info["name"],
            info["episode_ids"],
        )

def _trigger_metadata_update_task(item_id, item_name):
    """触发元数据同步任务"""
    logger.info(f"  ➜ 防抖计时器到期，开始同步《{item_name}》的元数据缓存。")
    logger.debug(f"  ➜ 元数据缓存同步对象：item_id={item_id}")
    task_manager.submit_task(
        task_sync_all_metadata,
        task_name=f"元数据同步: {item_name}",
        processor_type='media',
        item_id=item_id,
        item_name=item_name
    )


@webhook_bp.route('/api/emby/plugin-update', methods=['GET'])
def proxy_emby_plugin_update():
    """通过 ETK 的网络代理转发最新版 Emby 插件 DLL。"""
    upstream = None
    try:
        upstream = requests.get(
            'https://github.com/hbq0405/etk-mediainfo-bridge/releases/latest/download/ETKMediaInfoBridge.dll',
            headers={'User-Agent': 'EmbyToolKit-PluginUpdater'},
            stream=True,
            timeout=(15, 300),
            proxies=config_manager.get_proxies_for_requests(),
        )
        upstream.raise_for_status()
    except requests.RequestException as e:
        if upstream is not None:
            upstream.close()
        logger.error(f"  ➜ ETK 插件更新代理下载失败: {e}")
        return jsonify({'error': 'plugin_update_download_failed'}), 502

    def generate():
        try:
            for chunk in upstream.iter_content(chunk_size=128 * 1024):
                if chunk:
                    yield chunk
        finally:
            upstream.close()

    headers = {
        'Content-Disposition': 'attachment; filename="ETKMediaInfoBridge.dll"',
        'Cache-Control': 'no-store',
    }
    content_length = upstream.headers.get('Content-Length')
    if content_length:
        headers['Content-Length'] = content_length
    return Response(
        stream_with_context(generate()),
        content_type='application/octet-stream',
        headers=headers,
    )


@webhook_bp.route('/api/emby/metadata', methods=['GET'])
def get_emby_metadata_by_path():
    from database.metadata_provider_db import (
        load_emby_metadata,
        resolve_metadata_identity_by_path,
    )

    path = str(request.args.get('path') or '').strip()
    requested_type = str(request.args.get('item_type') or '').strip().title()
    if not path or requested_type not in {'Movie', 'Series', 'Season', 'Episode'}:
        return jsonify({'error': 'invalid metadata path request'}), 400

    def _number(name):
        value = request.args.get(name)
        try:
            return int(value) if value not in (None, '') else None
        except (TypeError, ValueError):
            return None

    season_number = _number('season_number')
    episode_number = _number('episode_number')
    identity = resolve_metadata_identity_by_path(
        path,
        requested_type,
        season_number=season_number,
        episode_number=episode_number,
    )
    if not identity or not identity.get('tmdb_id'):
        return jsonify({'error': 'metadata path identity not found'}), 404

    payload = load_emby_metadata(
        identity['tmdb_id'],
        identity['media_type'],
        requested_type,
        season_number=identity.get('season_number'),
        episode_number=identity.get('episode_number'),
    )
    if not payload:
        return jsonify({'error': 'metadata cache not found'}), 404
    from handler.media_image_cache import archive_metadata_images
    archive_metadata_images(payload, request.host_url, archive_sources=False)
    response = jsonify(payload)
    response.headers['Cache-Control'] = 'no-store'
    return response


@webhook_bp.route('/api/emby/collections/activate', methods=['POST'])
def activate_emby_collection():
    data = request.get_json(silent=True) or {}
    tmdb_collection_id = str(data.get('tmdb_collection_id') or '').strip()
    emby_collection_id = str(data.get('emby_collection_id') or '').strip()
    collection_name = str(data.get('name') or '').strip()
    if not tmdb_collection_id.isdigit() or not emby_collection_id:
        return jsonify({'ok': False, 'error': 'invalid collection activation request'}), 400

    activation = collections_handler.activate_collection_from_emby(
        tmdb_collection_id,
        emby_collection_id,
        collection_name,
    )
    if not activation.get('ok'):
        return jsonify({'ok': False, 'error': 'collection metadata cache not found'}), 404
    if activation.get('changed'):
        spawn(
            collections_handler.subscribe_missing_for_activated_collection,
            tmdb_collection_id,
        )
    return jsonify({'ok': True, **activation}), 200


@webhook_bp.route('/api/emby/metadata/backfill', methods=['POST'])
def trigger_emby_metadata_backfill():
    import task_manager
    from tasks.media import task_backfill_media_metadata

    submitted = task_manager.submit_task(
        task_function=task_backfill_media_metadata,
        task_name='补齐媒体元数据',
        processor_type='media',
        silent=True,
    )
    if submitted:
        return jsonify({'ok': True, 'submitted': True}), 202
    status = task_manager.get_task_status()
    if status.get('is_running') and status.get('current_action') == '补齐媒体元数据':
        return jsonify({'ok': True, 'submitted': False, 'reason': 'task_already_running'}), 200
    return jsonify({'ok': False, 'submitted': False, 'reason': 'etk_busy'}), 409


def _resolve_missing_metadata_backfill_targets(emby_item_ids):
    """Resolve Emby selections to unique root identities without contacting TMDb."""
    identities = {}
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT tmdb_id, item_type, parent_series_tmdb_id
            FROM media_metadata
            WHERE in_library IS TRUE AND emby_item_ids_json ?| %s
            """,
            (emby_item_ids,),
        )
        for row in cursor.fetchall():
            if row['item_type'] == 'Movie':
                tmdb_id, media_type = row['tmdb_id'], 'movie'
            else:
                tmdb_id, media_type = row.get('parent_series_tmdb_id') or row['tmdb_id'], 'tv'
            if tmdb_id:
                identities[(str(tmdb_id), media_type)] = {
                    'tmdb_id': str(tmdb_id),
                    'media_type': media_type,
                }
    from database.metadata_provider_db import needs_metadata_backfill
    return [
        target for target in identities.values()
        if needs_metadata_backfill(target['tmdb_id'], target['media_type'])
    ]


def _resolve_library_missing_metadata_backfill_targets(emby_library_id):
    """Resolve missing root metadata within one Emby library from local assets."""
    from database.metadata_provider_db import MEDIA_METADATA_SCHEMA_VERSION

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT root.tmdb_id, root.item_type
            FROM media_metadata root
            WHERE root.in_library IS TRUE
              AND root.item_type IN ('Movie', 'Series')
              AND (
                  root.metadata_schema_version < %s
                  OR NULLIF(BTRIM(root.title), '') IS NULL
                  OR NULLIF(BTRIM(root.overview), '') IS NULL
                  OR root.release_year IS NULL
                  OR NULLIF(BTRIM(COALESCE(
                      root.official_rating_json->>'US',
                      root.official_rating_json->>'us',
                      '')), '') IS NULL
                  OR root.genres_json IS NULL
                  OR root.genres_json = '[]'::jsonb
                  OR (
                      root.item_type = 'Series'
                      AND EXISTS (
                          SELECT 1 FROM media_metadata special
                          WHERE special.parent_series_tmdb_id = root.tmdb_id
                            AND special.item_type = 'Episode'
                            AND special.in_library IS TRUE
                            AND special.season_number = 0
                            AND special.tmdb_id LIKE root.tmdb_id || '-S0E%%'
                      )
                  )
              )
              AND (
                  EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(COALESCE(
                          root.asset_details_json, '[]'::jsonb)) asset
                      WHERE asset->>'source_library_id' = %s
                  )
                  OR (
                      root.item_type = 'Series'
                      AND EXISTS (
                          SELECT 1
                          FROM media_metadata episode
                          WHERE episode.parent_series_tmdb_id = root.tmdb_id
                            AND episode.item_type = 'Episode'
                            AND episode.in_library IS TRUE
                            AND EXISTS (
                                SELECT 1
                                FROM jsonb_array_elements(COALESCE(
                                    episode.asset_details_json, '[]'::jsonb)) asset
                                WHERE asset->>'source_library_id' = %s
                            )
                      )
                  )
              )
            """,
            (
                MEDIA_METADATA_SCHEMA_VERSION,
                str(emby_library_id),
                str(emby_library_id),
            ),
        )
        return [
            {
                'tmdb_id': str(row['tmdb_id']),
                'media_type': 'tv' if row['item_type'] == 'Series' else 'movie',
            }
            for row in cursor.fetchall()
            if row.get('tmdb_id')
        ]


def _trigger_emby_metadata_backfill(emby_item_ids):
    targets = _resolve_missing_metadata_backfill_targets(emby_item_ids)
    if not targets:
        return jsonify({'ok': True, 'submitted': False, 'reason': 'metadata_complete'}), 200

    from tasks.media import task_backfill_media_metadata_batch
    submitted = task_manager.submit_task(
        task_function=task_backfill_media_metadata_batch,
        task_name=f"补齐缺失元数据 ({len(targets)} 项)",
        processor_type='media',
        silent=True,
        targets=targets,
    )
    if submitted:
        return jsonify({'ok': True, 'submitted': True, 'count': len(targets)}), 202
    return jsonify({'ok': False, 'submitted': False, 'reason': 'etk_busy'}), 409


@webhook_bp.route('/api/emby/metadata/backfill/item', methods=['POST'])
def trigger_emby_item_metadata_backfill():
    emby_item_id = str((request.get_json(silent=True) or {}).get('emby_item_id') or '').strip()
    if not emby_item_id:
        return jsonify({'ok': False, 'error': 'emby_item_id is required'}), 400
    return _trigger_emby_metadata_backfill([emby_item_id])


@webhook_bp.route('/api/emby/metadata/backfill/items', methods=['POST'])
def trigger_emby_items_metadata_backfill():
    values = (request.get_json(silent=True) or {}).get('emby_item_ids') or []
    item_ids = list(dict.fromkeys(
        str(value).strip() for value in values if str(value or '').strip()
    ))
    if not item_ids:
        return jsonify({'ok': False, 'error': 'emby_item_ids is required'}), 400
    return _trigger_emby_metadata_backfill(item_ids)


@webhook_bp.route('/api/emby/metadata/backfill/library', methods=['POST'])
def trigger_emby_library_metadata_backfill():
    data = request.get_json(silent=True) or {}
    library_id = str(data.get('emby_library_id') or '').strip()
    refresh_mode = str(data.get('refresh_mode') or '').strip().lower()
    if not library_id.isdigit():
        return jsonify({'ok': False, 'error': 'emby_library_id is required'}), 400
    if refresh_mode != 'missing_metadata':
        return jsonify({'ok': False, 'error': 'unsupported refresh_mode'}), 400

    targets = _resolve_library_missing_metadata_backfill_targets(library_id)
    if not targets:
        return jsonify({'ok': True, 'submitted': False, 'reason': 'metadata_complete'}), 200

    from tasks.media import task_backfill_media_metadata_batch
    submitted = task_manager.submit_task(
        task_function=task_backfill_media_metadata_batch,
        task_name=f"补齐缺失元数据 (媒体库 {library_id}，{len(targets)} 项)",
        processor_type='media',
        silent=True,
        targets=targets,
    )
    if submitted:
        return jsonify({'ok': True, 'submitted': True, 'count': len(targets)}), 202
    return jsonify({'ok': False, 'submitted': False, 'reason': 'etk_busy'}), 409


@webhook_bp.route('/api/emby/intro-detection/backfill', methods=['POST'])
def trigger_emby_intro_detection_backfill():
    """Queue the opt-in active-watch intro fallback from the bridge task."""
    from handler.intro_detection_service import enqueue_active_backfill

    result = enqueue_active_backfill()
    if result.get('ok'):
        return jsonify(result), 202 if result.get('queued') else 200
    return jsonify(result), 503 if result.get('reason') == 'service_unavailable' else 200


@webhook_bp.route('/api/emby/metadata/images/refresh', methods=['POST'])
def refresh_emby_metadata_images():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from database.metadata_provider_db import (
        build_image_language_parameter,
        build_image_language_priority,
        replace_cached_image_paths,
        resolve_metadata_identity_by_path,
        select_image_path,
    )

    path = str(request.args.get('path') or '').strip()
    requested_tmdb_id = str(request.args.get('tmdb_id') or '').strip()
    requested_type = str(request.args.get('item_type') or '').strip().title()
    if requested_type not in {'Movie', 'Series', 'Season', 'Episode', 'Boxset'}:
        return jsonify({'ok': False, 'error': 'invalid image refresh request'}), 400
    if requested_type == 'Boxset':
        if not requested_tmdb_id.isdigit():
            return jsonify({'ok': False, 'error': 'invalid collection tmdb id'}), 400
    elif not path:
        return jsonify({'ok': False, 'error': 'invalid image refresh request'}), 400

    def _number(name):
        value = request.args.get(name)
        try:
            return int(value) if value not in (None, '') else None
        except (TypeError, ValueError):
            return None

    season_number = _number('season_number')
    episode_number = _number('episode_number')
    if requested_type == 'Boxset':
        tmdb_id = requested_tmdb_id
        media_type = 'movie'
    else:
        identity = resolve_metadata_identity_by_path(
            path,
            requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        if not identity or not identity.get('tmdb_id'):
            return jsonify({'ok': False, 'error': 'metadata path identity not found'}), 404

        tmdb_id = str(identity['tmdb_id'])
        media_type = str(identity['media_type'])
        season_number = identity.get('season_number')
        episode_number = identity.get('episode_number')
    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    if not api_key:
        return jsonify({'ok': False, 'error': 'tmdb api key is not configured'}), 503

    try:
        if requested_type == 'Boxset':
            from database import tmdb_collection_db

            details = tmdb.get_collection_details(int(tmdb_id), api_key, skip_fallback=True)
            if not details:
                return jsonify({'ok': False, 'error': 'tmdb collection image lookup failed'}), 502
            updated = tmdb_collection_db.update_native_collection_images(
                tmdb_id,
                details.get('poster_path'),
                details.get('backdrop_path'),
            )
            logger.info(
                "  ➤[图片替换] 已按 %s 优先级重选 BoxSet TMDb:%s 的图片，更新 %s 条缓存。",
                '原语言' if config_manager.APP_CONFIG.get(
                    constants.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE, 'zh'
                ) == 'original' else '简体中文',
                tmdb_id,
                updated,
            )
            return jsonify({'ok': True, 'updated': updated}), 200

        if media_type == 'movie':
            base = tmdb.get_movie_details(int(tmdb_id), api_key, append_to_response='')
        else:
            base = tmdb.get_tv_details(
                int(tmdb_id), api_key, append_to_response='', allow_english_fallback=False
            )
        if not base:
            return jsonify({'ok': False, 'error': 'tmdb media identity lookup failed'}), 502

        preference = config_manager.APP_CONFIG.get(
            constants.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE, 'zh'
        )
        priorities = build_image_language_priority(base.get('original_language'), preference)
        image_languages = build_image_language_parameter(priorities)

        root_images = None
        season_posters = None
        episode_still = None
        if requested_type in {'Movie', 'Series'}:
            if media_type == 'movie':
                image_data = tmdb.get_movie_details(
                    int(tmdb_id), api_key, append_to_response='images',
                    include_image_language=image_languages,
                )
            else:
                image_data = tmdb.get_tv_details(
                    int(tmdb_id), api_key, append_to_response='images',
                    include_image_language=image_languages,
                    allow_english_fallback=False,
                )
            if not image_data:
                return jsonify({'ok': False, 'error': 'tmdb image lookup failed'}), 502
            images = image_data.get('images') or {}
            backdrop = select_image_path(images.get('backdrops'), priorities) or image_data.get('backdrop_path')
            root_images = {
                'poster': select_image_path(images.get('posters'), priorities) or image_data.get('poster_path'),
                'backdrop': backdrop,
                'logo': select_image_path(images.get('logos'), priorities),
                'thumb': backdrop,
            }

            if requested_type == 'Series':
                seasons = [
                    item for item in image_data.get('seasons') or []
                    if item.get('season_number') is not None
                ]
                season_posters = {int(item['season_number']): None for item in seasons}

                def _fetch_season_poster(number):
                    details = tmdb.get_season_details_tmdb(
                        int(tmdb_id), number, api_key,
                        append_to_response='images',
                        include_image_language=image_languages,
                    )
                    if not details:
                        return number, None
                    return number, (
                        select_image_path((details.get('images') or {}).get('posters'), priorities)
                        or details.get('poster_path')
                    )

                with ThreadPoolExecutor(max_workers=min(4, max(1, len(seasons)))) as executor:
                    futures = [
                        executor.submit(_fetch_season_poster, int(item['season_number']))
                        for item in seasons
                    ]
                    for future in as_completed(futures):
                        number, poster_path = future.result()
                        season_posters[number] = poster_path
        elif requested_type == 'Season':
            if season_number is None:
                return jsonify({'ok': False, 'error': 'season number is required'}), 400
            details = tmdb.get_season_details_tmdb(
                int(tmdb_id), int(season_number), api_key,
                append_to_response='images',
                include_image_language=image_languages,
            )
            if not details:
                return jsonify({'ok': False, 'error': 'tmdb season image lookup failed'}), 502
            season_posters = {
                int(season_number): (
                    select_image_path((details.get('images') or {}).get('posters'), priorities)
                    or details.get('poster_path')
                )
            }
        else:
            if season_number is None or episode_number is None:
                return jsonify({'ok': False, 'error': 'episode numbers are required'}), 400
            details = tmdb.get_episode_details_tmdb(
                int(tmdb_id), int(season_number), int(episode_number), api_key,
                append_to_response='images',
                include_image_language=image_languages,
            )
            if not details:
                return jsonify({'ok': False, 'error': 'tmdb episode image lookup failed'}), 502
            episode_still = (
                select_image_path((details.get('images') or {}).get('stills'), priorities)
                or details.get('still_path')
            )

        updated = replace_cached_image_paths(
            tmdb_id,
            media_type,
            requested_type,
            root_images=root_images,
            season_number=season_number,
            season_posters=season_posters,
            episode_number=episode_number,
            episode_still=episode_still,
        )
        logger.info(
            "  ➜ [图片替换] 已按 %s 优先级重选 %s TMDb:%s 的图片，更新 %s 条缓存。",
            '原语言' if preference == 'original' else '简体中文', requested_type, tmdb_id, updated,
        )
        return jsonify({'ok': True, 'updated': updated}), 200
    except Exception as exc:
        logger.error("  ➜ [图片替换] 重选 TMDb 图片失败: %s", exc, exc_info=True)
        return jsonify({'ok': False, 'error': str(exc)}), 500


@webhook_bp.route('/api/emby/metadata/images/search', methods=['GET'])
def search_emby_metadata_images():
    from database.metadata_provider_db import (
        resolve_metadata_identity_by_path,
    )
    from handler.media_image_policy import (
        ImagePolicySyncError,
        get_live_image_candidates,
    )

    requested_type = str(request.args.get('item_type') or '').strip().title()
    path = str(request.args.get('path') or '').strip()
    tmdb_id = str(request.args.get('tmdb_id') or '').strip()
    if requested_type not in {'Movie', 'Series', 'Season', 'Episode', 'Boxset'}:
        return jsonify({'error': 'invalid image search request'}), 400

    def _number(name):
        value = request.args.get(name)
        try:
            return int(value) if value not in (None, '') else None
        except (TypeError, ValueError):
            return None

    season_number = _number('season_number')
    episode_number = _number('episode_number')
    api_key = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TMDB_API_KEY)
    if not api_key:
        return jsonify({'error': 'tmdb api key is not configured'}), 503
    if requested_type == 'Boxset':
        if not tmdb_id.isdigit():
            return jsonify({'error': 'invalid collection tmdb id'}), 400
        media_type = 'movie'
    else:
        identity = resolve_metadata_identity_by_path(
            path,
            requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        if not identity or not identity.get('tmdb_id'):
            return jsonify({'error': 'metadata path identity not found'}), 404
        tmdb_id = str(identity['tmdb_id'])
        media_type = str(identity['media_type'])
        season_number = identity.get('season_number')
        episode_number = identity.get('episode_number')
    include_all_languages = str(
        request.args.get('include_all_languages') or ''
    ).strip().lower() in {'1', 'true', 'yes', 'on'}
    try:
        candidates = get_live_image_candidates(
            requested_type,
            tmdb_id,
            media_type,
            season_number=season_number,
            episode_number=episode_number,
            include_all_languages=include_all_languages,
        )
    except ImagePolicySyncError as exc:
        return jsonify({'error': str(exc)}), 502

    response = jsonify({'images': candidates})
    response.headers['Cache-Control'] = 'no-store'
    return response


@webhook_bp.route('/api/emby/metadata/images/sync', methods=['POST'])
def sync_emby_metadata_images():
    from database.metadata_provider_db import resolve_metadata_identity_by_path
    from handler.media_image_policy import (
        ImagePolicyCollectionNotFound,
        ImagePolicySyncError,
        sync_image_policy,
    )

    requested_type = str(request.args.get('item_type') or '').strip().title()
    path = str(request.args.get('path') or '').strip()
    tmdb_id = str(request.args.get('tmdb_id') or '').strip()
    if requested_type not in {'Movie', 'Series', 'Season', 'Episode', 'Boxset'}:
        return jsonify({'error': 'invalid image sync request'}), 400

    def _number(name):
        value = request.args.get(name)
        try:
            return int(value) if value not in (None, '') else None
        except (TypeError, ValueError):
            return None

    season_number = _number('season_number')
    episode_number = _number('episode_number')
    if requested_type == 'Boxset':
        if not tmdb_id.isdigit():
            return jsonify({'error': 'invalid collection tmdb id'}), 400
        media_type = 'movie'
    else:
        identity = resolve_metadata_identity_by_path(
            path,
            requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        if not identity or not identity.get('tmdb_id'):
            return jsonify({'error': 'metadata path identity not found'}), 404
        tmdb_id = str(identity['tmdb_id'])
        media_type = str(identity['media_type'])
        season_number = identity.get('season_number')
        episode_number = identity.get('episode_number')

    data = request.get_json(silent=True) or {}
    try:
        images = sync_image_policy(
            requested_type,
            tmdb_id,
            media_type,
            data.get('rules') or [],
            request.host_url,
            season_number=season_number,
            episode_number=episode_number,
            force=bool(data.get('force')),
        )
    except ImagePolicyCollectionNotFound:
        from database import tmdb_collection_db
        from routes.tmdb_collections import _schedule_missing_collection_cleanup

        row = tmdb_collection_db.get_native_collection_by_tmdb_id(tmdb_id)
        _schedule_missing_collection_cleanup(tmdb_id, row)
        return jsonify({'error': 'tmdb collection no longer exists'}), 410
    except ImagePolicySyncError as exc:
        return jsonify({'error': str(exc)}), 502
    response = jsonify({'ok': True, 'images': images})
    response.headers['Cache-Control'] = 'no-store'
    return response


# --- 外部事件路由：MoviePilot Webhook / ETK Emby 插件事件 ---
@webhook_bp.route('/webhook/emby', methods=['POST'])
@webhook_bp.route('/api/emby/events', methods=['POST'])
@extensions.processor_ready_required
def emby_webhook():
    data = request.get_json(silent=True) or {}
    event_type = data.get("Event") # Emby
    mp_event_type = data.get("type") # MP
    is_plugin_endpoint = request.path == '/api/emby/events'
    if is_plugin_endpoint:
        if data.get('_etk_source') != 'ETKMediaInfoBridge' or not event_type:
            return jsonify({"status": "invalid_plugin_event"}), 400
    elif event_type:
        logger.info("  ➜ 已忽略旧 Emby Webhook 事件；请使用 ETK MediaInfo Bridge 插件。")
        return jsonify({"status": "emby_webhook_removed"}), 200
    # ======================================================================
    # ★★★ 处理 MoviePilot 订阅助手事件 ★★★
    # ======================================================================
    if mp_event_type in {"download.added", "subscribe.added", "subscribe.modified", "subscribe.deleted", "subscribe.complete"}:
        try:
            handled = SubscribeAssistantManager(config_manager.APP_CONFIG).handle_moviepilot_event(mp_event_type, data)
            return jsonify({"status": "subscribe_assistant_processed" if handled else "subscribe_assistant_ignored"}), 200
        except Exception as e:
            logger.error(f"  ➜ [订阅助手] MP Webhook 事件处理失败: {mp_event_type} -> {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500

    # ======================================================================
    # ★★★ 处理 MoviePilot transfer.complete 事件 ★★★
    # ======================================================================
    if mp_event_type in ["transfer.complete", "transfer.subtitle.complete"]:
        try:
            transfer_info = data.get("data", {}).get("transferinfo", {})
            media_info = data.get("data", {}).get("mediainfo", {})
            meta_info = data.get("data", {}).get("meta", {}) 
            
            target_item = transfer_info.get("target_item", {})
            target_dir = transfer_info.get("target_diritem", {})
            source_item = transfer_info.get("fileitem") or data.get("data", {}).get("fileitem") or {}
            
            file_id = target_item.get("fileid")
            file_name = target_item.get("name")
            file_type = target_item.get("type") 
            pickcode = target_item.get("pickcode")
            dir_cid = target_dir.get("fileid")

            tmdb_id = media_info.get("tmdb_id")
            media_type_cn = media_info.get("type") 
            title = media_info.get("title")
            
            begin_season = meta_info.get("begin_season")
            begin_episode = meta_info.get("begin_episode")
            
            if not tmdb_id or not file_id:
                logger.warning("  ➜ MP 通知缺少 tmdb_id 或 file_id，无法处理。")
                return jsonify({"status": "ignored_missing_data"}), 200

            media_type = 'tv' if media_type_cn == '电视剧' else 'movie'
            
            if file_type == 'file':
                file_info = {
                    'file_id': file_id,
                    'name': file_name,
                    'parent_id': dir_cid,
                    'pickcode': pickcode,
                    'tmdb_id': tmdb_id,
                    'media_type': media_type,
                    'title': title,
                    'season_num': begin_season,
                    'episode_num': begin_episode,
                    'size': None,
                    'fs': None,
                    '115_path': target_item.get("path") # ★ 核心新增：直接提取 115 物理路径
                }
                
                log_prefix = "MP字幕上传" if mp_event_type == "transfer.subtitle.complete" else "MP视频上传"
                
                config = get_config()
                mp_classify_enabled = bool(config.get(constants.CONFIG_OPTION_115_MP_CLASSIFY, False))
                
                if mp_classify_enabled:
                    logger.info(f"  ➜ [{log_prefix}] 收到文件：{file_name}。MP 直出已开启，直接处理。")
                    spawn(_process_mp_passthrough_immediate, file_info)
                    return jsonify({"status": "processing_single_file_passthrough"}), 200
                else:
                    logger.info(f"  ➜ [{log_prefix}] 收到文件：{file_name}。已加入合并缓冲池，等待同集字幕或其他版本。")
                    _enqueue_mp_file(file_info)
                    return jsonify({"status": "processing_single_file"}), 200
            else:
                logger.debug(f"  ➜ [MP上传] 忽略非文件类型的通知: {file_name}")
                return jsonify({"status": "ignored_not_file"}), 200

        except Exception as e:
            logger.error(f"  ➜ [MP上传] 处理失败: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500
        
    logger.debug(f"  ➜ 收到 Emby 插件事件: {event_type}")

    USER_DATA_EVENTS = [
        "item.markfavorite", "item.unmarkfavorite",
        "item.markplayed", "item.markunplayed",
        "playback.start", "playback.pause", "playback.stop",
        "playback.halfway", "item.rate"
    ]

    if event_type == "user.policyupdated":
        updated_user = data.get("User", {})
        updated_user_id = updated_user.get("Id")
        updated_user_name = updated_user.get("Name", "未知用户")
        
        if not updated_user_id:
            return jsonify({"status": "event_ignored_no_user_id"}), 200

        # --- 立即反查并更新本地 Policy ---
        try:
            def _update_local_policy_task():
                try:
                    # 获取最新详情
                    user_details = emby.get_user_details(
                        updated_user_id, 
                        config_manager.APP_CONFIG.get("emby_server_url"), 
                        config_manager.APP_CONFIG.get("emby_api_key")
                    )
                    if user_details and 'Policy' in user_details:
                        # 更新数据库
                        user_db.upsert_emby_users_batch([user_details])
                        logger.info(f"  ➜ Emby事件: 已更新用户 {updated_user_id} 的本地权限缓存。")
                except Exception as e:
                    logger.error(f"  ➜ Emby事件更新本地 Policy 失败: {e}")

            # 异步执行，不阻塞 Emby 事件响应
            spawn(_update_local_policy_task)
        except Exception as e:
            logger.error(f"启动 Policy 更新任务失败: {e}")

        # ★★★ 核心逻辑: 在处理前，先检查信号旗 ★★★
        with SYSTEM_UPDATE_LOCK:
            last_update_time = SYSTEM_UPDATE_MARKERS.get(updated_user_id)
            # 如果找到了标记，并且时间戳在我们的抑制窗口期内
            if last_update_time and (time.time() - last_update_time) < RECURSION_SUPPRESSION_WINDOW:
                logger.debug(f"  ➜ 忽略由系统内部同步触发的用户 '{updated_user_name}' 权限更新事件。")
                # 为了保险起见，用完就删掉这个标记
                del SYSTEM_UPDATE_MARKERS[updated_user_id]
                # 直接返回成功，不再创建任何后台任务
                return jsonify({"status": "event_ignored_system_triggered"}), 200
        
        # 如果上面的检查通过了（即这是一个正常的手动操作），才继续执行原来的逻辑
        logger.info(f"  ➜ 检测到用户 '{updated_user_name}' 的权限策略已更新，将分派后台任务检查模板同步。")
        task_manager.submit_task(
            task_auto_sync_template_on_policy_change,
            task_name=f"自动同步权限 (源: {updated_user_name})",
            processor_type='media',
            updated_user_id=updated_user_id
        )
        return jsonify({"status": "auto_sync_task_submitted"}), 202

    if event_type in USER_DATA_EVENTS:
        user_from_webhook = data.get("User", {})
        user_id = user_from_webhook.get("Id")
        user_name = user_from_webhook.get("Name")
        user_name_for_log = user_name or user_id
        item_from_webhook = data.get("Item", {})
        item_id_from_webhook = item_from_webhook.get("Id")
        item_type_from_webhook = item_from_webhook.get("Type")

        if not user_id or not item_id_from_webhook:
            return jsonify({"status": "event_ignored_missing_data"}), 200

        user_data_from_event = data.get("UserData") or item_from_webhook.get("UserData") or {}
        exact_favorite_handled = False
        if event_type in ["item.markfavorite", "item.unmarkfavorite", "item.rate"]:
            favorite_value = None
            if event_type == "item.markfavorite":
                favorite_value = True
            elif event_type == "item.unmarkfavorite":
                favorite_value = False
            elif isinstance(user_data_from_event, dict) and 'IsFavorite' in user_data_from_event:
                favorite_value = bool(user_data_from_event.get('IsFavorite'))
            if favorite_value is not None and item_type_from_webhook in ['Series', 'Season', 'Episode']:
                user_db.set_emby_favorite_item(user_id, item_id_from_webhook, bool(favorite_value))
                exact_favorite_handled = True
                if favorite_value:
                    try:
                        from handler import intro_detection_service
                        spawn(intro_detection_service.enqueue_favorite_item, dict(item_from_webhook))
                    except Exception as e:
                        logger.debug(f"  ➜ [片头声纹提取] 分派收藏触发失败: {e}")

        if event_type == "playback.halfway":
            if item_type_from_webhook != 'Episode':
                return jsonify({"status": "event_ignored_non_episode_halfway"}), 200
            playback_info = data.get("PlaybackInfo") or {}
            try:
                pos = float(
                    playback_info.get('PositionTicks')
                    or playback_info.get('PlaybackPositionTicks')
                    or data.get('PositionTicks')
                    or 0
                )
                runtime = float(
                    playback_info.get('RunTimeTicks')
                    or item_from_webhook.get('RunTimeTicks')
                    or 0
                )
            except Exception:
                pos = runtime = 0
            if runtime > 0 and pos / runtime < 0.5:
                return jsonify({"status": "event_ignored_before_halfway"}), 200
            try:
                from handler import intro_detection_service
                spawn(intro_detection_service.enqueue_playback_halfway, dict(item_from_webhook))
            except Exception as e:
                logger.debug(f"  ➜ [片头声纹提取] 分派播放过半触发失败: {e}")


        id_to_update_in_db = None
        if item_type_from_webhook in ['Movie', 'Series']:
            id_to_update_in_db = item_id_from_webhook
        elif item_type_from_webhook == 'Episode':
            series_id = emby.get_series_id_from_child_id(
                item_id=item_id_from_webhook,
                base_url=config_manager.APP_CONFIG.get("emby_server_url"),
                api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                user_id=user_id
            )
            if series_id:
                id_to_update_in_db = series_id
        
        if not id_to_update_in_db:
            if exact_favorite_handled:
                return jsonify({"status": "exact_favorite_updated"}), 200
            return jsonify({"status": "event_ignored_unsupported_type_or_not_found"}), 200

        update_data = {"user_id": user_id, "item_id": id_to_update_in_db}
        
        if event_type in ["item.markfavorite", "item.unmarkfavorite", "item.markplayed", "item.markunplayed", "item.rate"]:
            user_data_from_item = user_data_from_event
            if 'IsFavorite' in user_data_from_item:
                update_data['is_favorite'] = user_data_from_item['IsFavorite']
            if 'Played' in user_data_from_item:
                update_data['played'] = user_data_from_item['Played']
                if user_data_from_item['Played']:
                    update_data['playback_position_ticks'] = 0
                    update_data['last_played_date'] = datetime.now(timezone.utc)

        elif event_type in ["playback.start", "playback.pause", "playback.stop", "playback.halfway"]:
            playback_info = data.get("PlaybackInfo", {})
            if playback_info:
                position_ticks = playback_info.get('PositionTicks')
                if position_ticks is not None:
                    update_data['playback_position_ticks'] = position_ticks
                
                update_data['last_played_date'] = datetime.now(timezone.utc)
                
                if event_type == "playback.stop":
                    if playback_info.get('PlayedToCompletion') is True:
                        update_data['played'] = True
                        update_data['playback_position_ticks'] = 0
                    else:
                        update_data['played'] = False
                    try:
                        cleanup_data = dict(data)
                        cleanup_data['_etk_webhook_remote_addr'] = request.remote_addr or ''
                        spawn(cleanup_for_playback_stop, cleanup_data)
                        spawn(_maybe_promote_virtual_import_from_playback, cleanup_data)
                        try:
                            from reverse_proxy import clear_play_concurrency_for_playback_stop
                            spawn(clear_play_concurrency_for_playback_stop, cleanup_data)
                        except Exception as e:
                            logger.debug(f"  ➜ [并发控制] 播放停止清理任务分配失败: {e}")
                    except Exception as e:
                        logger.error(f"  ➜ [复制播放] 停止播放清理任务分配失败: {e}")

            # 发送有灵魂的图文播放通知 
            notify_types = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES, constants.DEFAULT_TELEGRAM_NOTIFY_TYPES)
            if 'playback' in notify_types and event_type in ["playback.start", "playback.pause", "playback.stop"]:
                try:
                    # 使用 spawn 异步丢给后台处理，避免网络波动阻塞 Emby 事件响应
                    spawn(telegram.send_playback_notification, data)
                except Exception as e:
                    logger.error(f"  ➜ 发送播放通知任务分配失败: {e}")

        try:
            if len(update_data) > 2:
                user_db.upsert_user_media_data(update_data)
                user_db.upsert_user_media_data(update_data)
                item_name_for_log = f"ID:{id_to_update_in_db}"
                try:
                    # 为了日志，只请求 Name 字段，提高效率
                    item_details_for_log = emby.get_emby_item_details(
                        item_id=id_to_update_in_db,
                        emby_server_url=config_manager.APP_CONFIG.get("emby_server_url"),
                        emby_api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                        user_id=user_id,
                        fields="Name"
                    )
                    if item_details_for_log and item_details_for_log.get("Name"):
                        item_name_for_log = item_details_for_log.get("Name")
                except Exception:
                    # 如果获取失败，不影响主流程，日志中继续使用ID
                    pass
                logger.trace(f"  ➜ Emby事件: 已更新用户 '{user_name_for_log}' 对项目 '{item_name_for_log}' 的状态 ({event_type})。")
                return jsonify({"status": "user_data_updated"}), 200
            else:
                logger.debug(f"  ➜ Emby事件 '{event_type}' 未包含可更新的用户数据，已忽略。")
                return jsonify({"status": "event_ignored_no_updatable_data"}), 200
        except Exception as e:
            logger.error(f"  ➜ 通过 Emby 事件更新用户媒体数据时失败: {e}", exc_info=True)
            return jsonify({"status": "error_updating_user_data"}), 500

    trigger_events = ["metadata.update", "image.update", "collection.items.removed"]
    if event_type not in trigger_events:
        logger.debug(f"  ➜ Emby事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
        return jsonify({"status": "event_ignored_not_in_trigger_list"}), 200
    
    item_from_webhook = data.get("Item", {}) if data else {}
    original_item_id = item_from_webhook.get("Id")
    original_item_type = item_from_webhook.get("Type")
    original_item_name = item_from_webhook.get("Name", "未知项目")
    original_item_path = item_from_webhook.get("Path")
    
    # 如果是分集，将名字格式化为 "剧名 - 集名"，方便日志搜索
    raw_name = item_from_webhook.get("Name", "未知项目")
    series_name = item_from_webhook.get("SeriesName")
    
    if original_item_type == "Episode" and series_name:
        original_item_name = f"{series_name} - {raw_name}"
    else:
        original_item_name = raw_name
    
    trigger_types = ["Movie", "Series", "Season", "Episode", "BoxSet"]
    if not (original_item_id and original_item_type in trigger_types):
        logger.debug(f"  ➜ Emby事件 '{event_type}' (项目: {original_item_name}, 类型: {original_item_type}) 被忽略。")
        return jsonify({"status": "event_ignored_no_id_or_wrong_type"}), 200

    # ======================================================================
    # ★★★ 处理 collection.items.removed (检查是否变空消失) ★★★
    # ======================================================================
    if event_type == "collection.items.removed":
        # Emby 发送此事件时，Item 指的是合集本身
        collection_id = item_from_webhook.get("Id")
        collection_name = item_from_webhook.get("Name")

        if collection_id in DELETING_COLLECTIONS:
            logger.debug(f"  ➜ Emby事件: 忽略合集 '{collection_name}' 的移除通知 (正在执行手动删除)。")
            return jsonify({"status": "ignored_manual_deletion"}), 200
        
        if collection_id:
            logger.info(f"  ➜ Emby事件: 合集 '{collection_name}' 有成员移除，正在检查合集存活状态...")
            
            def _check_collection_survival_task(processor=None):
                details = emby.get_emby_item_details(
                    item_id=collection_id,
                    emby_server_url=config_manager.APP_CONFIG.get("emby_server_url"),
                    emby_api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                    user_id=config_manager.APP_CONFIG.get("emby_user_id"),
                    fields="Id",
                    silent_404=True
                )
                
                if not details:
                    logger.info(f"  ➜ 合集《{collection_name}》已在 Emby 中消失，正在取消激活本地记录。")
                    logger.debug(f"  ➜ 已消失合集 ID：{collection_id}")
                    tmdb_collection_db.deactivate_native_collection_by_emby_id(collection_id)
                else:
                    logger.debug(f"  ➜ 合集 '{collection_name}' 依然存在，无需操作。")

            task_manager.submit_task(
                _check_collection_survival_task,
                task_name=f"检查合集存活: {collection_name}",
                processor_type='media'
            )
            return jsonify({"status": "collection_removal_check_started"}), 202

    # 过滤不在处理范围的媒体库
    if event_type in ["metadata.update", "image.update"]:
        processor = extensions.media_processor_instance
        
        # --- 【拦截 1】如果是系统正在生成的封面，直接拦截，不查库，不报错 ---
        if event_type == "image.update" and original_item_id in UPDATING_IMAGES:
            logger.debug(f"  ➜ Emby事件: 忽略项目 '{original_item_name}' 的图片更新通知 (系统生成的封面)。")
            return jsonify({"status": "ignored_self_triggered_update"}), 200
        
        # --- 【拦截 2】如果是系统正在更新元数据，直接拦截 ---
        if event_type == "metadata.update" and original_item_id in UPDATING_METADATA:
            logger.debug(f"  ➜ Emby事件: 忽略项目 '{original_item_name}' 的元数据更新通知 (系统触发的更新)。")
            return jsonify({"status": "ignored_self_triggered_metadata_update"}), 200

        # --- 【拦截 3】如果是合集(BoxSet)，它没有物理路径，直接跳过库路径检查 ---
        if original_item_type == "BoxSet":
            logger.trace(f"  ➜ Emby事件: 项目 '{original_item_name}' 是合集类型，跳过媒体库路径检查。")
            library_info = None 
        else:
            # 正常的媒体项，才去获取所属库信息
            library_info = emby.get_library_root_for_item(
                original_item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id, 
                item_path=original_item_path
            )
        
        if library_info:
            lib_id = library_info.get("Id")
            lib_name = library_info.get("Name", "未知库")
            allowed_libs = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS) or []

            # 【关键拦截点】
            if lib_id not in allowed_libs:
                logger.trace(f"  ➜ Emby事件: 项目 '{original_item_name}' 所属库 '{lib_name}' (ID: {lib_id}) 不在处理范围内，已跳过。")
                return jsonify({"status": "ignored_library"}), 200

        if _should_skip_non_etk_strm_webhook(original_item_type, original_item_name, original_item_path):
            return jsonify({"status": "ignored_non_etk_strm"}), 200

    # --- 为 元数据更新 事件准备变量 ---
    id_to_process = original_item_id
    name_for_task = original_item_name
    
    if original_item_type == "Episode":
        series_id = emby.get_series_id_from_child_id(
            original_item_id, extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=original_item_name
        )
        if not series_id:
            logger.warning(f"  ➜ Emby事件 '{event_type}': 剧集 '{original_item_name}' 未找到所属剧集，跳过。")
            return jsonify({"status": "event_ignored_episode_no_series_id"}), 200
        id_to_process = series_id
        
        full_series_details = emby.get_emby_item_details(
            item_id=id_to_process, emby_server_url=extensions.media_processor_instance.emby_url,
            emby_api_key=extensions.media_processor_instance.emby_api_key, user_id=extensions.media_processor_instance.emby_user_id
        )
        if full_series_details:
            name_for_task = full_series_details.get("Name", f"未知剧集(ID:{id_to_process})")

    # --- 处理元数据更新事件 ---
    if event_type == "metadata.update":
        with UPDATE_DEBOUNCE_LOCK:
            if id_to_process in UPDATE_DEBOUNCE_TIMERS:
                old_timer = UPDATE_DEBOUNCE_TIMERS[id_to_process]
                old_timer.kill()
                logger.debug(f"  ➜ 已为 '{name_for_task}' 取消了旧的同步计时器，将以最新的元数据更新事件为准。")

            logger.info(f"  ➜ 为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的元数据同步延迟，以合并连续的更新事件。")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_metadata_update_task,
                item_id=id_to_process,
                item_name=name_for_task
            )
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer
        return jsonify({"status": "metadata_update_task_debounced", "item_id": id_to_process}), 202

    if event_type == "image.update":
        from urllib.parse import parse_qs, unquote, urlparse
        from database.metadata_provider_db import (
            resolve_metadata_identity_by_path,
            update_cached_image_path,
        )

        image = data.get("Image") if isinstance(data.get("Image"), dict) else {}
        image_type = str(image.get("Type") or "").strip().split("/", 1)[0].title()
        image_url = str(image.get("Url") or "").strip()
        if not image_type:
            return jsonify({"status": "image_update_observed", "item_id": id_to_process}), 200

        def _number(value):
            try:
                return int(value) if value not in (None, "") else None
            except (TypeError, ValueError):
                return None

        season_number = _number(item_from_webhook.get("IndexNumber")) if original_item_type == "Season" else (
            _number(item_from_webhook.get("ParentIndexNumber"))
        )
        episode_number = _number(item_from_webhook.get("IndexNumber")) if original_item_type == "Episode" else None
        identity = resolve_metadata_identity_by_path(
            original_item_path,
            original_item_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        if not identity or not identity.get("tmdb_id"):
            logger.warning("  ➜ [图片更新] 无法定位 '%s' 的 ETK 元数据记录。", original_item_name)
            return jsonify({"status": "image_update_identity_not_found", "item_id": original_item_id}), 200

        image_path = None
        source_url = image_url
        if image_url:
            parsed = urlparse(image_url)
            proxied_url = parse_qs(parsed.query).get("url", [None])[0]
            if proxied_url:
                parsed = urlparse(unquote(proxied_url))
            marker = "/t/p/"
            marker_index = parsed.path.find(marker)
            if marker_index >= 0:
                image_path = "/" + parsed.path[marker_index + len(marker):].split("/", 1)[-1].lstrip("/")
                source_url = f"https://image.tmdb.org/t/p/original/{image_path.lstrip('/')}"

        content_hash = ""
        if image_path:
            metadata_image_value = image_path
        else:
            current_image = emby.get_emby_item_image_bytes(
                original_item_id,
                image_type,
                processor.emby_url,
                processor.emby_api_key,
            )
            if not current_image:
                logger.warning(
                    "  ➜ [图片更新] 无法读取 '%s' 的当前 Emby %s 图片，未写入 ETK 缓存。",
                    original_item_name, image_type,
                )
                return jsonify({"status": "image_update_observed", "item_id": original_item_id}), 200
            image_bytes, mime_type = current_image
            if not source_url:
                source_url = (
                    f"emby-manual-image://{original_item_type}/{identity['tmdb_id']}/"
                    f"{identity.get('season_number') or 0}/{identity.get('episode_number') or 0}/"
                    f"{image_type}/{original_item_id}/{int(time.time())}"
                )
            from handler.media_image_cache import cache_image_bytes, cache_token
            cached = cache_image_bytes(source_url, image_bytes, mime_type, force=True)
            if not cached or not cached.get("content_hash"):
                logger.warning(
                    "  ➜ [图片更新] '%s' 的 %s 图片写入 ETK 图片仓库失败。",
                    original_item_name, image_type,
                )
                return jsonify({"status": "image_update_archive_failed", "item_id": original_item_id}), 200
            content_hash = str(cached["content_hash"])
            metadata_image_value = cache_token(content_hash)

        updated = update_cached_image_path(
            identity["tmdb_id"],
            identity["media_type"],
            original_item_type,
            image_type,
            metadata_image_value,
            season_number=identity.get("season_number"),
            episode_number=identity.get("episode_number"),
        )
        from handler.media_image_policy import update_manual_policy_image
        policy_updated = update_manual_policy_image(
            original_item_type,
            identity["tmdb_id"],
            image_type,
            source_url,
            request.host_url,
            season_number=identity.get("season_number"),
            episode_number=identity.get("episode_number"),
            content_hash=content_hash,
        )
        updated = max(updated, policy_updated)
        logger.info(
            "  ➜ [图片更新] 已将 '%s' 手动选择的 %s 图片写入 ETK 缓存，更新 %s 条记录。",
            original_item_name, image_type, updated,
        )
        return jsonify({"status": "image_update_cached", "item_id": original_item_id, "updated": updated}), 200

    return jsonify({"status": "event_unhandled"}), 500
