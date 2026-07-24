import hashlib
import logging
import mimetypes
import os
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode, urlparse

import requests

import config_manager
import constants
from database.connection import get_db_connection


logger = logging.getLogger(__name__)

MAX_IMAGE_BYTES = 32 * 1024 * 1024
CACHE_TOKEN_PREFIX = "etk-cache://"
_URL_LOCKS = [threading.Lock() for _ in range(32)]
_MIME_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/avif": ".avif",
    "image/svg+xml": ".svg",
}


def image_archive_enabled() -> bool:
    return bool(
        config_manager.APP_CONFIG.get(
            constants.CONFIG_OPTION_MEDIA_IMAGE_ARCHIVE_ENABLED,
            False,
        )
    )


def get_image_archive_path() -> str:
    configured = str(
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_MEDIA_IMAGE_ARCHIVE_PATH) or ""
    ).strip()
    if configured:
        configured = os.path.expandvars(os.path.expanduser(configured))
        if not os.path.isabs(configured):
            configured = os.path.join(config_manager.PERSISTENT_DATA_PATH, configured)
        return os.path.abspath(configured)
    return os.path.abspath(os.path.join(config_manager.PERSISTENT_DATA_PATH, "media_images"))


def _safe_cached_path(relative_path: str):
    root = get_image_archive_path()
    relative_path = str(relative_path or "").replace("/", os.sep).replace("\\", os.sep)
    path = os.path.abspath(os.path.join(root, relative_path))
    try:
        if os.path.commonpath([root, path]) != root:
            return None
    except ValueError:
        return None
    return path


def _source_record(source_url: str, *, require_file: bool = True):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_url, content_hash, relative_path, mime_type, byte_size
                FROM media_image_cache WHERE source_url=%s
                """,
                (source_url,),
            )
            row = cursor.fetchone()
    if not row:
        return None
    path = _safe_cached_path(row.get("relative_path"))
    if require_file and (not path or not os.path.isfile(path)):
        return None
    return {**dict(row), "path": path}


def _save_record(
    source_url: str,
    content_hash: str,
    relative_path: str,
    mime_type: str,
    byte_size: int,
):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO media_image_cache (
                    source_url, content_hash, relative_path, mime_type, byte_size
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE SET
                    content_hash=EXCLUDED.content_hash,
                    relative_path=EXCLUDED.relative_path,
                    mime_type=EXCLUDED.mime_type,
                    byte_size=EXCLUDED.byte_size,
                    last_accessed_at=NOW()
                """,
                (source_url, content_hash, relative_path, mime_type, byte_size),
            )
        conn.commit()


def _store_bytes(source_url: str, image_data: bytes, mime_type: str):
    if not image_data:
        raise ValueError("图片内容为空")
    if len(image_data) > MAX_IMAGE_BYTES:
        raise ValueError("图片超过 32MB 限制")

    mime_type = str(mime_type or "").split(";", 1)[0].strip().lower()
    if not mime_type.startswith("image/"):
        raise ValueError("缓存内容不是图片")

    content_hash = hashlib.sha256(image_data).hexdigest()
    extension = _MIME_EXTENSIONS.get(mime_type) or mimetypes.guess_extension(mime_type) or ".img"
    relative_path = f"{content_hash[:2]}/{content_hash}{extension}"
    final_path = _safe_cached_path(relative_path)
    if not final_path:
        raise ValueError("图片仓库路径无效")

    temp_dir = os.path.join(get_image_archive_path(), ".tmp")
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    temp_path = os.path.join(temp_dir, uuid.uuid4().hex)
    try:
        if not os.path.isfile(final_path):
            with open(temp_path, "wb") as output:
                output.write(image_data)
            os.replace(temp_path, final_path)
        _save_record(source_url, content_hash, relative_path, mime_type, len(image_data))
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass

    return {
        "source_url": source_url,
        "content_hash": content_hash,
        "relative_path": relative_path,
        "mime_type": mime_type,
        "byte_size": len(image_data),
        "path": final_path,
    }


def _download_image(source_url: str):
    parsed = urlparse(source_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    response = requests.get(
        source_url,
        stream=True,
        timeout=(15, 90),
        headers={
            "User-Agent": config_manager.APP_CONFIG.get("user_agent") or "Mozilla/5.0"
        },
        proxies=config_manager.get_proxies_for_requests(),
    )
    try:
        response.raise_for_status()
        mime_type = str(response.headers.get("Content-Type") or "").split(";", 1)[0]
        mime_type = mime_type.strip().lower()
        if not mime_type.startswith("image/"):
            mime_type = str(mimetypes.guess_type(parsed.path)[0] or "").lower()
        if not mime_type.startswith("image/"):
            raise ValueError("远端响应不是图片")

        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_IMAGE_BYTES:
            raise ValueError("图片超过 32MB 限制")

        chunks = []
        byte_size = 0
        for chunk in response.iter_content(chunk_size=128 * 1024):
            if not chunk:
                continue
            byte_size += len(chunk)
            if byte_size > MAX_IMAGE_BYTES:
                raise ValueError("图片超过 32MB 限制")
            chunks.append(chunk)
        return _store_bytes(source_url, b"".join(chunks), mime_type)
    finally:
        response.close()


def cache_remote_image(source_url: str):
    source_url = str(source_url or "").strip()
    if not image_archive_enabled() or not source_url:
        return None
    try:
        cached = _source_record(source_url)
        if cached:
            return cached

        lock_key = int(hashlib.sha256(source_url.encode("utf-8")).hexdigest()[:8], 16)
        with _URL_LOCKS[lock_key % len(_URL_LOCKS)]:
            cached = _source_record(source_url)
            if cached:
                return cached
            return _download_image(source_url)
    except Exception as exc:
        logger.warning("  ➜ [图片仓库] 缓存图片失败，继续使用原始地址: %s", exc)
        return None


def cache_image_bytes(source_url: str, image_data: bytes, mime_type: str, *, force: bool = False):
    source_url = str(source_url or "").strip()
    if (not force and not image_archive_enabled()) or not source_url:
        return None
    try:
        return _store_bytes(source_url, image_data, mime_type)
    except Exception as exc:
        logger.warning("  ➜ [图片仓库] 保存上传图片失败: %s", exc)
        return None


def get_cached_image(content_hash: str):
    content_hash = str(content_hash or "").strip().lower()
    if not re.fullmatch(r"[a-f0-9]{64}", content_hash):
        return None
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT content_hash, relative_path, mime_type, byte_size
                FROM media_image_cache
                WHERE content_hash=%s
                ORDER BY last_accessed_at DESC NULLS LAST LIMIT 1
                """,
                (content_hash,),
            )
            row = cursor.fetchone()
            if row:
                cursor.execute(
                    "UPDATE media_image_cache SET last_accessed_at=NOW() WHERE content_hash=%s",
                    (content_hash,),
                )
        conn.commit()
    if not row:
        return None
    path = _safe_cached_path(row.get("relative_path"))
    if not path or not os.path.isfile(path):
        return None
    return {**dict(row), "path": path}


def cache_token(content_hash: str) -> str:
    return f"{CACHE_TOKEN_PREFIX}{content_hash}"


def _episode_screenshot_source(series_tmdb_id: str, season_number: int, episode_number: int):
    series_tmdb_id = str(series_tmdb_id or "").strip()
    if not series_tmdb_id:
        raise ValueError("缺少剧集 TMDb ID")
    return (
        f"episode-screenshot://{series_tmdb_id}/"
        f"S{int(season_number):02d}/E{int(episode_number):02d}"
    )


def _delete_file_if_unreferenced(content_hash: str):
    content_hash = str(content_hash or "").strip().lower()
    if not re.fullmatch(r"[a-f0-9]{64}", content_hash):
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM media_image_cache WHERE content_hash=%s) AS used",
                    (content_hash,),
                )
                cache_used = bool((cursor.fetchone() or {}).get("used"))
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM media_metadata WHERE screenshot_hash=%s) AS used",
                    (content_hash,),
                )
                metadata_used = bool((cursor.fetchone() or {}).get("used"))
                if not metadata_used:
                    token = cache_token(content_hash)
                    cursor.execute(
                        """
                        SELECT EXISTS(
                            SELECT 1 FROM media_metadata
                            WHERE poster_path=%s OR backdrop_path=%s
                               OR logo_path=%s OR thumb_path=%s
                        ) AS used
                        """,
                        (token, token, token, token),
                    )
                    metadata_used = bool((cursor.fetchone() or {}).get("used"))
    except Exception as exc:
        logger.warning("  ➜ [图片仓库] 检查截图引用失败: %s", exc)
        return
    if cache_used or metadata_used:
        return

    shard = _safe_cached_path(content_hash[:2])
    if not shard or not os.path.isdir(shard):
        return
    for path in Path(shard).glob(f"{content_hash}.*"):
        try:
            path.unlink()
        except OSError as exc:
            logger.warning("  ➜ [图片仓库] 删除失效截图失败: %s", exc)


def archive_episode_screenshot(
    series_tmdb_id: str,
    season_number: int,
    episode_number: int,
    image_data: bytes,
    mime_type: str = "image/jpeg",
) -> str | None:
    """Persist the current fallback screenshot for one TMDb episode."""
    if not image_archive_enabled():
        return None
    try:
        source_url = _episode_screenshot_source(
            series_tmdb_id, season_number, episode_number
        )
    except (TypeError, ValueError):
        return None

    try:
        old_record = _source_record(source_url, require_file=False)
        lock_key = int(hashlib.sha256(source_url.encode("utf-8")).hexdigest()[:8], 16)
        with _URL_LOCKS[lock_key % len(_URL_LOCKS)]:
            cached = _store_bytes(source_url, image_data, mime_type)
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE media_metadata
                        SET screenshot_hash=%s, last_updated_at=NOW()
                        WHERE parent_series_tmdb_id=%s AND item_type='Episode'
                          AND season_number=%s AND episode_number=%s
                          AND (poster_path IS NULL OR btrim(poster_path)='')
                        RETURNING screenshot_hash
                        """,
                        (
                            cached["content_hash"], str(series_tmdb_id),
                            int(season_number), int(episode_number),
                        ),
                    )
                    updated = cursor.fetchall()
                conn.commit()
            if not updated:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "DELETE FROM media_image_cache WHERE source_url=%s",
                            (source_url,),
                        )
                    conn.commit()
                _delete_file_if_unreferenced(cached["content_hash"])
                return None
    except Exception as exc:
        logger.warning("  ➜ [图片仓库] 保存分集截图失败: %s", exc)
        return None

    old_hash = (old_record or {}).get("content_hash")
    if old_hash and old_hash != cached["content_hash"]:
        _delete_file_if_unreferenced(old_hash)
    return cached["content_hash"]


def _read_existing_emby_primary(item_id: str):
    base_url = str(
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or ""
    ).rstrip("/")
    api_key = str(
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY) or ""
    ).strip()
    user_id = str(
        config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID) or ""
    ).strip()
    if not all([item_id, base_url, api_key, user_id]):
        return None

    headers = {"X-Emby-Token": api_key}
    try:
        with requests.Session() as session:
            session.trust_env = False
            details = session.get(
                f"{base_url}/Users/{user_id}/Items/{item_id}",
                params={"api_key": api_key, "Fields": "ImageTags"},
                headers={**headers, "Accept": "application/json"},
                timeout=(5, 15),
            )
            if details.status_code != 200:
                logger.debug(
                    "  ➜ [图片仓库] 跳过读取 Emby Item %s：详情返回 HTTP %s。",
                    item_id,
                    details.status_code,
                )
                return None
            if not ((details.json().get("ImageTags") or {}).get("Primary")):
                return None

            response = session.get(
                f"{base_url}/Items/{item_id}/Images/Primary",
                params={"api_key": api_key},
                headers=headers,
                stream=True,
                timeout=(5, 30),
            )
            if response.status_code != 200:
                logger.debug(
                    "  ➜ [图片仓库] 跳过读取 Emby Item %s 的 Primary：HTTP %s。",
                    item_id,
                    response.status_code,
                )
                return None

            mime_type = str(response.headers.get("Content-Type") or "").split(";", 1)[0]
            if not mime_type.startswith("image/"):
                return None
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > MAX_IMAGE_BYTES:
                return None

            chunks = []
            byte_size = 0
            for chunk in response.iter_content(chunk_size=128 * 1024):
                if not chunk:
                    continue
                byte_size += len(chunk)
                if byte_size > MAX_IMAGE_BYTES:
                    return None
                chunks.append(chunk)
            return (b"".join(chunks), mime_type) if chunks else None
    except (ValueError, requests.RequestException) as exc:
        logger.debug("  ➜ [图片仓库] 读取 Emby Item %s 的现有截图失败: %s", item_id, exc)
        return None


def _archive_existing_emby_episode_image(payload, emby_item_ids):
    for item_id in emby_item_ids or []:
        result = _read_existing_emby_primary(str(item_id))
        if not result:
            continue
        image_data, mime_type = result
        content_hash = archive_episode_screenshot(
            payload.get("series_tmdb_id"),
            payload.get("season_number"),
            payload.get("episode_number"),
            image_data,
            mime_type,
        )
        if content_hash:
            logger.debug(
                "  ➜ [图片仓库] 已从 Emby Item %s 补存 S%02dE%02d 的兜底截图。",
                item_id,
                int(payload.get("season_number")),
                int(payload.get("episode_number")),
            )
            return content_hash
    return None


def discard_episode_screenshot(
    series_tmdb_id: str,
    season_number: int,
    episode_number: int,
) -> bool:
    """Delete a fallback screenshot after the episode has a formal still."""
    try:
        source_url = _episode_screenshot_source(
            series_tmdb_id, season_number, episode_number
        )
    except (TypeError, ValueError):
        return False

    try:
        hashes = set()
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT screenshot_hash FROM media_metadata
                    WHERE parent_series_tmdb_id=%s AND item_type='Episode'
                      AND season_number=%s AND episode_number=%s
                      AND screenshot_hash IS NOT NULL
                    """,
                    (str(series_tmdb_id), int(season_number), int(episode_number)),
                )
                hashes.update(row["screenshot_hash"] for row in cursor.fetchall())
                cursor.execute(
                    "SELECT content_hash FROM media_image_cache WHERE source_url=%s",
                    (source_url,),
                )
                hashes.update(row["content_hash"] for row in cursor.fetchall())
                cursor.execute(
                    """
                    UPDATE media_metadata SET screenshot_hash=NULL, last_updated_at=NOW()
                    WHERE parent_series_tmdb_id=%s AND item_type='Episode'
                      AND season_number=%s AND episode_number=%s
                      AND screenshot_hash IS NOT NULL
                    """,
                    (str(series_tmdb_id), int(season_number), int(episode_number)),
                )
                metadata_updated = cursor.rowcount
                cursor.execute(
                    "DELETE FROM media_image_cache WHERE source_url=%s",
                    (source_url,),
                )
                cache_deleted = cursor.rowcount
            conn.commit()

        for content_hash in hashes:
            _delete_file_if_unreferenced(content_hash)
        return bool(metadata_updated or cache_deleted)
    except Exception as exc:
        logger.warning("  ➜ [图片仓库] 清理被正式剧照替换的截图失败: %s", exc)
        return False


def _cached_url(content_hash: str, base_url: str):
    query = urlencode({"cache": content_hash})
    return f"{str(base_url or '').rstrip('/')}/api/image_proxy?{query}"


def _archive_url(source_url: str, base_url: str):
    source_url = str(source_url or "").strip()
    if source_url.startswith(CACHE_TOKEN_PREFIX):
        content_hash = source_url[len(CACHE_TOKEN_PREFIX):].strip().lower()
        if re.fullmatch(r"[a-f0-9]{64}", content_hash):
            return _cached_url(content_hash, base_url)
        return source_url
    cached = cache_remote_image(source_url)
    if not cached:
        return source_url
    return _cached_url(cached["content_hash"], base_url)


def archive_policy_images(images, base_url: str):
    values = [dict(item) for item in images or [] if isinstance(item, dict)]
    sources = {
        str(item.get("source_url") or "").strip()
        for item in values
        if item.get("source_url")
    }
    cached_by_source = {}
    if image_archive_enabled() and sources:
        with ThreadPoolExecutor(max_workers=min(4, len(sources))) as executor:
            futures = {
                source: executor.submit(cache_remote_image, source)
                for source in sources
            }
            for source, future in futures.items():
                try:
                    cached_by_source[source] = future.result()
                except Exception as exc:
                    logger.warning("  ➜ [图片仓库] 缓存策略图片失败: %s", exc)

    for item in values:
        source = str(item.get("source_url") or "").strip()
        if source.startswith(CACHE_TOKEN_PREFIX):
            token_hash = source[len(CACHE_TOKEN_PREFIX):].strip().lower()
            if re.fullmatch(r"[a-f0-9]{64}", token_hash):
                item["content_hash"] = token_hash
        cached = cached_by_source.get(source)
        if cached:
            item["content_hash"] = cached["content_hash"]
        content_hash = str(item.get("content_hash") or "").strip().lower()
        if content_hash and get_cached_image(content_hash):
            item["url"] = _cached_url(content_hash, base_url)
            item["thumbnail_url"] = item["url"]
        else:
            item.pop("content_hash", None)
            item["url"] = source
            item["thumbnail_url"] = item.get("thumbnail_source_url") or source
    return values


def discard_unreferenced_policy_sources(source_urls):
    from database.image_policy_db import source_is_referenced

    for source_url in {str(value or "").strip() for value in source_urls if value}:
        if source_is_referenced(source_url):
            continue
        record = _source_record(source_url, require_file=False)
        if not record:
            continue
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM media_image_cache WHERE source_url=%s",
                    (source_url,),
                )
            conn.commit()
        _delete_file_if_unreferenced(record.get("content_hash"))


def archive_metadata_images(payload, base_url: str, *, archive_sources: bool = True):
    if not isinstance(payload, dict):
        return payload
    screenshot_hash = str(payload.pop("_screenshot_hash", "") or "").strip()
    emby_item_ids = payload.pop("_emby_item_ids", [])
    images = payload.get("images")
    if not isinstance(images, dict):
        return payload

    original_primary = images.get("primary")
    targets = {
        key: value
        for key, value in images.items()
        if key in {"primary", "backdrop", "logo", "thumb"}
        and value
        and (
            str(value).startswith(CACHE_TOKEN_PREFIX)
            or (archive_sources and image_archive_enabled())
        )
    }
    if targets:
        with ThreadPoolExecutor(max_workers=min(4, len(targets))) as executor:
            futures = {
                key: executor.submit(_archive_url, value, base_url)
                for key, value in targets.items()
            }
            for key, future in futures.items():
                try:
                    images[key] = future.result()
                except Exception as exc:
                    logger.warning("  ➜ [图片仓库] 生成本地图片地址失败: %s", exc)

    if (
        image_archive_enabled()
        and payload.get("item_type") == "Episode"
        and not screenshot_hash
        and not original_primary
    ):
        screenshot_hash = _archive_existing_emby_episode_image(payload, emby_item_ids)
        if screenshot_hash:
            screenshot_url = _cached_url(screenshot_hash, base_url)
            images["primary"] = screenshot_url
            images["backdrop"] = images.get("backdrop") or screenshot_url
            images["thumb"] = images.get("thumb") or screenshot_url

    formal_primary_returned = (
        payload.get("item_type") == "Episode"
        and screenshot_hash
        and original_primary
        and not str(original_primary).startswith(CACHE_TOKEN_PREFIX)
    )
    if formal_primary_returned:
        timer = threading.Timer(
            30,
            discard_episode_screenshot,
            args=(
                payload.get("series_tmdb_id"),
                payload.get("season_number"),
                payload.get("episode_number"),
            ),
        )
        timer.daemon = True
        timer.start()
    return payload
