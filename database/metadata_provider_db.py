import json
from datetime import date, datetime
from typing import Any, Dict, Optional

from psycopg2.extras import Json

import config_manager
import constants
import utils
from database.connection import get_db_connection


MEDIA_METADATA_SCHEMA_VERSION = 1


def build_image_language_priority(original_language: str, preference: str):
    """Return the configured image language groups from highest to lowest priority."""
    original = str(original_language or "").strip().lower()
    chinese = ("zh-cn", "zh-tw", "zh-hk", "zh-sg", "zh", "cn")
    english_or_textless = ("en", "null")
    groups = []

    def _add(values):
        values = tuple(value for value in values if value)
        if values and values not in groups:
            groups.append(values)

    if str(preference or "").strip().lower() == "original":
        if original.startswith("zh") or original == "cn":
            _add(chinese)
        else:
            _add((original,))
        _add(english_or_textless)
        if not (original.startswith("zh") or original == "cn"):
            _add(chinese)
    else:
        _add(("zh-cn",))
        _add(("zh-tw", "zh-hk", "zh-sg", "zh", "cn"))
        _add(english_or_textless)
        if original and not original.startswith("zh") and original not in {"cn", "en"}:
            _add((original,))
    return groups


def build_image_language_parameter(priority_groups) -> str:
    tmdb_codes = {
        "zh-cn": "zh-CN",
        "zh-tw": "zh-TW",
        "zh-hk": "zh-HK",
        "zh-sg": "zh-SG",
    }
    values = []
    for group in priority_groups or []:
        for value in group:
            value = tmdb_codes.get(value, value)
            if value != "cn" and value not in values:
                values.append(value)
    return ",".join(values)


def select_image_path(images, priority_groups) -> Optional[str]:
    """Select the highest-rated image in the first configured language group that has one."""
    candidates = sort_image_candidates(images, priority_groups)
    return str(candidates[0]["file_path"]) if candidates else None


def sort_image_candidates(images, priority_groups):
    """Sort all TMDb images by ETK language preference, rating and vote count."""
    candidates = [item for item in images or [] if isinstance(item, dict) and item.get("file_path")]

    def _language(value):
        return str(value or "null").strip().lower()

    def _rank(item):
        language = _language(item.get("iso_639_1"))
        group_rank = len(priority_groups or [])
        for index, group in enumerate(priority_groups or []):
            if language in group:
                group_rank = index
                break
        try:
            vote_average = float(item.get("vote_average") or 0)
        except (TypeError, ValueError):
            vote_average = 0
        try:
            vote_count = int(item.get("vote_count") or 0)
        except (TypeError, ValueError):
            vote_count = 0
        return group_rank, -vote_average, -vote_count

    return sorted(candidates, key=_rank)


def preferred_image_candidates(images, priority_groups):
    """Return every candidate in the first configured language group that has images."""
    ordered = sort_image_candidates(images, priority_groups)
    for group in priority_groups or []:
        selected = [
            item for item in ordered
            if str(item.get("iso_639_1") or "null").strip().lower() in group
        ]
        if selected:
            return selected
    return ordered


def select_collection_image_paths(details, images, preference: str):
    """Select collection artwork using the first movie's original language."""
    parts = (details or {}).get("parts") or []
    original_language = next(
        (
            str(item.get("original_language") or "").strip()
            for item in parts
            if isinstance(item, dict) and item.get("original_language")
        ),
        "",
    )
    priorities = build_image_language_priority(original_language, preference)
    return {
        "poster_path": (
            select_image_path((images or {}).get("posters"), priorities)
            or (details or {}).get("poster_path")
        ),
        "backdrop_path": (
            select_image_path((images or {}).get("backdrops"), priorities)
            or (details or {}).get("backdrop_path")
        ),
        "original_language": original_language,
    }


def get_cached_original_language(tmdb_id: str, media_type: str) -> str:
    item_type = "Series" if str(media_type or "").lower() == "tv" else "Movie"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT original_language FROM media_metadata WHERE tmdb_id=%s AND item_type=%s",
                (str(tmdb_id), item_type),
            )
            row = cursor.fetchone()
    return str((row or {}).get("original_language") or "")


def replace_cached_image_paths(
    tmdb_id: str,
    media_type: str,
    requested_type: str,
    *,
    root_images: Optional[Dict[str, Optional[str]]] = None,
    season_number: Optional[int] = None,
    season_posters: Optional[Dict[int, Optional[str]]] = None,
    episode_number: Optional[int] = None,
    episode_still: Optional[str] = None,
) -> int:
    """Replace only cached image paths, leaving all other localized metadata untouched."""
    requested_type = str(requested_type or "").strip().title()
    media_type = str(media_type or "").strip().lower()
    updated = 0
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if requested_type in {"Movie", "Series"} and root_images is not None:
                item_type = "Movie" if media_type == "movie" else "Series"
                cursor.execute(
                    """
                    UPDATE media_metadata SET
                        poster_path=%s, backdrop_path=%s, logo_path=%s, thumb_path=%s,
                        last_updated_at=NOW()
                    WHERE tmdb_id=%s AND item_type=%s
                    """,
                    (
                        root_images.get("poster"), root_images.get("backdrop"),
                        root_images.get("logo"), root_images.get("thumb"),
                        str(tmdb_id), item_type,
                    ),
                )
                updated += cursor.rowcount

                if item_type == "Series" and season_posters is not None:
                    cursor.execute(
                        """
                        UPDATE media_metadata SET poster_path=NULL, last_updated_at=NOW()
                        WHERE parent_series_tmdb_id=%s AND item_type='Season'
                        """,
                        (str(tmdb_id),),
                    )
                    for number, poster_path in season_posters.items():
                        cursor.execute(
                            """
                            UPDATE media_metadata SET poster_path=%s, last_updated_at=NOW()
                            WHERE parent_series_tmdb_id=%s AND item_type='Season' AND season_number=%s
                            """,
                            (poster_path, str(tmdb_id), int(number)),
                        )
                        updated += cursor.rowcount
            elif requested_type == "Season" and season_number is not None:
                poster_path = (season_posters or {}).get(int(season_number))
                cursor.execute(
                    """
                    UPDATE media_metadata SET poster_path=%s, last_updated_at=NOW()
                    WHERE parent_series_tmdb_id=%s AND item_type='Season' AND season_number=%s
                    """,
                    (poster_path, str(tmdb_id), int(season_number)),
                )
                updated += cursor.rowcount
            elif requested_type == "Episode" and season_number is not None and episode_number is not None:
                cursor.execute(
                    """
                    UPDATE media_metadata SET
                        poster_path=%s, backdrop_path=%s, logo_path=NULL, thumb_path=%s,
                        last_updated_at=NOW()
                    WHERE parent_series_tmdb_id=%s AND item_type='Episode'
                      AND season_number=%s AND episode_number=%s
                    """,
                    (
                        episode_still, episode_still, episode_still, str(tmdb_id),
                        int(season_number), int(episode_number),
                    ),
                )
                updated += cursor.rowcount
        conn.commit()
    return updated


def update_cached_image_path(
    tmdb_id: str,
    media_type: str,
    requested_type: str,
    image_type: str,
    image_path: str,
    *,
    season_number: Optional[int] = None,
    episode_number: Optional[int] = None,
) -> int:
    """Update one manually selected TMDb image without changing the other cached images."""
    requested_type = str(requested_type or "").strip().title()
    image_type = str(image_type or "").strip().title()
    column = {
        "Primary": "poster_path",
        "Backdrop": "backdrop_path",
        "Logo": "logo_path",
        "Thumb": "thumb_path",
    }.get(image_type)
    if not column or not image_path:
        return 0

    if requested_type in {"Movie", "Series"}:
        item_type = "Movie" if str(media_type or "").lower() == "movie" else "Series"
        where_sql = "tmdb_id=%s AND item_type=%s"
        where_params = (str(tmdb_id), item_type)
    elif requested_type == "Season" and season_number is not None and image_type == "Primary":
        where_sql = "parent_series_tmdb_id=%s AND item_type='Season' AND season_number=%s"
        where_params = (str(tmdb_id), int(season_number))
    elif requested_type == "Episode" and season_number is not None and episode_number is not None:
        where_sql = (
            "parent_series_tmdb_id=%s AND item_type='Episode' "
            "AND season_number=%s AND episode_number=%s"
        )
        where_params = (str(tmdb_id), int(season_number), int(episode_number))
    else:
        return 0

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE media_metadata SET {column}=%s, last_updated_at=NOW() WHERE {where_sql}",
                (image_path, *where_params),
            )
            updated = cursor.rowcount
        conn.commit()
    if updated and not str(image_path).startswith("etk-cache://"):
        from handler.media_image_cache import cache_remote_image
        cache_remote_image(_tmdb_image_url(image_path, "original"))
    if (
        updated
        and requested_type == "Episode"
        and image_type == "Primary"
        and season_number is not None
        and episode_number is not None
    ):
        from handler.media_image_cache import discard_episode_screenshot
        discard_episode_screenshot(
            str(tmdb_id), int(season_number), int(episode_number)
        )
    return updated


def _keywords(details: Dict[str, Any], media_type: str):
    container = details.get("keywords") or {}
    if isinstance(container, list):
        return container
    key = "results" if media_type == "tv" else "keywords"
    if isinstance(container, dict):
        return container.get(key) or []
    return []


def _directors(details: Dict[str, Any]):
    credits = details.get("credits") or {}
    return [
        {
            "id": person.get("id"),
            "name": person.get("name"),
            "profile_path": person.get("profile_path"),
        }
        for person in credits.get("crew") or []
        if person.get("job") in {"Director", "Series Director"} and person.get("id")
    ]


def _date_value(value):
    if not value:
        return None
    return str(value)[:10]


def _runtime(details: Dict[str, Any], media_type: str):
    value = details.get("runtime")
    if media_type == "tv" and not value:
        values = details.get("episode_run_time") or []
        value = values[0] if values else None
    try:
        value = int(value)
        return value if value > 0 else None
    except (TypeError, ValueError):
        return None


def persist_initial_tmdb_metadata(
    details: Dict[str, Any],
    media_type: str,
    *,
    title: str = "",
    original_title: str = "",
    rating_label: str = "",
    image_language_preference: str = "zh",
) -> bool:
    """Persist the pre-Emby TMDb snapshot without overwriting translated metadata."""
    if not isinstance(details, dict):
        return False
    tmdb_id = str(details.get("id") or "").strip()
    media_type = str(media_type or "").lower()
    if not tmdb_id.isdigit() or media_type not in {"movie", "tv"}:
        return False

    images = details.get("images") or {}
    priorities = build_image_language_priority(
        details.get("original_language"), image_language_preference
    )
    poster_path = select_image_path(images.get("posters"), priorities) or details.get("poster_path")
    backdrop_path = select_image_path(images.get("backdrops"), priorities) or details.get("backdrop_path")
    logo_path = select_image_path(images.get("logos"), priorities)
    thumb_path = backdrop_path
    item_type = "Series" if media_type == "tv" else "Movie"
    release_date = details.get("first_air_date") if media_type == "tv" else details.get("release_date")
    countries = details.get("origin_country") or [
        item.get("iso_3166_1")
        for item in details.get("production_countries") or []
        if item.get("iso_3166_1")
    ]
    row = {
        "tmdb_id": tmdb_id,
        "item_type": item_type,
        "title": title or details.get("name") or details.get("title"),
        "original_title": original_title or details.get("original_name") or details.get("original_title"),
        "original_language": details.get("original_language"),
        "overview": details.get("overview"),
        "tagline": details.get("tagline"),
        "release_date": _date_value(release_date),
        "release_year": int(str(release_date)[:4]) if release_date and str(release_date)[:4].isdigit() else None,
        "last_air_date": _date_value(details.get("last_air_date")),
        "poster_path": poster_path,
        "backdrop_path": backdrop_path,
        "logo_path": logo_path,
        "thumb_path": thumb_path,
        "homepage": details.get("homepage"),
        "runtime_minutes": _runtime(details, media_type),
        "rating": details.get("vote_average"),
        "custom_rating": rating_label if rating_label and rating_label != "未知" else None,
        "imdb_id": (details.get("external_ids") or {}).get("imdb_id"),
        "genres_json": details.get("genres") or [],
        "directors_json": _directors(details),
        "production_companies_json": details.get("production_companies") or [],
        "networks_json": details.get("networks") or [],
        "countries_json": countries,
        "keywords_json": _keywords(details, media_type),
        "total_episodes": details.get("number_of_episodes") or 0,
        "watchlist_tmdb_status": details.get("status"),
        "metadata_schema_version": MEDIA_METADATA_SCHEMA_VERSION,
    }

    columns = list(row)
    values = [
        Json(value, dumps=lambda obj: json.dumps(obj, ensure_ascii=False))
        if column.endswith("_json") else value
        for column, value in row.items()
    ]
    protected = {
        "title", "original_title", "original_language", "overview", "tagline",
        "release_date", "release_year", "last_air_date", "homepage", "runtime_minutes",
        "rating", "custom_rating", "imdb_id", "genres_json", "directors_json",
        "production_companies_json", "networks_json", "countries_json", "keywords_json",
        "total_episodes", "watchlist_tmdb_status",
    }
    updates = []
    for column in columns:
        if column in {"tmdb_id", "item_type"}:
            continue
        if column == "metadata_schema_version":
            updates.append(
                f"{column}=CASE WHEN media_metadata.actors_ready IS TRUE "
                f"THEN media_metadata.{column} ELSE EXCLUDED.{column} END"
            )
        elif column in protected:
            updates.append(
                f"{column}=CASE WHEN media_metadata.actors_ready IS TRUE "
                f"THEN COALESCE(media_metadata.{column}, EXCLUDED.{column}) ELSE EXCLUDED.{column} END"
            )
        else:
            updates.append(f"{column}=COALESCE(media_metadata.{column}, EXCLUDED.{column})")
    updates.extend([
        "metadata_ready=TRUE",
        "actors_ready=media_metadata.actors_ready",
        "last_updated_at=NOW()",
    ])

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO media_metadata ({', '.join(columns)}, metadata_ready, actors_ready)
                VALUES ({', '.join(['%s'] * len(columns))}, TRUE, FALSE)
                ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {', '.join(updates)}
                """,
                values,
            )

            if media_type == "tv":
                for season in details.get("seasons") or []:
                    season_id = str(season.get("id") or "").strip()
                    season_number = season.get("season_number")
                    if not season_id or season_number is None:
                        continue
                    cursor.execute(
                        """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, parent_series_tmdb_id, season_number,
                            title, overview, release_date, poster_path, total_episodes,
                            metadata_ready, actors_ready, metadata_schema_version
                        ) VALUES (%s, 'Season', %s, %s, %s, %s, %s, %s, %s, TRUE, FALSE, %s)
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            parent_series_tmdb_id=EXCLUDED.parent_series_tmdb_id,
                            season_number=EXCLUDED.season_number,
                            title=CASE WHEN media_metadata.actors_ready IS TRUE THEN media_metadata.title ELSE EXCLUDED.title END,
                            overview=CASE WHEN media_metadata.actors_ready IS TRUE THEN media_metadata.overview ELSE EXCLUDED.overview END,
                            release_date=CASE WHEN media_metadata.actors_ready IS TRUE THEN media_metadata.release_date ELSE EXCLUDED.release_date END,
                            poster_path=COALESCE(media_metadata.poster_path, EXCLUDED.poster_path),
                            total_episodes=CASE WHEN media_metadata.actors_ready IS TRUE THEN media_metadata.total_episodes ELSE EXCLUDED.total_episodes END,
                            metadata_ready=TRUE,
                            actors_ready=media_metadata.actors_ready,
                            metadata_schema_version=CASE WHEN media_metadata.actors_ready IS TRUE
                                THEN media_metadata.metadata_schema_version ELSE EXCLUDED.metadata_schema_version END,
                            last_updated_at=NOW()
                        """,
                        (
                            season_id, tmdb_id, season_number, season.get("name"),
                            season.get("overview"), _date_value(season.get("air_date")),
                            season.get("poster_path"), season.get("episode_count") or 0,
                            MEDIA_METADATA_SCHEMA_VERSION,
                        ),
                    )
        conn.commit()
    return True


def has_initial_tmdb_metadata(tmdb_id: str, media_type: str) -> bool:
    item_type = "Series" if str(media_type).lower() == "tv" else "Movie"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM media_metadata
                WHERE tmdb_id=%s AND item_type=%s AND metadata_ready IS TRUE
                  AND metadata_schema_version >= %s
                LIMIT 1
                """,
                (str(tmdb_id), item_type, MEDIA_METADATA_SCHEMA_VERSION),
            )
            return cursor.fetchone() is not None


def needs_metadata_backfill(tmdb_id: str, media_type: str) -> bool:
    item_type = "Series" if str(media_type).lower() == "tv" else "Movie"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM media_metadata
                WHERE tmdb_id=%s AND item_type=%s AND in_library IS TRUE
                  AND (
                      metadata_schema_version < %s
                      OR (
                          item_type='Series'
                          AND EXISTS (
                              SELECT 1 FROM media_metadata episode
                              WHERE episode.parent_series_tmdb_id=media_metadata.tmdb_id
                                AND episode.item_type='Episode'
                                AND episode.in_library IS TRUE
                                AND episode.season_number=0
                                AND episode.tmdb_id LIKE media_metadata.tmdb_id || '-S0E%%'
                          )
                      )
                  )
                LIMIT 1
                """,
                (str(tmdb_id), item_type, MEDIA_METADATA_SCHEMA_VERSION),
            )
            return cursor.fetchone() is not None


def _json_value(value, default):
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _normalize_media_path(value: str) -> str:
    path = str(value or "").strip().replace("\\", "/")
    if not path:
        return ""
    while "//" in path:
        path = path.replace("//", "/")
    return path.rstrip("/") or "/"


def register_metadata_asset_path(
    tmdb_id: str,
    media_type: str,
    path: str,
    *,
    season_number: Optional[int] = None,
    episode_number: Optional[int] = None,
) -> bool:
    """Persist a physical path before Emby performs its first metadata lookup."""
    tmdb_id = str(tmdb_id or "").strip()
    media_type = str(media_type or "").strip().lower()
    path = _normalize_media_path(path)
    item_type = "Series" if media_type == "tv" else "Movie"
    if not tmdb_id.isdigit() or media_type not in {"movie", "tv"} or not path:
        return False

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT asset_details_json
                FROM media_metadata
                WHERE tmdb_id=%s AND item_type=%s
                FOR UPDATE
                """,
                (tmdb_id, item_type),
            )
            row = cursor.fetchone()
            if not row:
                return False
            assets = _json_value(row.get("asset_details_json"), [])
            assets = assets if isinstance(assets, list) else []
            target = next(
                (
                    asset for asset in assets
                    if isinstance(asset, dict)
                    and _normalize_media_path(asset.get("path")) == path
                ),
                None,
            )
            if target is None:
                target = {"path": path}
                assets.append(target)
            if season_number is not None:
                target["season_number"] = int(season_number)
            if episode_number is not None:
                target["episode_number"] = int(episode_number)
            cursor.execute(
                """
                UPDATE media_metadata
                SET asset_details_json=%s, last_updated_at=NOW()
                WHERE tmdb_id=%s AND item_type=%s
                """,
                (Json(assets, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)), tmdb_id, item_type),
            )
        conn.commit()
    return True


def resolve_metadata_identity_by_path(
    path: str,
    requested_type: str,
    *,
    season_number: Optional[int] = None,
    episode_number: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Resolve a physical Emby path to the root TMDb identity stored by ETK."""
    path = _normalize_media_path(path)
    requested_type = str(requested_type or "").strip().title()
    if not path or requested_type not in {"Movie", "Series", "Season", "Episode"}:
        return None
    item_types = ("Movie",) if requested_type == "Movie" else ("Series", "Season", "Episode")
    child_prefix = path.rstrip("/") + "/%"

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT m.tmdb_id, m.item_type, m.parent_series_tmdb_id,
                       m.season_number, m.episode_number, asset.value AS asset
                FROM media_metadata m
                CROSS JOIN LATERAL jsonb_array_elements(
                    CASE WHEN jsonb_typeof(m.asset_details_json)='array'
                         THEN m.asset_details_json ELSE '[]'::jsonb END
                ) AS asset(value)
                WHERE m.item_type=ANY(%s)
                  AND asset.value->>'path' IS NOT NULL
                  AND btrim(asset.value->>'path') <> ''
                  AND (
                      lower(asset.value->>'path')=lower(%s)
                      OR lower(asset.value->>'path') LIKE lower(%s)
                      OR lower(%s) LIKE lower(rtrim(asset.value->>'path', '/')) || '/%%'
                  )
                """,
                (list(item_types), path, child_prefix, path),
            )
            candidates = [dict(row) for row in cursor.fetchall()]
    if not candidates:
        return None

    def _score(row):
        asset = _json_value(row.get("asset"), {})
        asset_path = _normalize_media_path(asset.get("path") if isinstance(asset, dict) else "")
        return (
            0 if asset_path.casefold() == path.casefold() else 1,
            0 if row.get("item_type") == requested_type else 1,
            0 if season_number is None or row.get("season_number") in {None, season_number} else 1,
            0 if episode_number is None or row.get("episode_number") in {None, episode_number} else 1,
            abs(len(asset_path) - len(path)),
        )

    selected = min(candidates, key=_score)
    asset = _json_value(selected.get("asset"), {})
    is_movie = selected.get("item_type") == "Movie"
    root_tmdb_id = selected.get("tmdb_id") if is_movie else (
        selected.get("parent_series_tmdb_id") or selected.get("tmdb_id")
    )
    return {
        "tmdb_id": str(root_tmdb_id or ""),
        "media_type": "movie" if is_movie else "tv",
        "season_number": season_number if season_number is not None else (
            selected.get("season_number") if selected.get("season_number") is not None
            else asset.get("season_number")
        ),
        "episode_number": episode_number if episode_number is not None else (
            selected.get("episode_number") if selected.get("episode_number") is not None
            else asset.get("episode_number")
        ),
    }


def _text_date(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value) if value else None


def _source_names(values):
    result = []
    seen = set()
    for value in values or []:
        name = value.get("name") if isinstance(value, dict) else value
        name = str(name or "").strip()
        key = name.casefold()
        if name and key not in seen:
            seen.add(key)
            result.append(name)
    return result


def _mapped_labels(values, mapping, id_field):
    id_map = {}
    name_map = {}
    for entry in mapping or []:
        if not isinstance(entry, dict):
            continue
        label = str(entry.get("label") or "").strip()
        if not label:
            continue
        for value in entry.get(id_field) or []:
            id_map[str(value)] = label
        for value in entry.get("en") or []:
            name = str(value or "").strip()
            if name:
                name_map[name.casefold()] = label

    result = []
    seen = set()
    for value in values or []:
        item_id = value.get("id") if isinstance(value, dict) else None
        item_name = value.get("name") if isinstance(value, dict) else value
        label = id_map.get(str(item_id)) if item_id is not None else None
        if not label and item_name:
            label = name_map.get(str(item_name).strip().casefold())
        if label and label not in seen:
            seen.add(label)
            result.append(label)
    return result


def _provider_tags_and_studios(row):
    from database import settings_db

    keywords = _json_value(row.get("keywords_json"), [])
    companies = _json_value(row.get("production_companies_json"), [])
    networks = _json_value(row.get("networks_json"), [])

    if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_KEYWORD_TO_TAGS, False):
        try:
            keyword_mapping = settings_db.get_setting("keyword_mapping") or utils.DEFAULT_KEYWORD_MAPPING
        except Exception:
            keyword_mapping = utils.DEFAULT_KEYWORD_MAPPING
        tags = _mapped_labels(keywords, keyword_mapping, "ids")
    else:
        tags = _source_names(keywords)

    if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_STUDIO_TO_CHINESE, False):
        try:
            studio_mapping = settings_db.get_setting("studio_mapping") or utils.DEFAULT_STUDIO_MAPPING
        except Exception:
            studio_mapping = utils.DEFAULT_STUDIO_MAPPING
        studios = _mapped_labels(networks, studio_mapping, "network_ids")
        for label in _mapped_labels(companies, studio_mapping, "company_ids"):
            if label not in studios:
                studios.append(label)
    else:
        studios = _source_names(networks + companies)

    return tags, studios


def _format_actor_role_for_emby(role: Any, row: Dict[str, Any]) -> str:
    clean_role = utils.strip_character_role_display_prefix(role)
    if (
        not clean_role
        or clean_role in {"演员", "配音"}
        or not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_ACTOR_ROLE_ADD_PREFIX, False)
    ):
        return clean_role

    genres = _json_value(row.get("genres_json"), [])
    genre_names = {
        str(item.get("name") if isinstance(item, dict) else item).strip()
        for item in genres
        if item
    }
    is_voice_role = bool(genre_names & {"Animation", "动画", "Documentary", "纪录", "记录"})
    return f"{'配' if is_voice_role else '饰'} {clean_role}"


def load_emby_metadata(
    tmdb_id: str,
    root_media_type: str,
    requested_type: str,
    *,
    season_number: Optional[int] = None,
    episode_number: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Return one normalized metadata document for the Emby bridge provider."""
    requested_type = str(requested_type or "").strip().title()
    root_media_type = str(root_media_type or "").strip().lower()
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if requested_type == "Movie" and root_media_type == "movie":
                cursor.execute(
                    "SELECT * FROM media_metadata WHERE tmdb_id=%s AND item_type='Movie' AND metadata_ready IS TRUE",
                    (str(tmdb_id),),
                )
            elif requested_type == "Season":
                cursor.execute(
                    """
                    SELECT * FROM media_metadata
                    WHERE parent_series_tmdb_id=%s AND item_type='Season' AND season_number=%s
                      AND metadata_ready IS TRUE
                    ORDER BY last_updated_at DESC NULLS LAST LIMIT 1
                    """,
                    (str(tmdb_id), season_number),
                )
            elif requested_type == "Episode":
                cursor.execute(
                    """
                    SELECT * FROM media_metadata
                    WHERE parent_series_tmdb_id=%s AND item_type='Episode'
                      AND season_number=%s AND episode_number=%s
                      AND metadata_ready IS TRUE
                    ORDER BY last_updated_at DESC NULLS LAST LIMIT 1
                    """,
                    (str(tmdb_id), season_number, episode_number),
                )
            else:
                requested_type = "Series"
                cursor.execute(
                    "SELECT * FROM media_metadata WHERE tmdb_id=%s AND item_type='Series' AND metadata_ready IS TRUE",
                    (str(tmdb_id),),
                )
            row = cursor.fetchone()

            if not row and requested_type == "Episode":
                cursor.execute(
                    "SELECT * FROM media_metadata WHERE tmdb_id=%s AND item_type='Series' AND metadata_ready IS TRUE",
                    (str(tmdb_id),),
                )
                parent = cursor.fetchone()
                if not parent:
                    return None
                row = dict(parent)
                row.update({
                    "tmdb_id": None,
                    "item_type": "Episode",
                    "title": None,
                    "overview": None,
                    "season_number": season_number,
                    "episode_number": episode_number,
                    "poster_path": None,
                    "backdrop_path": None,
                    "logo_path": None,
                    "thumb_path": None,
                    "actors_ready": False,
                })
            if not row:
                return None

            row = dict(row)
            people = []
            collections = []
            if requested_type == "Movie" and root_media_type == "movie":
                cursor.execute(
                    """
                    SELECT tmdb_collection_id, name, all_tmdb_ids_json
                    FROM collections_info
                    WHERE all_tmdb_ids_json @> %s::jsonb
                    ORDER BY last_checked_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (json.dumps([str(tmdb_id)]),),
                )
                collection = cursor.fetchone()
                if collection and collection.get("tmdb_collection_id") and collection.get("name"):
                    collections.append({
                        "tmdb_id": str(collection["tmdb_collection_id"]),
                        "name": collection["name"],
                        "member_tmdb_ids": [
                            str(value)
                            for value in _json_value(collection.get("all_tmdb_ids_json"), [])
                            if value is not None
                        ],
                    })
            if row.get("actors_ready"):
                actor_links = _json_value(row.get("actors_json"), [])
                actor_ids = [link.get("tmdb_id") for link in actor_links if link.get("tmdb_id")]
                actor_map = {}
                if actor_ids:
                    cursor.execute(
                        """
                        SELECT tmdb_person_id, primary_name, profile_path
                        FROM person_metadata WHERE tmdb_person_id=ANY(%s)
                        """,
                        (actor_ids,),
                    )
                    actor_map = {item["tmdb_person_id"]: item for item in cursor.fetchall()}
                for link in actor_links:
                    actor = actor_map.get(link.get("tmdb_id"))
                    if actor:
                        people.append({
                            "name": actor.get("primary_name"),
                            "role": _format_actor_role_for_emby(link.get("character"), row),
                            "type": "Actor",
                            "tmdb_id": str(actor.get("tmdb_person_id")),
                            "image_url": _tmdb_image_url(actor.get("profile_path"), "w500"),
                            "order": link.get("order", 999),
                        })
                for director in _json_value(row.get("directors_json"), []):
                    if director.get("name"):
                        people.append({
                            "name": director.get("name"),
                            "type": "Director",
                            "tmdb_id": str(director.get("id") or ""),
                            "image_url": _tmdb_image_url(director.get("profile_path"), "w500"),
                            "order": 1000,
                        })

    official_ratings = _json_value(row.get("official_rating_json"), {})
    rating = official_ratings.get("US")
    tags, studios = _provider_tags_and_studios(row)
    screenshot_url = None
    if requested_type == "Episode" and not row.get("poster_path") and row.get("screenshot_hash"):
        from handler.media_image_cache import cache_token
        screenshot_url = cache_token(str(row["screenshot_hash"]))
    return {
        "item_type": requested_type,
        "tmdb_id": str(row.get("tmdb_id") or ""),
        "series_tmdb_id": str(tmdb_id) if root_media_type == "tv" else "",
        "imdb_id": row.get("imdb_id"),
        "name": row.get("title"),
        "original_title": row.get("original_title"),
        "overview": row.get("overview"),
        "tagline": row.get("tagline"),
        "premiere_date": _text_date(row.get("release_date")),
        "end_date": _text_date(row.get("last_air_date")),
        "production_year": row.get("release_year"),
        "community_rating": row.get("rating"),
        "official_rating": rating,
        "runtime_minutes": row.get("runtime_minutes"),
        "genres": [item.get("name") if isinstance(item, dict) else item for item in _json_value(row.get("genres_json"), [])],
        "tags": tags,
        "studios": studios,
        "season_number": (
            row.get("season_number") if row.get("season_number") is not None else season_number
        ) if requested_type in {"Season", "Episode"} else None,
        "episode_number": (
            row.get("episode_number") if row.get("episode_number") is not None else episode_number
        ) if requested_type == "Episode" else None,
        "actors_ready": bool(row.get("actors_ready")),
        "people": sorted(people, key=lambda item: item.get("order", 999)),
        "collections": collections,
        "_screenshot_hash": str(row.get("screenshot_hash") or ""),
        "_emby_item_ids": [
            str(value)
            for value in _json_value(row.get("emby_item_ids_json"), [])
            if value
        ],
        "images": {
            "primary": _tmdb_image_url(row.get("poster_path"), "original") or screenshot_url,
            "backdrop": _tmdb_image_url(row.get("backdrop_path"), "original") or screenshot_url,
            "logo": _tmdb_image_url(row.get("logo_path"), "original"),
            "thumb": _tmdb_image_url(row.get("thumb_path"), "original") or screenshot_url,
        },
    }


def _tmdb_image_url(path, size):
    if not path:
        return None
    path = str(path)
    if path.startswith(("http://", "https://", "etk-cache://")):
        return path
    return f"https://image.tmdb.org/t/p/{size}/{path.lstrip('/')}"
