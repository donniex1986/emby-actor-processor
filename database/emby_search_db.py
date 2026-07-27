import re
import unicodedata
from typing import Iterable, Optional

from psycopg2.extras import execute_values

try:
    from pypinyin import Style, pinyin
except ImportError:
    class Style:
        NORMAL = "normal"
        FIRST_LETTER = "first_letter"

    def pinyin(value, style=None, strict=False, errors="ignore", heteronym=False):
        return [[character] for character in str(value or "")]

from database.connection import get_db_connection


_COMPACT_RE = re.compile(r"[^0-9a-z\u3400-\u9fff]+", re.IGNORECASE)
_SEARCH_PART_RE = re.compile(r"[0-9a-z\u3400-\u9fff]+", re.IGNORECASE)
_PINYIN_INITIAL_OVERRIDES = {
    "甄嬛传": "zhz",
}


def _normalize(value) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).casefold().strip()


def _compact(value) -> str:
    return _COMPACT_RE.sub("", _normalize(value))[:512]


def _pinyin(value: str, style: Style) -> str:
    values = []
    for part in pinyin(value, style=style, strict=False, errors="default"):
        if not part:
            continue
        text = str(part[0] or "")
        values.append(text[:1] if style == Style.FIRST_LETTER else text)
    return "".join(values).casefold()[:512]


def _pinyin_variants(value: str, style: Style) -> list[str]:
    variants = [_pinyin(value, style)]
    if style == Style.FIRST_LETTER:
        override = _PINYIN_INITIAL_OVERRIDES.get(value)
        if override and override not in variants:
            variants.append(override)
    return variants


def _ngrams(value: str) -> list[str]:
    value = _compact(value)
    if not value:
        return []
    grams = set()
    for size in range(1, min(3, len(value)) + 1):
        grams.update(value[index:index + size] for index in range(len(value) - size + 1))
    return sorted(grams)


def _prepare_item(item: dict) -> Optional[tuple]:
    try:
        item_id = int(item.get("id"))
    except (TypeError, ValueError):
        return None
    item_type = str(item.get("type") or "").strip()
    title = str(item.get("title") or "").strip()
    if item_id <= 0 or not item_type or not title:
        return None
    original_title = str(item.get("original_title") or "").strip()
    series_name = str(item.get("series_name") or "").strip()
    search_compact = _compact(" ".join((title, original_title, series_name)))
    pinyin_full = _pinyin(search_compact, Style.NORMAL)
    initial_variants = []
    for source in dict.fromkeys(
        value for value in (title, original_title, series_name) if value
    ):
        initial_sources = [_compact(source)]
        initial_sources.extend(
            _compact(part) for part in _SEARCH_PART_RE.findall(_normalize(source))
        )
        for initial_source in dict.fromkeys(value for value in initial_sources if value):
            for variant in _pinyin_variants(initial_source, Style.FIRST_LETTER):
                if variant not in initial_variants:
                    initial_variants.append(variant)
    pinyin_initials = "|".join(initial_variants)
    return (
        item_id,
        item_type,
        title,
        original_title,
        series_name,
        search_compact,
        pinyin_full,
        pinyin_initials,
        _ngrams(search_compact),
        sorted(set(
            _ngrams(pinyin_full)
            + [gram for variant in initial_variants for gram in _ngrams(variant)]
        )),
    )


def start_rebuild(token: str) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM emby_search_rebuild_state WHERE started_at < NOW() - INTERVAL '30 minutes'"
            )
            cursor.execute(
                """
                INSERT INTO emby_search_rebuild_state(token, started_at)
                VALUES (%s, NOW())
                ON CONFLICT (token) DO UPDATE SET started_at = NOW()
                """,
                (token,),
            )
        conn.commit()


def upsert_items(items: Iterable[dict]) -> int:
    rows = [row for row in (_prepare_item(item) for item in items) if row is not None]
    if not rows:
        return 0
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO emby_search_index (
                    emby_item_id, item_type, title, original_title, series_name,
                    search_compact, pinyin_full, pinyin_initials,
                    search_ngrams, pinyin_ngrams
                ) VALUES %s
                ON CONFLICT (emby_item_id) DO UPDATE SET
                    item_type = EXCLUDED.item_type,
                    title = EXCLUDED.title,
                    original_title = EXCLUDED.original_title,
                    series_name = EXCLUDED.series_name,
                    search_compact = EXCLUDED.search_compact,
                    pinyin_full = EXCLUDED.pinyin_full,
                    pinyin_initials = EXCLUDED.pinyin_initials,
                    search_ngrams = EXCLUDED.search_ngrams,
                    pinyin_ngrams = EXCLUDED.pinyin_ngrams,
                    updated_at = NOW()
                """,
                rows,
                page_size=500,
            )
        conn.commit()
    return len(rows)


def complete_rebuild(token: str) -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT started_at FROM emby_search_rebuild_state WHERE token = %s FOR UPDATE",
                (token,),
            )
            state = cursor.fetchone()
            if not state:
                return 0
            cursor.execute(
                "DELETE FROM emby_search_index WHERE updated_at < %s",
                (state["started_at"],),
            )
            deleted = cursor.rowcount
            cursor.execute("DELETE FROM emby_search_rebuild_state WHERE token = %s", (token,))
        conn.commit()
    return deleted


def abort_rebuild(token: str) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM emby_search_rebuild_state WHERE token = %s", (token,))
        conn.commit()


def delete_items(item_ids: Iterable) -> int:
    ids = []
    for value in item_ids:
        try:
            item_id = int(value)
        except (TypeError, ValueError):
            continue
        if item_id > 0 and item_id not in ids:
            ids.append(item_id)
    if not ids:
        return 0
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM emby_search_index WHERE emby_item_id = ANY(%s)",
                (ids,),
            )
            deleted = cursor.rowcount
        conn.commit()
    return deleted


def _hide_series_children(rows: list[dict]) -> list[dict]:
    if not any(row.get("item_type") == "Series" for row in rows):
        return rows
    return [
        row
        for row in rows
        if row.get("item_type") not in {"Season", "Episode"}
    ]


def search(query: str, item_types: Optional[list[str]] = None, limit: int = 300) -> dict:
    compact = _compact(query)
    if not compact:
        return {"ready": True, "items": []}
    pinyin_enabled = re.search(r"[\u3400-\u9fff]", compact) is None
    full = _pinyin(compact, Style.NORMAL) or compact
    initials = compact
    search_grams = _ngrams(compact)
    full_grams = _ngrams(full)
    initial_grams = _ngrams(initials)
    item_types = [value.strip() for value in (item_types or []) if value.strip()]
    limit = max(1, min(int(limit or 300), 500))

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS(SELECT 1 FROM emby_search_index LIMIT 1) AS populated,
                       EXISTS(
                           SELECT 1 FROM emby_search_rebuild_state
                           WHERE started_at >= NOW() - INTERVAL '30 minutes'
                       ) AS rebuilding,
                       EXISTS(
                           SELECT 1
                           FROM emby_search_index
                           WHERE updated_at < (
                               SELECT MIN(started_at)
                               FROM emby_search_rebuild_state
                               WHERE started_at >= NOW() - INTERVAL '30 minutes'
                           )
                       ) AS has_previous_index
                """
            )
            state = cursor.fetchone()
            if not state["populated"] or (
                state["rebuilding"] and not state["has_previous_index"]
            ):
                return {"ready": False, "items": []}
            cursor.execute(
                """
                SELECT emby_item_id, item_type, title, series_name,
                       CASE
                           WHEN item_type = 'Movie' THEN 0
                           WHEN item_type = 'Series' THEN 1
                           WHEN item_type = 'BoxSet' THEN 2
                           WHEN item_type IN ('MusicAlbum', 'Audio', 'MusicVideo') THEN 3
                           WHEN item_type = 'Person' THEN 4
                           WHEN item_type = 'MusicArtist' THEN 5
                           ELSE 6
                       END AS type_priority,
                       CASE
                           WHEN search_compact = %(compact)s THEN 1000
                           WHEN search_compact LIKE %(compact)s || '%%' THEN 900
                           WHEN POSITION(%(compact)s IN search_compact) > 0 THEN 800
                           WHEN %(pinyin_enabled)s AND pinyin_full = %(full)s THEN 700
                           WHEN %(pinyin_enabled)s AND pinyin_full LIKE %(full)s || '%%' THEN 600
                           WHEN %(pinyin_enabled)s AND POSITION(%(full)s IN pinyin_full) > 0 THEN 500
                           WHEN %(pinyin_enabled)s AND %(initials)s = ANY(
                               string_to_array(pinyin_initials, '|')
                           ) THEN 450
                           WHEN %(pinyin_enabled)s AND EXISTS (
                               SELECT 1
                               FROM unnest(string_to_array(pinyin_initials, '|')) AS variant
                               WHERE POSITION(%(initials)s IN variant) > 0
                           ) THEN 400
                           ELSE 300
                       END - CHAR_LENGTH(search_compact) AS rank
                FROM emby_search_index
                WHERE (%(item_types)s::text[] IS NULL OR item_type = ANY(%(item_types)s::text[]))
                  AND (
                      search_ngrams @> %(search_grams)s::text[]
                      OR (
                          %(pinyin_enabled)s
                          AND (
                              pinyin_ngrams @> %(full_grams)s::text[]
                              OR pinyin_ngrams @> %(initial_grams)s::text[]
                          )
                      )
                  )
                  AND (
                      POSITION(%(compact)s IN search_compact) > 0
                      OR (
                          %(pinyin_enabled)s
                          AND (
                              POSITION(%(full)s IN pinyin_full) > 0
                              OR EXISTS (
                                  SELECT 1
                                  FROM unnest(string_to_array(pinyin_initials, '|')) AS variant
                                  WHERE POSITION(%(initials)s IN variant) > 0
                              )
                          )
                      )
                  )
                ORDER BY type_priority ASC, rank DESC, title ASC, emby_item_id ASC
                LIMIT %(query_limit)s
                """,
                {
                    "compact": compact,
                    "pinyin_enabled": pinyin_enabled,
                    "full": full,
                    "initials": initials,
                    "item_types": item_types or None,
                    "search_grams": search_grams,
                    "full_grams": full_grams,
                    "initial_grams": initial_grams,
                    "query_limit": min(limit * 2, 500),
                },
            )
            rows = _hide_series_children(cursor.fetchall())[:limit]
    return {
        "ready": True,
        "items": [
            {
                "id": int(row["emby_item_id"]),
                "type": row["item_type"],
                "title": row["title"],
                "rank": int(row["rank"]),
            }
            for row in rows
        ],
    }
