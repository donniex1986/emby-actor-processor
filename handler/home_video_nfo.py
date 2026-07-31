import xml.etree.ElementTree as ET
from datetime import datetime
import os


def _text(node, tag):
    child = node.find(tag)
    return str(child.text or '').strip() if child is not None else ''


def _texts(node, tag):
    return [str(child.text or '').strip() for child in node.findall(tag) if str(child.text or '').strip()]


def _number(value, *, integer=False):
    try:
        return int(float(value)) if integer else float(value)
    except (TypeError, ValueError):
        return None


def _date(value):
    value = str(value or '').strip()
    if not value:
        return None
    normalized = value.replace(' ', 'T')
    if len(normalized) == 10:
        normalized += 'T00:00:00'
    try:
        datetime.fromisoformat(normalized.replace('Z', '+00:00'))
    except ValueError:
        return value
    return normalized


def match_nfo_to_strm_paths(nfo_path, strm_paths):
    """Return normalized STRM paths associated with a Kodi NFO path."""
    normalize = lambda value: str(value or '').strip().replace('\\', '/').rstrip('/').casefold()
    nfo_path = normalize(nfo_path)
    candidates = [normalize(path) for path in strm_paths or [] if normalize(path)]
    nfo_dir, _, nfo_name = nfo_path.rpartition('/')
    if nfo_name == 'tvshow.nfo':
        return [path for path in candidates if path.startswith(nfo_dir + '/')]
    if nfo_name == 'movie.nfo':
        return [path for path in candidates if path.rpartition('/')[0] == nfo_dir]
    stem = os.path.splitext(nfo_name)[0]
    return [
        path for path in candidates
        if path.rpartition('/')[0] == nfo_dir
        and os.path.splitext(path.rpartition('/')[2])[0] == stem
    ]


def parse_home_video_nfo(xml_data):
    """Parse common Kodi/Emby NFO fields without external lookups."""
    if isinstance(xml_data, bytes):
        try:
            root = ET.fromstring(xml_data)
        except (ET.ParseError, ValueError):
            for encoding in ('utf-8-sig', 'gb18030'):
                try:
                    root = ET.fromstring(xml_data.decode(encoding))
                    break
                except (UnicodeDecodeError, ET.ParseError):
                    continue
            else:
                raise ET.ParseError('无法识别 NFO XML 编码或结构')
    else:
        root = ET.fromstring(str(xml_data or '').strip())

    provider_ids = {}
    for unique_id in root.findall('uniqueid'):
        provider = str(unique_id.get('type') or '').strip().lower()
        value = str(unique_id.text or '').strip()
        if provider and value:
            provider_ids[provider] = value
    for tag, provider in (('tmdbid', 'tmdb'), ('imdbid', 'imdb'), ('tvdbid', 'tvdb')):
        value = _text(root, tag)
        if value:
            provider_ids.setdefault(provider, value)

    people = []
    for actor in root.findall('actor'):
        name = _text(actor, 'name')
        if not name:
            continue
        person = {
            'Name': name,
            'Role': _text(actor, 'role'),
            'Type': _text(actor, 'type') or 'Actor',
        }
        people.append({key: value for key, value in person.items() if value})
    for tag, person_type in (('director', 'Director'), ('writer', 'Writer'), ('credits', 'Writer')):
        for name in _texts(root, tag):
            if not any(p.get('Name') == name and p.get('Type') == person_type for p in people):
                people.append({'Name': name, 'Type': person_type})

    runtime_minutes = _number(_text(root, 'runtime'), integer=True)
    images = []
    for thumb in root.findall('thumb'):
        reference = str(thumb.text or '').strip()
        if not reference:
            continue
        aspect = str(thumb.get('aspect') or '').strip().lower()
        image_type = {
            'banner': 'Banner', 'landscape': 'Thumb', 'thumb': 'Thumb',
            'clearlogo': 'Logo', 'logo': 'Logo', 'disc': 'Disc',
            'discart': 'Disc',
        }.get(aspect, 'Primary')
        images.append({'type': image_type, 'reference': reference})
    fanart = root.find('fanart')
    if fanart is not None:
        for thumb in fanart.findall('thumb'):
            reference = str(thumb.text or '').strip()
            if reference:
                images.append({'type': 'Backdrop', 'reference': reference})

    return {
        'root_type': str(root.tag or '').strip().lower(),
        'Name': _text(root, 'title'),
        'OriginalTitle': _text(root, 'originaltitle'),
        'SortName': _text(root, 'sorttitle'),
        'Overview': _text(root, 'plot') or _text(root, 'outline'),
        'Taglines': _texts(root, 'tagline'),
        'ProductionYear': _number(_text(root, 'year'), integer=True),
        'PremiereDate': _date(_text(root, 'premiered') or _text(root, 'aired')),
        'DateCreated': _date(_text(root, 'dateadded')),
        'CommunityRating': _number(_text(root, 'rating')),
        'CriticRating': _number(_text(root, 'criticrating')),
        'OfficialRating': _text(root, 'mpaa'),
        'Genres': _texts(root, 'genre'),
        'Tags': _texts(root, 'tag'),
        'Studios': _texts(root, 'studio'),
        'ProductionLocations': _texts(root, 'country'),
        'People': people,
        'ProviderIds': provider_ids,
        'ParentIndexNumber': _number(_text(root, 'season') or _text(root, 'seasonnumber'), integer=True),
        'IndexNumber': _number(_text(root, 'episode') or _text(root, 'episodenumber'), integer=True),
        'RunTimeTicks': runtime_minutes * 60 * 10_000_000 if runtime_minutes else None,
        'images': images,
    }


def build_missing_emby_metadata(current, parsed):
    """Build an Emby update containing only fields missing from the current item."""
    current = current or {}
    parsed = parsed or {}
    update = {}

    def missing(value):
        return value is None or value == '' or value == [] or value == {}

    for field in (
        'Name', 'OriginalTitle', 'SortName', 'Overview', 'Taglines',
        'ProductionYear', 'PremiereDate', 'DateCreated', 'CommunityRating',
        'CriticRating', 'OfficialRating', 'Genres', 'Tags',
        'ProductionLocations', 'People', 'ParentIndexNumber', 'IndexNumber',
        'RunTimeTicks',
    ):
        value = parsed.get(field)
        if not missing(value) and missing(current.get(field)):
            update[field] = value

    studios = parsed.get('Studios') or []
    if studios and missing(current.get('Studios')):
        update['Studios'] = [{'Name': name} for name in studios]

    provider_ids = dict(current.get('ProviderIds') or {})
    provider_key = {'tmdb': 'Tmdb', 'imdb': 'Imdb', 'tvdb': 'Tvdb'}
    changed = False
    for provider, value in (parsed.get('ProviderIds') or {}).items():
        key = provider_key.get(str(provider).lower(), str(provider))
        if value and not provider_ids.get(key):
            provider_ids[key] = value
            changed = True
    if changed:
        update['ProviderIds'] = provider_ids

    return update
