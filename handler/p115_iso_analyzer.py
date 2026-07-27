import json
import logging
import os
import re
import struct
import time

import requests

logger = logging.getLogger(__name__)


_ALIPAY_MINI_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 "
    "AlipayDefined(nt:WIFI,ws:390|844|3.0) AliApp(AP/10.5.33.8143) "
    "AlipayClient/10.5.33.8143 Language/zh-Hans Region/CN"
)

_WEB_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _u16(data, offset=0):
    return struct.unpack_from("<H", data, offset)[0]


def _u32(data, offset=0):
    return struct.unpack_from("<I", data, offset)[0]


def _u64(data, offset=0):
    return struct.unpack_from("<Q", data, offset)[0]


def _be16(data, offset=0):
    return struct.unpack_from(">H", data, offset)[0]


def _be32(data, offset=0):
    return struct.unpack_from(">I", data, offset)[0]


def _node_value(file_node, *keys):
    for key in keys:
        if isinstance(file_node, dict):
            value = file_node.get(key)
        else:
            value = getattr(file_node, key, None)
        if value not in (None, "", [], {}):
            return value
    return None


def _file_name(file_node, fallback="unknown.iso"):
    return str(
        _node_value(file_node, "fn", "n", "file_name", "name", "original_name")
        or fallback
        or "unknown.iso"
    )


def _file_size(file_node):
    try:
        return int(_node_value(file_node, "fs", "size", "file_size") or 0)
    except Exception:
        return 0


def _pick_code(file_node):
    value = _node_value(file_node, "pc", "pick_code", "pickcode")
    return str(value or "").strip()


def _extract_down_url(resp):
    if not resp:
        return ""
    if isinstance(resp, str):
        return resp if resp.startswith(("http://", "https://")) else ""
    try:
        text = str(resp)
        if text.startswith(("http://", "https://")):
            return text
    except Exception:
        pass
    if not isinstance(resp, dict):
        return ""
    for key in ("url", "download_url", "downurl", "direct_url"):
        value = resp.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value
        nested = _extract_down_url(value)
        if nested:
            return nested
    data = resp.get("data")
    if isinstance(data, dict):
        for value in data.values():
            nested = _extract_down_url(value)
            if nested:
                return nested
        return _extract_down_url(data)
    if isinstance(data, list):
        for value in data:
            nested = _extract_down_url(value)
            if nested:
                return nested
    return ""


def _get_direct_url(client, pick_code):
    candidates = []
    seen = set()
    for ua in (_WEB_UA, _ALIPAY_MINI_UA):
        if ua and ua not in seen:
            candidates.append(ua)
            seen.add(ua)

    methods = ("download_url", "openapi_downurl")
    last_error = None
    for method_name in methods:
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        for ua in candidates:
            try:
                try:
                    resp = method(pick_code, user_agent=ua)
                except TypeError:
                    resp = method(pick_code)
                url = _extract_down_url(resp)
                if url:
                    return url, ua, method_name
            except Exception as exc:
                last_error = exc
    if last_error:
        raise RuntimeError(f"获取 115 直链失败: {last_error}")
    raise RuntimeError("获取 115 直链失败")


class _RangeReader:
    def __init__(self, url, user_agent, timeout=60):
        self.url = url
        self.user_agent = user_agent
        self.timeout = timeout
        self.intervals = []
        self.requests = 0
        self.bytes = 0

    def read(self, start, length):
        if length <= 0:
            return b""
        end = int(start) + int(length)
        start = int(start)
        for cached_start, cached_end, data in self.intervals:
            if start >= cached_start and end <= cached_end:
                return data[start - cached_start:end - cached_start]

        resp = requests.get(
            self.url,
            headers={
                "Range": f"bytes={start}-{end - 1}",
                "User-Agent": self.user_agent,
                "Accept": "*/*",
                "Connection": "close",
            },
            timeout=self.timeout,
            allow_redirects=True,
        )
        self.requests += 1
        content = resp.content or b""
        if resp.status_code != 206 or len(content) != length:
            raise RuntimeError(
                f"Range 读取失败: HTTP={resp.status_code}, got={len(content)}, expected={length}"
            )
        self.bytes += len(content)
        self.intervals.append((start, end, content))
        return content

    def prefetch(self, ranges, max_gap=262144, max_chunk=16 * 1024 * 1024):
        clean = []
        for start, length in ranges:
            if length:
                clean.append((int(start), int(start) + int(length)))
        if not clean:
            return {"merged": 0, "bytes": 0}
        clean.sort()

        merged = []
        cur_start, cur_end = clean[0]
        for start, end in clean[1:]:
            if start <= cur_end + max_gap and end - cur_start <= max_chunk:
                cur_end = max(cur_end, end)
            else:
                merged.append((cur_start, cur_end))
                cur_start, cur_end = start, end
        merged.append((cur_start, cur_end))

        total = 0
        for start, end in merged:
            self.read(start, end - start)
            total += end - start
        return {"merged": len(merged), "bytes": total}


class _MappedRangeReader:
    def __init__(self, reader, ranges):
        self.reader = reader
        self.ranges = [(int(start), int(length)) for start, length in ranges if length]
        self.size = sum(length for _, length in self.ranges)

    @property
    def requests(self):
        return self.reader.requests

    @property
    def bytes(self):
        return self.reader.bytes

    def _map(self, start, length):
        start = int(start)
        length = int(length)
        if length <= 0:
            return []
        if start < 0 or start + length > self.size:
            raise RuntimeError("Nested ISO range is out of bounds")

        remaining = length
        cursor = 0
        mapped = []
        for physical_start, span in self.ranges:
            if start >= cursor + span:
                cursor += span
                continue
            local_start = max(0, start - cursor)
            take = min(span - local_start, remaining)
            mapped.append((physical_start + local_start, take))
            start += take
            remaining -= take
            if remaining <= 0:
                break
            cursor += span
        return mapped

    def read(self, start, length):
        return b"".join(self.reader.read(offset, size) for offset, size in self._map(start, length))

    def prefetch(self, ranges, **kwargs):
        mapped = []
        for start, length in ranges:
            mapped.extend(self._map(start, length))
        return self.reader.prefetch(mapped, **kwargs)


class _UdfIsoReader:
    def __init__(self, url=None, user_agent=None, reader=None):
        self.reader = reader or _RangeReader(url, user_agent)
        self.part_start = None
        self.meta_blob = b""
        self._init_udf()

    @property
    def requests(self):
        return self.reader.requests

    @property
    def bytes(self):
        return self.reader.bytes

    def read(self, start, length):
        return self.reader.read(start, length)

    def sector(self, number, count=1):
        return self.read(number * 2048, count * 2048)

    def physical_offset(self, lbn):
        return (self.part_start + int(lbn)) * 2048

    def _init_udf(self):
        avdp = self.sector(256)
        if _u16(avdp, 0) != 2:
            raise RuntimeError("未找到 UDF AVDP")

        main_len = _u32(avdp, 16)
        main_loc = _u32(avdp, 20)
        descriptors = self.read(main_loc * 2048, main_len)

        logical_volume = None
        for offset in range(0, len(descriptors), 2048):
            desc = descriptors[offset:offset + 2048]
            tag_id = _u16(desc, 0)
            if tag_id == 5:
                self.part_start = _u32(desc, 188)
            elif tag_id == 6:
                logical_volume = desc
            elif tag_id == 8:
                break

        if self.part_start is None or logical_volume is None:
            raise RuntimeError("未找到 UDF 分区信息")

        self.fsd_len = _u32(logical_volume, 248) & 0x3fffffff
        self.fsd_lbn = _u32(logical_volume, 252)
        self.fsd_part = _u16(logical_volume, 256)

        part_map_len = _u32(logical_volume, 264)
        part_map_count = _u32(logical_volume, 268)
        pos = 440
        end = pos + part_map_len
        metadata_file_lbn = None
        has_physical_partition = False
        for _ in range(part_map_count):
            if pos + 2 > end:
                break
            map_type = logical_volume[pos]
            map_len = logical_volume[pos + 1]
            raw = logical_volume[pos:pos + map_len]
            if map_type == 1 and map_len >= 6:
                has_physical_partition = True
            elif map_type == 2 and map_len >= 44:
                ident = raw[5:28].rstrip(b"\0").decode("latin1", "ignore")
                if "Metadata Partition" in ident:
                    metadata_file_lbn = _u32(raw, 40)
                    break
            pos += map_len

        if metadata_file_lbn is None:
            if has_physical_partition and self.fsd_part == 0:
                return
            raise RuntimeError("未找到可用的 UDF 分区映射")

        metadata_fe = self.read(self.physical_offset(metadata_file_lbn), 2048)
        chunks = []
        for ad in self._allocation_descriptors(metadata_fe):
            if ad.get("len"):
                chunks.append(self.read(self.physical_offset(ad["lbn"]), ad["len"]))
        if not chunks:
            raise RuntimeError("UDF Metadata File 没有数据区")
        self.meta_blob = b"".join(chunks)

    def _file_entry_fields(self, entry):
        tag_id = _u16(entry, 0)
        flags = _u16(entry, 34)
        allocation_type = flags & 7
        if tag_id == 261:
            lea = _u32(entry, 168)
            return {
                "tag": tag_id,
                "atype": allocation_type,
                "file_type": entry[27],
                "info_len": _u64(entry, 56),
                "lad": _u32(entry, 172),
                "ad_start": 176 + lea,
            }
        if tag_id == 266:
            lea = _u32(entry, 208)
            return {
                "tag": tag_id,
                "atype": allocation_type,
                "file_type": entry[27],
                "info_len": _u64(entry, 56),
                "lad": _u32(entry, 212),
                "ad_start": 216 + lea,
            }
        return {
            "tag": tag_id,
            "atype": allocation_type,
            "file_type": None,
            "info_len": 0,
            "lad": 0,
            "ad_start": 0,
        }

    def _allocation_descriptors(self, entry):
        fields = self._file_entry_fields(entry)
        pos = fields["ad_start"]
        end = pos + fields["lad"]
        allocation_type = fields["atype"]
        out = []
        if allocation_type == 3:
            return [{"embedded": True, "len": fields["lad"], "data_start": pos}]
        while pos < end:
            if allocation_type == 0:
                if pos + 8 > end:
                    break
                raw_len = _u32(entry, pos)
                lbn = _u32(entry, pos + 4)
                pos += 8
                if raw_len or lbn:
                    out.append({"type": raw_len >> 30, "len": raw_len & 0x3fffffff, "lbn": lbn, "part": None})
            elif allocation_type == 1:
                if pos + 16 > end:
                    break
                raw_len = _u32(entry, pos)
                lbn = _u32(entry, pos + 4)
                part = _u16(entry, pos + 8)
                pos += 16
                if raw_len or lbn:
                    out.append({"type": raw_len >> 30, "len": raw_len & 0x3fffffff, "lbn": lbn, "part": part})
            else:
                break
        return out

    def read_partition(self, part, lbn, length=2048):
        if part == 0:
            return self.read(self.physical_offset(lbn), length)
        start = int(lbn) * 2048
        return self.meta_blob[start:start + int(length)]

    def read_file_entry(self, entry):
        return self.read_partition(entry["part"], entry["lbn"], max(2048, entry["len"]))

    def entry_info(self, entry):
        fe = self.read_file_entry(entry)
        return self._file_entry_fields(fe), self._allocation_descriptors(fe), fe

    def physical_ranges_for_file(self, entry):
        _, ads, _ = self.entry_info(entry)
        ranges = []
        for ad in ads:
            part = entry["part"] if ad.get("part") is None else ad["part"]
            if part == 0 and ad.get("len"):
                ranges.append((self.physical_offset(ad["lbn"]), ad["len"]))
        return ranges

    def prefetch_files(self, entries, **kwargs):
        ranges = []
        for entry in entries:
            ranges.extend(self.physical_ranges_for_file(entry))
        return self.reader.prefetch(ranges, **kwargs)

    def prefetch_file_entries(self, entries, **kwargs):
        ranges = []
        for entry in entries:
            if entry.get("part") == 0:
                ranges.append((self.physical_offset(entry["lbn"]), max(2048, int(entry.get("len") or 0))))
        return self.reader.prefetch(ranges, **kwargs)

    def file_data(self, file_entry, current_part):
        fields = self._file_entry_fields(file_entry)
        ads = self._allocation_descriptors(file_entry)
        if fields["atype"] == 3:
            start = ads[0]["data_start"]
            return file_entry[start:start + min(fields["info_len"], ads[0]["len"])]

        data = b""
        for ad in ads:
            part = current_part if ad.get("part") is None else ad["part"]
            if ad.get("len"):
                data += self.read_partition(part, ad["lbn"], ad["len"])
        return data[:fields["info_len"]]

    def _fid_name(self, data, pos):
        name_len = data[pos + 19]
        name_start = pos + 38 + _u16(data, pos + 36)
        raw = data[name_start:name_start + name_len]
        if not raw:
            return ""
        compression_id = raw[0]
        payload = raw[1:]
        if compression_id == 8:
            return payload.decode("latin1", "ignore")
        if compression_id == 16:
            return payload.decode("utf-16-be", "ignore")
        return payload.decode("utf-8", "ignore") or payload.decode("latin1", "ignore")

    def _parse_dir(self, data):
        pos = 0
        out = []
        while pos + 38 <= len(data):
            tag_id = _u16(data, pos)
            if tag_id == 0 and not data[pos:pos + 64].strip(b"\0"):
                break
            if tag_id != 257:
                break
            name_len = data[pos + 19]
            impl_use_len = _u16(data, pos + 36)
            record_len = (38 + impl_use_len + name_len + 3) & ~3
            name = self._fid_name(data, pos)
            if name:
                out.append({
                    "name": name,
                    "flags": data[pos + 18],
                    "len": _u32(data, pos + 20) & 0x3fffffff,
                    "lbn": _u32(data, pos + 24),
                    "part": _u16(data, pos + 28),
                })
            pos += record_len
        return out

    def list_dir(self, entry):
        fe = self.read_file_entry(entry)
        return self._parse_dir(self.file_data(fe, entry["part"]))

    def find(self, entry, name):
        target = str(name or "").upper()
        for child in self.list_dir(entry):
            if child["name"].upper() == target:
                return child
        return None

    def root(self):
        fsd = self.read_partition(self.fsd_part, self.fsd_lbn, max(2048, self.fsd_len))
        return {
            "name": "/",
            "len": _u32(fsd, 400) & 0x3fffffff,
            "lbn": _u32(fsd, 404),
            "part": _u16(fsd, 408),
        }


def _parse_mpls(data, stream_sizes):
    if not data.startswith(b"MPLS") or len(data) < 64:
        return None
    playlist_start = _be32(data, 8)
    if playlist_start <= 0 or playlist_start + 10 > len(data):
        return None

    item_count = _be16(data, playlist_start + 6)
    pos = playlist_start + 10
    clips = []
    duration = 0.0
    for _ in range(item_count):
        if pos + 22 > len(data):
            break
        item_len = _be16(data, pos)
        item_end = pos + 2 + item_len
        if item_len < 20 or item_end > len(data):
            break
        clip = data[pos + 2:pos + 7].decode("ascii", "ignore") + ".m2ts"
        in_time = _be32(data, pos + 14)
        out_time = _be32(data, pos + 18)
        seconds = max(0, out_time - in_time) / 45000.0
        duration += seconds
        clips.append({"clip": clip, "seconds": seconds})
        pos = item_end

    if not clips:
        return None
    unique = {item["clip"] for item in clips}
    return {
        "items": len(clips),
        "duration": duration,
        "unique_clip_count": len(unique),
        "unique_size": sum(stream_sizes.get(name, 0) for name in unique),
        "max_clip_size": max((stream_sizes.get(name, 0) for name in unique), default=0),
        "clips": clips,
    }


def _find_bdmv_dir(iso, root, max_wrapper_depth=4):
    """Find the most complete BDMV below a limited set of wrapper directories."""
    queue = [(root, [])]
    candidates = []
    seen = set()
    while queue:
        current, path = queue.pop(0)
        key = (current.get("part"), current.get("lbn"))
        if key in seen:
            continue
        seen.add(key)

        for child in iso.list_dir(current):
            if not (child.get("flags", 0) & 2):
                continue
            child_path = path + [child["name"]]
            if child["name"].upper() == "BDMV":
                counts = []
                for name in ("PLAYLIST", "STREAM", "CLIPINF"):
                    directory = iso.find(child, name)
                    counts.append(len(iso.list_dir(directory)) if directory else -1)
                score = (
                    sum(count > 0 for count in counts),
                    sum(count >= 0 for count in counts),
                    sum(max(0, count) for count in counts),
                    -len(child_path),
                )
                candidates.append((score, child, child_path))
            elif len(path) < max_wrapper_depth:
                queue.append((child, child_path))

    if not candidates:
        return None, []
    _, bdmv, path = max(candidates, key=lambda item: item[0])
    return bdmv, path


def _open_single_nested_iso(iso, root):
    candidates = [
        entry for entry in iso.list_dir(root)
        if not (entry.get("flags", 0) & 2) and entry["name"].lower().endswith(".iso")
    ]
    if len(candidates) != 1:
        return None, ""

    entry = candidates[0]
    fields, _, _ = iso.entry_info(entry)
    ranges = iso.physical_ranges_for_file(entry)
    if not ranges or sum(length for _, length in ranges) < fields["info_len"]:
        return None, ""
    return _UdfIsoReader(reader=_MappedRangeReader(iso.reader, ranges)), entry["name"]


def _read_file_slice(iso, ranges, offset, length):
    offset = max(0, int(offset or 0))
    remaining = max(0, int(length or 0))
    cursor = 0
    chunks = []
    for physical_start, span in ranges:
        span = int(span or 0)
        if remaining <= 0:
            break
        if offset >= cursor + span:
            cursor += span
            continue

        local_start = max(0, offset - cursor)
        take = min(span - local_start, remaining)
        if take > 0:
            chunks.append(iso.read(int(physical_start) + local_start, take))
            remaining -= take
            offset += take
        cursor += span
    return b"".join(chunks)


def _m2ts_pcr_values(data, logical_offset):
    by_pid = {}
    packet_size = 192
    first_packet = (-int(logical_offset or 0)) % packet_size
    for offset in range(first_packet, len(data) - packet_size + 1, packet_size):
        packet = data[offset + 4:offset + packet_size]
        if len(packet) != 188 or packet[0] != 0x47:
            continue
        if not (packet[3] & 0x20):
            continue
        adaptation_length = packet[4]
        if adaptation_length < 7 or adaptation_length > 183 or not (packet[5] & 0x10):
            continue

        pid = ((packet[1] & 0x1f) << 8) | packet[2]
        base = (
            (packet[6] << 25)
            | (packet[7] << 17)
            | (packet[8] << 9)
            | (packet[9] << 1)
            | (packet[10] >> 7)
        )
        extension = ((packet[10] & 1) << 8) | packet[11]
        by_pid.setdefault(pid, []).append(base * 300 + extension)
    return by_pid


def _m2ts_duration_from_pcr(iso, ranges, file_size, sample_size=8 * 1024 * 1024):
    if not ranges or file_size <= 0:
        return 0.0
    sample_size = min(int(sample_size), int(file_size))
    tail_offset = max(0, int(file_size) - sample_size)
    head = _read_file_slice(iso, ranges, 0, sample_size)
    tail = _read_file_slice(iso, ranges, tail_offset, sample_size)
    head_values = _m2ts_pcr_values(head, 0)
    tail_values = _m2ts_pcr_values(tail, tail_offset)

    common_pids = set(head_values) & set(tail_values)
    if not common_pids:
        return 0.0
    pid = max(common_pids, key=lambda key: len(head_values[key]) + len(tail_values[key]))
    first = head_values[pid][0]
    last = tail_values[pid][-1]
    wrap = (1 << 33) * 300
    if last < first:
        last += wrap
    duration = (last - first) / 27000000.0
    return duration if 0 < duration < 24 * 60 * 60 else 0.0


def _probe_m2ts_sample(iso, entry, file_size):
    import shutil
    import subprocess

    if not shutil.which("ffprobe"):
        raise RuntimeError("ffprobe 不可用，无法采样 M2TS")

    ranges = iso.physical_ranges_for_file(entry)
    if not ranges:
        raise RuntimeError("主 M2TS 没有可读取的物理区段")
    probe_size = min(int(file_size), 16 * 1024 * 1024)
    sample = _read_file_slice(iso, ranges, 0, probe_size)
    if not sample:
        raise RuntimeError("主 M2TS 采样为空")

    cmd = [
        "ffprobe", "-hide_banner", "-v", "error", "-f", "mpegts",
        "-analyzeduration", "20000000", "-probesize", str(probe_size),
        "-print_format", "json", "-show_streams", "pipe:0",
    ]
    proc = subprocess.run(
        cmd,
        input=sample,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    if proc.returncode != 0:
        error = (proc.stderr or b"").decode("utf-8", "replace").strip()
        raise RuntimeError(f"M2TS 采样探测失败: {error[:300]}")

    probe_data = json.loads(proc.stdout or b"{}")
    streams = probe_data.get("streams") or []
    if not any(stream.get("codec_type") == "video" for stream in streams):
        raise RuntimeError("M2TS 采样未识别到视频流")
    duration = _m2ts_duration_from_pcr(iso, ranges, file_size)
    return streams, duration


def _audio_channels(format_rate, codec_name, file_name):
    text = str(file_name or "").lower()
    if re.search(r"(?<!\d)7[._ ]?1(?!\d)", text):
        if codec_name in {"truehd", "dts"}:
            return 8
    if re.search(r"(?<!\d)5[._ ]?1(?!\d)", text):
        return 6
    audio_format = (format_rate >> 4) & 0x0f
    if audio_format == 1:
        return 1
    if audio_format == 3:
        return 2
    if audio_format == 6:
        return 6
    if audio_format == 12:
        return 8
    return 0


def _sample_rate(format_rate):
    code = format_rate & 0x0f
    if code == 1:
        return "48000"
    if code == 4:
        return "96000"
    if code == 5:
        return "192000"
    return ""


def _codec_from_clpi(coding_type):
    video = {
        0x01: "mpeg1video",
        0x02: "mpeg2video",
        0x1B: "h264",
        0xEA: "vc1",
        0x24: "hevc",
    }
    audio = {
        0x80: ("pcm_bluray", ""),
        0x81: ("ac3", ""),
        0x82: ("dts", ""),
        0x83: ("truehd", "Dolby TrueHD"),
        0x84: ("eac3", ""),
        0x85: ("dts", "DTS-HD HRA"),
        0x86: ("dts", "DTS-HD MA"),
        0xA1: ("eac3", "Secondary Audio"),
        0xA2: ("dts", "Secondary Audio"),
    }
    subtitle = {
        0x90: "hdmv_pgs_subtitle",
        0x91: "hdmv_pgs_subtitle",
        0x92: "text",
    }
    if coding_type in video:
        return "video", video[coding_type], ""
    if coding_type in audio:
        codec, profile = audio[coding_type]
        return "audio", codec, profile
    if coding_type in subtitle:
        return "subtitle", subtitle[coding_type], ""
    return "other", f"0x{coding_type:02x}", ""


def _parse_clpi_streams(data, file_name):
    if not data.startswith(b"HDMV") or len(data) < 64:
        return []
    program_start = _be32(data, 12)
    if program_start <= 0 or program_start + 8 > len(data):
        return []
    program_len = _be32(data, program_start)
    program_end = min(len(data), program_start + 4 + program_len)

    def score(items):
        return sum(1 for item in items if item.get("codec_type") in {"video", "audio", "subtitle"})

    best = []
    for delta in range(10, 20):
        for mode in ("payload", "total"):
            pos = program_start + delta
            items = []
            while pos + 3 <= program_end and len(items) < 96:
                pid = _be16(data, pos)
                length = data[pos + 2]
                if length < 4:
                    break
                if mode == "payload":
                    raw = data[pos + 3:pos + 3 + length]
                    next_pos = pos + 3 + length
                else:
                    raw = data[pos + 3:pos + length]
                    next_pos = pos + length
                if next_pos > len(data) or not raw:
                    break

                kind, codec, profile = _codec_from_clpi(raw[0])
                item = {
                    "index": len(items),
                    "id": pid,
                    "codec_type": kind,
                    "codec_name": codec,
                    "profile": profile,
                    "tags": {},
                    "disposition": {},
                }
                if kind == "video" and len(raw) >= 4:
                    vf = (raw[1] >> 4) & 0x0f
                    fr = raw[1] & 0x0f
                    width, height = {
                        1: (720, 480),
                        2: (720, 576),
                        3: (720, 480),
                        4: (1920, 1080),
                        5: (1280, 720),
                        6: (1920, 1080),
                        7: (720, 576),
                        8: (3840, 2160),
                    }.get(vf, (0, 0))
                    frame_rate = {
                        1: "24000/1001",
                        2: "24/1",
                        3: "25/1",
                        4: "30000/1001",
                        6: "50/1",
                        7: "60000/1001",
                    }.get(fr, "0/0")
                    item.update({
                        "width": width,
                        "height": height,
                        "avg_frame_rate": frame_rate,
                        "r_frame_rate": frame_rate,
                        "display_aspect_ratio": "16:9" if (raw[2] >> 4) == 3 else "",
                    })
                    if codec == "hevc":
                        item["pix_fmt"] = "yuv420p10le"
                        item["bits_per_raw_sample"] = "10"
                    if width >= 3800:
                        item["color_space"] = "bt2020nc"
                        item["color_primaries"] = "bt2020"
                        item["color_transfer"] = "smpte2084"
                    name_upper = str(file_name or "").upper()
                    if re.search(r"(^|[ ._\\-])(DV|DOVI|DOLBY[ ._\\-]*VISION)([ ._\\-]|$)", name_upper):
                        item["side_data_list"] = [{
                            "side_data_type": "DOVI configuration record",
                            "dv_profile": 7,
                            "dv_bl_signal_compatibility_id": 6,
                        }]
                elif kind == "audio" and len(raw) >= 5:
                    lang = raw[2:5].decode("latin1", "ignore").strip("\0")
                    channels = _audio_channels(raw[1], codec, file_name)
                    item["tags"]["language"] = lang
                    item["channels"] = channels
                    item["sample_rate"] = _sample_rate(raw[1])
                    if channels:
                        item["channel_layout"] = "7.1" if channels == 8 else ("5.1" if channels == 6 else ("stereo" if channels == 2 else "mono"))
                elif kind == "subtitle" and len(raw) >= 4:
                    lang = raw[1:4].decode("latin1", "ignore").strip("\0")
                    item["tags"]["language"] = lang
                items.append(item)
                pos = next_pos

            if score(items) > score(best):
                best = items
    return [item for item in best if item.get("codec_type") in {"video", "audio", "subtitle"}]


def _detect_iso_effects(file_name, streams):
    text = str(file_name or "").upper()
    has_hdr = bool(re.search(r"(^|[ ._\\-])(HDR10\\+|HDR10|HDR)([ ._\\-]|$)", text))
    has_dv = bool(re.search(r"(^|[ ._\\-])(DV|DOVI|DOLBY[ ._\\-]*VISION)([ ._\\-]|$)", text))
    for stream in streams:
        if stream.get("codec_type") != "video":
            continue
        if has_hdr or stream.get("height", 0) >= 2160:
            stream.setdefault("color_space", "bt2020nc")
            stream.setdefault("color_primaries", "bt2020")
            stream.setdefault("color_transfer", "smpte2084")
        if has_dv:
            stream.setdefault("side_data_list", [{
                "side_data_type": "DOVI configuration record",
                "dv_profile": 7,
                "dv_bl_signal_compatibility_id": 6,
            }])


def probe_bluray_iso(client, file_node, sha1=None):
    name = _file_name(file_node, sha1 or "unknown.iso")
    if not name.lower().endswith(".iso"):
        return None
    pc = _pick_code(file_node)
    if not pc:
        return None

    started = time.perf_counter()
    url, ua, url_method = _get_direct_url(client, pc)
    iso = _UdfIsoReader(url, ua)

    root = iso.root()
    bdmv, bdmv_path = _find_bdmv_dir(iso, root)
    if not bdmv:
        nested_iso, nested_name = _open_single_nested_iso(iso, root)
        if nested_iso:
            nested_root = nested_iso.root()
            nested_bdmv, nested_path = _find_bdmv_dir(nested_iso, nested_root)
            if nested_bdmv:
                iso = nested_iso
                root = nested_root
                bdmv = nested_bdmv
                bdmv_path = [nested_name] + nested_path
    if not bdmv:
        raise RuntimeError("ISO 内没有 BDMV 目录")
    playlist_dir = iso.find(bdmv, "PLAYLIST")
    stream_dir = iso.find(bdmv, "STREAM")
    clipinf_dir = iso.find(bdmv, "CLIPINF")
    if not (playlist_dir and stream_dir and clipinf_dir):
        raise RuntimeError("BDMV 目录缺少 PLAYLIST/STREAM/CLIPINF")

    stream_entries = iso.list_dir(stream_dir)
    if not stream_entries:
        for child in iso.list_dir(bdmv):
            if not (child.get("flags", 0) & 2):
                continue
            nested_stream = iso.find(child, "STREAM")
            nested_entries = iso.list_dir(nested_stream) if nested_stream else []
            if nested_entries:
                stream_dir = nested_stream
                stream_entries = nested_entries
                break
    iso.prefetch_file_entries(stream_entries, max_gap=262144, max_chunk=16 * 1024 * 1024)
    stream_sizes = {}
    stream_ads = {}
    stream_entries_by_name = {}
    for entry in stream_entries:
        fields, ads, _ = iso.entry_info(entry)
        stream_sizes[entry["name"]] = fields["info_len"]
        stream_ads[entry["name"]] = ads
        stream_entries_by_name[entry["name"]] = entry
    if not stream_sizes:
        raise RuntimeError("BDMV STREAM 目录为空")

    playlist_entries = iso.list_dir(playlist_dir)
    iso.prefetch_file_entries(playlist_entries, max_gap=262144, max_chunk=16 * 1024 * 1024)
    iso.prefetch_files(playlist_entries, max_gap=262144, max_chunk=16 * 1024 * 1024)

    playlists = []
    for entry in playlist_entries:
        data = iso.file_data(iso.read_file_entry(entry), entry["part"])
        parsed = _parse_mpls(data, stream_sizes)
        if parsed:
            playlists.append({"name": entry["name"], **parsed})

    main_playlist = None
    largest_stream = max(stream_sizes, key=stream_sizes.get)
    if playlists:
        playlists.sort(key=lambda item: (item["max_clip_size"], item["unique_size"], item["duration"]), reverse=True)
        referenced_clips = {clip["clip"] for item in playlists for clip in item["clips"]}
        largest_playlist_size = max(item["unique_size"] for item in playlists)
        if (
            largest_stream not in referenced_clips
            and stream_sizes[largest_stream] > largest_playlist_size * 2
        ):
            main_clip = largest_stream
        else:
            main_playlist = playlists[0]
            main_clip = max({item["clip"] for item in main_playlist["clips"]}, key=lambda clip: stream_sizes.get(clip, 0))
    else:
        main_clip = largest_stream

    clpi_name = os.path.splitext(main_clip)[0] + ".clpi"
    clpi_entry = iso.find(clipinf_dir, clpi_name)
    streams = []
    if clpi_entry:
        iso.prefetch_file_entries([clpi_entry], max_gap=0, max_chunk=1024 * 1024)
        iso.prefetch_files([clpi_entry], max_gap=0, max_chunk=1024 * 1024)
        clpi_data = iso.file_data(iso.read_file_entry(clpi_entry), clpi_entry["part"])
        streams = _parse_clpi_streams(clpi_data, name)
    duration = float((main_playlist or {}).get("duration") or 0)
    fallback = ""
    main_stream_entry = stream_entries_by_name.get(main_clip)
    if not main_stream_entry:
        raise RuntimeError(f"未找到主片 M2TS: {main_clip}")
    if not streams:
        streams, sampled_duration = _probe_m2ts_sample(
            iso,
            main_stream_entry,
            stream_sizes.get(main_clip, 0),
        )
        duration = duration or sampled_duration
        fallback = "m2ts_sample"
    elif not main_playlist:
        duration = _m2ts_duration_from_pcr(
            iso,
            iso.physical_ranges_for_file(main_stream_entry),
            stream_sizes.get(main_clip, 0),
        )
        fallback = "largest_m2ts"

    _detect_iso_effects(name, streams)

    size = _file_size(file_node)
    raw_probe = {
        "format": {
            "filename": name,
            "format_name": "bluray,iso",
            "format_long_name": "Blu-ray ISO",
            "duration": f"{duration:.3f}" if duration > 0 else "",
            "size": str(size or stream_sizes.get(main_clip) or 0),
        },
        "streams": streams,
        "chapters": [],
        "_iso_probe": {
            "main_playlist": (main_playlist or {}).get("name") or "",
            "main_clip": main_clip,
            "main_clip_size": stream_sizes.get(main_clip, 0),
            "playlist_count": len(playlists),
            "bdmv_path": "/".join(bdmv_path),
            "fallback": fallback,
            "range_requests": iso.requests,
            "range_bytes": iso.bytes,
            "url_method": url_method,
            "elapsed_sec": round(time.perf_counter() - started, 3),
        },
    }
    if duration > 0 and size:
        raw_probe["format"]["bit_rate"] = str(int(size * 8 / duration))
    return raw_probe
