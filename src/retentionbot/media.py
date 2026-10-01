from __future__ import annotations

import re
from urllib.parse import urlsplit

_ID = re.compile(r"^[A-Za-z0-9_-]+$")


def split_mxc(uri: str) -> tuple[str, str]:
    parts = urlsplit(uri)
    media_id = parts.path.removeprefix("/")
    if (
        parts.scheme != "mxc"
        or not parts.netloc
        or parts.username
        or parts.query
        or parts.fragment
        or not _ID.fullmatch(media_id)
    ):
        raise ValueError("Invalid MXC URI")
    return parts.netloc, media_id


def attachment_uris(content: dict) -> set[str]:
    """Extract actual attachments and explicit thumbnails, including encrypted files."""
    result: set[str] = set()
    for item in [content, content.get("m.new_content", {})]:
        if not isinstance(item, dict):
            continue
        info = item.get("info", {})
        if not isinstance(info, dict):
            info = {}
        candidates = [item.get("url"), info.get("thumbnail_url")]
        for file in [item.get("file"), info.get("thumbnail_file")]:
            if isinstance(file, dict):
                candidates.append(file.get("url"))
        for candidate in candidates:
            if isinstance(candidate, str):
                try:
                    split_mxc(candidate)
                except ValueError:
                    continue
                result.add(candidate)
    return result


def relation(content: dict) -> tuple[str | None, str | None]:
    related = content.get("m.relates_to", {})
    if not isinstance(related, dict):
        return None, None
    kind = related.get("rel_type")
    target = related.get("event_id")
    if kind in {"m.replace", "m.annotation"} and isinstance(target, str):
        return kind, target
    return None, None
