from __future__ import annotations

from collections.abc import Mapping


def _plain_json(value):
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json(item) for item in value]
    return value


def cache_reset_sentinel(base_sentinel: str, generation: int) -> str:
    if generation <= 0 or not base_sentinel.startswith("@") or ":" not in base_sentinel:
        raise ValueError("Invalid cache-reset sentinel")
    localpart, server_name = base_sentinel[1:].split(":", 1)
    if localpart != "__retention_cache_reset" or not server_name:
        raise ValueError("Cache-reset sentinel localpart must be __retention_cache_reset")
    return f"@{localpart}-{generation}:{server_name}"


def set_ignored_user_reset_sentinel(content, base_sentinel: str, generation: int) -> dict:
    """Replace our reserved sentinel while preserving real ignored-user entries."""
    if content is None:
        result = {}
    elif isinstance(content, Mapping):
        result = _plain_json(content)
    else:
        raise ValueError("m.ignored_user_list content must be an object")

    ignored = result.get("ignored_users", {})
    if not isinstance(ignored, Mapping):
        raise ValueError("m.ignored_user_list.ignored_users must be an object")

    current = cache_reset_sentinel(base_sentinel, generation)
    base_localpart, server_name = base_sentinel[1:].split(":", 1)
    prefix = f"@{base_localpart}-"
    suffix = f":{server_name}"

    cleaned = {}
    for user_id, value in ignored.items():
        is_ours = False
        if isinstance(user_id, str) and user_id.startswith(prefix) and user_id.endswith(suffix):
            middle = user_id[len(prefix) : -len(suffix)]
            is_ours = middle.isdigit()
        if user_id == base_sentinel or is_ours:
            continue
        cleaned[user_id] = value

    cleaned[current] = {}
    result["ignored_users"] = cleaned
    return result
