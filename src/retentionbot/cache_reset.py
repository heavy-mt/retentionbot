from __future__ import annotations

from collections.abc import Mapping


def toggle_ignored_user_sentinel(content, sentinel: str) -> dict:
    """Toggle one reserved ignored-user sentinel without changing real entries."""
    if content is None:
        result = {}
    elif isinstance(content, Mapping):
        result = dict(content)
    else:
        raise ValueError("m.ignored_user_list content must be an object")

    ignored = result.get("ignored_users", {})
    if not isinstance(ignored, Mapping):
        raise ValueError("m.ignored_user_list.ignored_users must be an object")

    ignored = dict(ignored)
    if sentinel in ignored:
        ignored.pop(sentinel)
    else:
        ignored[sentinel] = {}
    result["ignored_users"] = ignored
    return result
