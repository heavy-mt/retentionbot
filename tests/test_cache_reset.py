import pytest

from retentionbot.cache_reset import (
    cache_reset_sentinel,
    set_ignored_user_reset_sentinel,
)

SENTINEL = "@__retention_cache_reset:test.local"


def test_cache_reset_generation_is_monotonic_and_preserves_real_ignored_users():
    original = {
        "ignored_users": {"@blocked:test.local": {"reason": "keep"}},
        "custom": {"keep": True},
    }
    first = set_ignored_user_reset_sentinel(original, SENTINEL, 4)
    second = set_ignored_user_reset_sentinel(first, SENTINEL, 9)

    assert first == {
        "ignored_users": {
            "@blocked:test.local": {"reason": "keep"},
            "@__retention_cache_reset-4:test.local": {},
        },
        "custom": {"keep": True},
    }
    assert second == {
        "ignored_users": {
            "@blocked:test.local": {"reason": "keep"},
            "@__retention_cache_reset-9:test.local": {},
        },
        "custom": {"keep": True},
    }
    assert original == {
        "ignored_users": {"@blocked:test.local": {"reason": "keep"}},
        "custom": {"keep": True},
    }


def test_cache_reset_removes_only_reserved_generation_sentinels():
    original = {
        "ignored_users": {
            "@blocked:test.local": {},
            "@__retention_cache_reset-12:test.local": {},
            "@similar:test.local": {},
        }
    }
    updated = set_ignored_user_reset_sentinel(original, SENTINEL, 13)
    assert set(updated["ignored_users"]) == {
        "@blocked:test.local",
        "@similar:test.local",
        "@__retention_cache_reset-13:test.local",
    }


def test_cache_reset_sentinel_requires_reserved_localpart_and_positive_generation():
    assert cache_reset_sentinel(SENTINEL, 7) == "@__retention_cache_reset-7:test.local"
    with pytest.raises(ValueError):
        cache_reset_sentinel("@other:test.local", 7)
    with pytest.raises(ValueError):
        cache_reset_sentinel(SENTINEL, 0)


@pytest.mark.parametrize("content", [{"ignored_users": []}, {"ignored_users": "bad"}])
def test_cache_reset_rejects_malformed_ignore_list(content):
    with pytest.raises(ValueError):
        set_ignored_user_reset_sentinel(content, SENTINEL, 1)
