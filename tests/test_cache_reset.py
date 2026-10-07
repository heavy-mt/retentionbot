import pytest

from retentionbot.cache_reset import toggle_ignored_user_sentinel


SENTINEL = "@__retention_cache_reset:test.local"


def test_cache_reset_adds_sentinel_and_preserves_real_ignored_users():
    original = {
        "ignored_users": {"@blocked:test.local": {"reason": "keep"}},
        "custom": {"keep": True},
    }
    updated = toggle_ignored_user_sentinel(original, SENTINEL)

    assert updated == {
        "ignored_users": {
            "@blocked:test.local": {"reason": "keep"},
            SENTINEL: {},
        },
        "custom": {"keep": True},
    }
    assert original == {
        "ignored_users": {"@blocked:test.local": {"reason": "keep"}},
        "custom": {"keep": True},
    }


def test_cache_reset_removes_only_its_own_sentinel():
    original = {
        "ignored_users": {
            "@blocked:test.local": {},
            SENTINEL: {},
        }
    }
    assert toggle_ignored_user_sentinel(original, SENTINEL) == {
        "ignored_users": {"@blocked:test.local": {}}
    }


@pytest.mark.parametrize("content", [{"ignored_users": []}, {"ignored_users": "bad"}])
def test_cache_reset_rejects_malformed_ignore_list(content):
    with pytest.raises(ValueError):
        toggle_ignored_user_sentinel(content, SENTINEL)
