import json
import logging
from io import StringIO

import pytest

from retentionbot.jsonlog import JsonFormatter
from retentionbot.policy import ServerPolicy, duration


def test_server_defaults_limits_and_minimum_are_distinct():
    server = ServerPolicy(default_min=100, default_max=1000, allowed_min=500, allowed_max=2000)
    assert server.effective({}).max_lifetime == 1000
    assert server.effective({"max_lifetime": 1}).max_lifetime == 500
    assert server.effective({"max_lifetime": 10000}).max_lifetime == 2000
    assert server.effective({"min_lifetime": 200}).max_lifetime == 1000
    assert server.requested('{"min_lifetime":200,"max_lifetime":10000}') == {
        "min_lifetime": 200,
        "max_lifetime": 2000,
    }
    with pytest.raises(ValueError):
        server.requested('{"min_lifetime":3000,"max_lifetime":10000}')
    assert duration("1d") == 86400000
    with pytest.raises(ValueError):
        duration(True)


def test_json_log_is_one_line_even_with_newlines_and_exception():
    output = StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(JsonFormatter())
    logger = logging.Logger("test")
    logger.addHandler(handler)
    try:
        raise RuntimeError("sensitive upstream URL")
    except RuntimeError:
        logger.exception("Ошибка\nПовторим позднее", extra={"event": "job.retry"})
    assert len(output.getvalue().splitlines()) == 1
    record = json.loads(output.getvalue())
    assert record["message"] == "Ошибка\nПовторим позднее"
    assert record["event"] == "job.retry"
    assert record["error_type"] == "RuntimeError"
    assert "sensitive" not in output.getvalue()
