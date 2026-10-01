from __future__ import annotations

import json
import logging
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    def format(self, record):
        result = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            result["error_type"] = record.exc_info[0].__name__
        for name in ("event", "room_id", "event_id", "uri", "code"):
            if hasattr(record, name):
                result[name] = getattr(record, name)
        # Newlines in messages are escaped by JSON; every record is exactly one physical line.
        return json.dumps(result, ensure_ascii=False, separators=(",", ":"))
