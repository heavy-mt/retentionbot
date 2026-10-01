from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

UNITS = {
    "ms": 1,
    "s": 1000,
    "m": 60000,
    "h": 3600000,
    "d": 86400000,
    "w": 604800000,
    "y": 31536000000,
}


def duration(value, *, minimum: int = 0) -> int:
    if type(value) is int:
        result = value
    elif isinstance(value, str) and (match := re.fullmatch(r"(\d+)(ms|s|m|h|d|w|y)?", value)):
        result = int(match[1]) * UNITS[match[2] or "ms"]
    else:
        raise ValueError("Срок задаётся целым числом миллисекунд или строкой вроде 1h, 7d.")
    if result < minimum or result > 2**63 - 1:
        raise ValueError("Недопустимый срок хранения.")
    return result


@dataclass(frozen=True)
class Policy:
    min_lifetime: int | None
    max_lifetime: int | None


@dataclass(frozen=True)
class ServerPolicy:
    default_min: int | None = None
    default_max: int | None = None
    allowed_min: int | None = None
    allowed_max: int | None = None

    @classmethod
    def from_file(cls, path: Path):
        config = yaml.safe_load(path.read_text())
        retention = config.get("retention", {})
        if retention.get("enabled") is not True:
            raise ValueError("В конфигурации Synapse должен быть включён retention.enabled.")
        default = retention.get("default_policy") or {}

        def parse(value):
            return None if value is None else duration(value)

        policy = cls(
            parse(default.get("min_lifetime")),
            parse(default.get("max_lifetime")),
            parse(retention.get("allowed_lifetime_min")),
            parse(retention.get("allowed_lifetime_max")),
        )
        if policy.allowed_min is not None and policy.allowed_max is not None:
            if policy.allowed_min > policy.allowed_max:
                raise ValueError("Некорректные серверные границы retention.")
        policy.effective({})
        return policy

    def effective(self, content: dict) -> Policy:
        # Missing fields fall back independently, just as in Synapse.
        def field(name, default):
            value = content.get(name)
            return value if type(value) is int and value >= 0 else default

        low = field("min_lifetime", self.default_min)
        high = field("max_lifetime", self.default_max)
        if high is not None:
            if self.allowed_min is not None:
                high = max(high, self.allowed_min)
            if self.allowed_max is not None:
                high = min(high, self.allowed_max)
            if high <= 0 or (low is not None and low > high):
                raise ValueError("min_lifetime превышает допустимый max_lifetime.")
        return Policy(low, high)

    def requested(self, argument: str) -> dict:
        raw = json.loads(argument)
        content = {"max_lifetime": duration(raw["max_lifetime"], minimum=1)}
        if "min_lifetime" in raw:
            content["min_lifetime"] = duration(raw["min_lifetime"])
        effective = self.effective(content)
        # Write the actual bounded maximum so native visibility and purge agree.
        return {k: v for k, v in asdict(effective).items() if v is not None}
