"""Run stdlib image-compatible checks under the repository's pytest fixtures."""

import subprocess
import sys
from pathlib import Path


def test_room_limiter_regressions():
    checker = Path(__file__).resolve().parents[1] / "deploy/check-synapse-room-limiter.py"
    result = subprocess.run(
        [sys.executable, str(checker), "-v"], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr
