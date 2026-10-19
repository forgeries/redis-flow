import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("source", "expected", "error"),
    [
        (None, "10", None),
        ("PROCESS_THREAD_COUNT = 3\n_hidden = 99\n", "3", None),
        (
            "__all__ = ['PROCESS_THREAD_COUNT']\n"
            "PROCESS_THREAD_COUNT = 4\nUNEXPORTED = 99\n",
            "4",
            None,
        ),
        (
            "raise RuntimeError('broken settings')\n",
            None,
            "RuntimeError: broken settings",
        ),
        (
            "import rf_nonexistent_settings_dependency\n",
            None,
            "No module named 'rf_nonexistent_settings_dependency'",
        ),
        ("PROCESS_THREAD_COUNT =\n", None, "SyntaxError"),
    ],
)
def test_settings_import_only_ignores_an_absent_module(
    tmp_path, source, expected, error
):
    if source is not None:
        (tmp_path / "settings.py").write_text(source, encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from rf import default; "
            "assert not hasattr(default, '_hidden'); "
            "assert not hasattr(default, 'UNEXPORTED'); "
            "print(default.PROCESS_THREAD_COUNT)",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if error is None:
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == expected
    else:
        assert result.returncode != 0
        assert error in result.stderr


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("JOB_LOST_TIMEOUT", 3),
        ("HEARTBEAT_INTERVAL", 0),
        ("HEARTBEAT_INTERVAL", float("nan")),
        ("REDIS_SOCKET_TIMEOUT", None),
        ("REDIS_CONNECT_TIMEOUT", -1),
        ("BUFFER_MAX_RECORDS", 0),
        ("BUFFER_MAX_RECORDS", True),
        ("HEARTBEAT_BATCH_SIZE", 1.5),
    ],
)
def test_runtime_settings_reject_invalid_values(monkeypatch, name, value):
    from rf import default

    monkeypatch.setattr(default, name, value)
    with pytest.raises(ValueError, match=name):
        default.validate_runtime_settings()


def test_default_timing_has_room_for_connection_and_command_timeouts():
    from rf import default

    default.validate_runtime_settings()
    assert default.JOB_LOST_TIMEOUT >= 2 * (
        default.HEARTBEAT_INTERVAL
        + default.REDIS_SOCKET_TIMEOUT
        + default.REDIS_CONNECT_TIMEOUT
    )
