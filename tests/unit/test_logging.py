import logging
import uuid

import pytest

from rf.utils.log import RotatingFileHandler, get_logger


@pytest.mark.parametrize("filename", ["worker.log", "worker.debug.log", "worker"])
def test_rotation_preserves_dotted_directories_and_backup_order(tmp_path, filename):
    directory = tmp_path / "logs.v1"
    directory.mkdir()
    path = directory / filename
    handler = RotatingFileHandler(path, backup_count=10, encoding="utf-8")
    try:
        for message in ("first", "second"):
            record = logging.LogRecord(
                "rotation", logging.INFO, "", 0, message, (), None
            )
            handler.emit(record)
            handler.doRollover()
        first_backup = directory / f"{path.stem}01{path.suffix}"
        second_backup = directory / f"{path.stem}02{path.suffix}"
        assert first_backup.read_text("utf-8") == "second\n"
        assert second_backup.read_text("utf-8") == "first\n"
        assert path.read_text("utf-8") == ""
    finally:
        handler.close()


def test_file_logger_accepts_bare_filename_and_explicit_zero_limits(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    logger = get_logger(
        name="test_" + uuid.uuid4().hex,
        path="worker.log",
        is_write_to_console=False,
        is_write_to_file=True,
        max_bytes=0,
        backup_count=0,
        encoding="utf-8",
    )
    try:
        logger.info("saved")
        assert "saved" in (tmp_path / "worker.log").read_text("utf-8")
        handler = logger.handlers[0]
        assert handler.max_bytes == 0
        assert handler.backup_count == 0
    finally:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
