from unittest.mock import Mock

from rf.db.redisdb import RedisDB
from rf.utils.log import Log


def test_connection_repr_does_not_expose_password(monkeypatch):
    monkeypatch.setattr(RedisDB, "get_connect", lambda self: None)
    assert "secret-value" not in repr(RedisDB(user_pass="secret-value"))
    assert "secret-value" not in repr(
        RedisDB(url="redis://user:secret-value@localhost")
    )


def test_logger_initializes_on_first_property_access(monkeypatch):
    import rf.utils.log as log_module

    logger = Mock()
    monkeypatch.setattr(Log, "log", None)
    monkeypatch.setattr(log_module, "get_logger", lambda: logger)
    Log().info("hello")
    logger.info.assert_called_once_with("hello")
