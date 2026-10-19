import logging

import pytest


@pytest.fixture(autouse=True)
def no_redis_connections(monkeypatch):
    from rf.db.redisdb import RedisDB
    from rf.utils.log import Log

    def forbidden(*args, **kwargs):
        raise AssertionError("Unit tests must not connect to Redis")

    monkeypatch.setattr(RedisDB, "get_connect", forbidden)
    monkeypatch.setattr(Log, "log", logging.getLogger("rf-tests"))
