import logging
import os
import uuid

import pytest

from rf import default
from rf.db.redisdb import RedisDB
from rf.utils.log import Log


@pytest.fixture
def redis_db(monkeypatch):
    url = os.environ.get("RF_TEST_REDIS_URL")
    if not url:
        pytest.skip("Set RF_TEST_REDIS_URL to an isolated test Redis")
    db = RedisDB(url=url, socket_timeout=2, socket_connect_timeout=2)
    monkeypatch.setattr(Log, "log", logging.getLogger("rf-integration"))
    return db


@pytest.fixture
def namespace(redis_db):
    name = "rf-test:" + uuid.uuid4().hex
    yield name
    client = redis_db.get_redis_obj()
    # Delete only this test namespace; other runs may share the Redis instance.
    for key in client.scan_iter(match="*" + name + "*"):
        client.delete(key)


@pytest.fixture
def components(monkeypatch, redis_db):
    from rf import collector, scheduler
    from rf.buffer import queue_buffer, task_buffer
    from rf.utils import redis_lock

    for module in (collector, scheduler, task_buffer, queue_buffer, redis_lock):
        monkeypatch.setattr(module, "RedisDB", lambda **kwargs: redis_db)
    monkeypatch.setattr(default, "PROCESS_THREAD_COUNT", 1)
    monkeypatch.setattr(default, "COLLECTOR_TASK_COUNT", 2)
    return redis_db
