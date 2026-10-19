"""Provide a renewable lock backed by one Redis instance.

Original author: Boris <boris_liu@foxmail.com>, 2019.
"""

import threading
import time
import uuid

from rf.db.redisdb import RedisDB
from rf.utils.log import log


class RedisLock:
    """Use an ownership token to guard lock renewal and release."""

    _RELEASE = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
    """
    _RENEW = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('EXPIRE', KEYS[1], ARGV[2])
        end
        return 0
    """

    def __init__(
        self, key, *, wait_timeout=0, lock_timeout=86400, redis_cli=None, redis_url=None
    ):
        self._redis_conn = redis_cli
        self.redis_url = redis_url
        self.lock_key = "redis_lock:{}".format(key)
        self.lock_timeout = lock_timeout
        self.wait_timeout = wait_timeout
        self.locked = False
        self._token = None
        self._stop_event = threading.Event()
        self._renew_thread = None

    @property
    def redis_conn(self):
        if self._redis_conn is None:
            self._redis_conn = RedisDB(url=self.redis_url).get_redis_obj()
        return self._redis_conn

    @redis_conn.setter
    def redis_conn(self, client):
        self._redis_conn = client

    def __enter__(self):
        if not self.locked:
            self.acquire()
        if self.locked:
            self._renew_thread = threading.Thread(target=self.prolong_life, daemon=True)
            self._renew_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def __repr__(self):
        return "<RedisLock: {}>".format(self.lock_key)

    def acquire(self):
        if self.locked:
            return True
        self._stop_event.clear()
        self._token = uuid.uuid4().hex
        start = time.monotonic()
        while not self._stop_event.is_set():
            if self.redis_conn.set(self.lock_key, self._token, nx=True, ex=5):
                self.locked = True
                return True
            remaining = self.wait_timeout - (time.monotonic() - start)
            if remaining <= 0:
                return False
            self._stop_event.wait(min(0.1, remaining))
        return False

    def release(self):
        self._stop_event.set()
        if self._renew_thread and self._renew_thread is not threading.current_thread():
            self._renew_thread.join()
        if self.locked:
            try:
                return bool(
                    self.redis_conn.eval(self._RELEASE, 1, self.lock_key, self._token)
                )
            finally:
                self.locked = False
        return False

    def renew(self):
        if not self.locked:
            return False
        renewed = self.redis_conn.eval(self._RENEW, 1, self.lock_key, self._token, 5)
        if not renewed:
            self.locked = False
        return bool(renewed)

    def prolong_life(self):
        start = time.monotonic()
        while not self._stop_event.wait(1):
            try:
                if self.lock_timeout and time.monotonic() - start >= self.lock_timeout:
                    self.release()
                    return
                if not self.renew():
                    return
            except Exception as exc:
                self.locked = False
                log.exception("Cannot renew the Redis lock: %s", exc)
                return
