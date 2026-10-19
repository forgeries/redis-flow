import json
import math

import redis
from redis.exceptions import ConnectionError

import rf.default as setting
from rf.utils.log import log


class RedisDB:
    """Manage a standalone Redis connection and task-specific queue operations."""

    def __init__(
        self,
        ip_ports=None,
        db=None,
        user_pass=None,
        url=None,
        decode_responses=True,
        max_connections=1000,
        **kwargs,
    ):
        """Connect using a URL or the first configured host:port endpoint.

        Missing connection options are loaded from rf.default at construction.
        max_connections limits the connection pool; extra keyword arguments
        are forwarded to redis-py. Socket and connect timeouts default to five seconds.
        """

        # Resolve settings at construction so application overrides take effect.
        if ip_ports is None:
            ip_ports = setting.REDIS_IP_PORTS
        if db is None:
            db = setting.REDIS_DB
        if user_pass is None:
            user_pass = setting.REDIS_USER_PASS

        self._redis = None
        self._url = url
        self._ip_ports = ip_ports
        self._db = db
        self._user_pass = user_pass
        self._decode_responses = decode_responses
        self._max_connections = max_connections
        kwargs.setdefault("socket_connect_timeout", setting.REDIS_CONNECT_TIMEOUT)
        kwargs.setdefault("socket_timeout", setting.REDIS_SOCKET_TIMEOUT)
        self._kwargs = kwargs
        self.get_connect()

    def __repr__(self):
        if self._url:
            return "<Redisdb url=<redacted>>"

        return "<Redisdb ip_ports: {} db:{}>".format(self._ip_ports, self._db)

    def get_connect(self):
        """Create the connection pool and verify connectivity with PING."""
        if not self._url:
            if not self._ip_ports:
                raise ConnectionError(
                    "Redis connection settings are missing. Set ip_ports or url."
                )
            ip_ports = (
                self._ip_ports
                if isinstance(self._ip_ports, list)
                else self._ip_ports.split(",")
            )
            ip, port = ip_ports[0].split(":")
            self._redis = redis.StrictRedis(
                host=ip,
                port=port,
                db=self._db,
                password=self._user_pass,
                decode_responses=self._decode_responses,
                max_connections=self._max_connections,
                **self._kwargs,
            )
        else:
            self._redis = redis.StrictRedis.from_url(
                self._url,
                decode_responses=self._decode_responses,
                max_connections=self._max_connections,
                **self._kwargs,
            )

        return self._redis.ping()

    def zadd_if_absent(self, table, values, prioritys):
        """Insert absent members in one batch, preserving leases on retries."""
        return self._redis.zadd(table, dict(zip(values, prioritys)), nx=True)

    def commit_tasks(self, table, error_table, jobs, done, errors):
        """Publish outputs and acknowledge sources atomically on standalone Redis.

        Validate key types before writing. Retried inserts preserve existing
        scores, and a source that is also an output stays queued. Lost responses
        and callback side effects can still cause duplicate execution.
        """
        for _, score in jobs + errors:
            if not math.isfinite(score):
                raise ValueError("Task priority must be a finite number.")
        script = """
            -- Lua errors do not roll back writes, so validate every key first.
            for _, key in ipairs(KEYS) do
                local kind = redis.call('TYPE', key).ok
                if kind ~= 'none' and kind ~= 'zset' then
                    return redis.error_reply('WRONGTYPE Task queues must use sorted sets.')
                end
            end
            local jobs = cjson.decode(ARGV[1])
            local done = cjson.decode(ARGV[2])
            local errors = cjson.decode(ARGV[3])
            local added = {}
            for _, job in ipairs(jobs) do
                redis.call('ZADD', KEYS[1], 'NX', job[2], job[1])
                added[job[1]] = true
            end
            for _, job in ipairs(errors) do
                redis.call('ZADD', KEYS[2], 'NX', job[2], job[1])
            end
            for _, value in ipairs(done) do
                -- Keep a source queued if this batch also resubmits it.
                if not added[value] then redis.call('ZREM', KEYS[1], value) end
            end
            return 1
        """
        return self._redis.register_script(script)(
            keys=[table, error_table],
            args=[
                json.dumps(jobs, allow_nan=False),
                json.dumps(done),
                json.dumps(errors, allow_nan=False),
            ],
        )

    def zrevrangebyscore_set_score(
        self, table, priority_min, priority_max, score, count=None
    ):
        """Select an inclusive score range in descending order and replace its scores.

        Selection and score updates run atomically; count limits the selection.
        """

        lua = """
            local min_score = ARGV[1]
            local max_score = ARGV[2]
            local set_score = ARGV[3]
            local count = ARGV[4]

            local datas = nil
            if count then
                datas = redis.call(
                    'zrevrangebyscore', KEYS[1], max_score, min_score,
                    'withscores', 'limit', 0, count
                )
            else
                datas = redis.call('zrevrangebyscore', KEYS[1], max_score, min_score, 'withscores')
            end

            local real_datas = {}

            for i=1, #datas, 2 do
                local data = datas[i]
                local score = datas[i+1]
                table.insert(real_datas, data)
                redis.call('zincrby', KEYS[1], set_score - score, datas[i])
            end

            return real_datas
        """
        cmd = self._redis.register_script(lua)
        if count:
            res = cmd(keys=[table], args=[priority_min, priority_max, score, count])
        else:
            res = cmd(keys=[table], args=[priority_min, priority_max, score])
        return res

    def renew_tasks(self, table, values, deadline):
        """Renew existing tasks in one command without recreating removed members."""
        if values:
            return self._redis.zadd(table, dict.fromkeys(values, deadline), xx=True)
        return 0

    def zget_count(self, table, priority_min=None, priority_max=None):
        """Count all members, or an inclusive range when both bounds are provided."""

        if priority_min is not None and priority_max is not None:
            return self._redis.zcount(table, priority_min, priority_max)
        else:
            return self._redis.zcard(table)

    def getkeys(self, regex):
        """Return keys matching a Redis glob pattern; this uses the blocking KEYS command."""
        return self._redis.keys(regex)

    def clear(self, table):
        try:
            self._redis.delete(table)
        except Exception as e:
            log.error("Cannot delete Redis key %s: %s", table, e)

    def get_redis_obj(self):
        """Return the redis-py client for commands outside the task queue interface."""
        return self._redis
