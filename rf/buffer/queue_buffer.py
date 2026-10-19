import collections
import json
import math
import threading

from rf.buffer.base import BatchBuffer
from rf.db.redisdb import RedisDB


class QueueBuffer(BatchBuffer):
    """Batch destination writes and confirm each receipt after Redis accepts it."""

    def __init__(self):
        super().__init__()
        self._db = RedisDB()
        self._jobs_deque = collections.deque()

    def put_job(self, job):
        """Snapshot an outbound task and return its write-confirmation event."""
        receipt = threading.Event()
        if not math.isfinite(job.Task.priority):
            raise ValueError("Task priority must be a finite number.")
        item = (
            job.RedisDB or self._db,
            job.redis_key,
            json.dumps(job.Task.to_dict, allow_nan=False),
            job.Task.priority,
            receipt,
        )
        self._append("_jobs_deque", [item])
        if self.is_full():
            self.flush()
        return receipt

    def get_jobs_count(self):
        with self._state_lock:
            return len(self._jobs_deque) + (len(self._pending) if self._pending else 0)

    def _snapshot(self):
        batch = list(self._jobs_deque)
        self._jobs_deque.clear()
        return batch or None

    def _write(self, batch):
        groups = {}
        for db, key, value, score, receipt in batch:
            if not receipt.is_set():
                groups.setdefault((db, key), []).append((value, score, receipt))
        for (db, key), items in groups.items():
            db.zadd_if_absent(
                key, [item[0] for item in items], [item[1] for item in items]
            )
            for _, _, receipt in items:
                receipt.set()
