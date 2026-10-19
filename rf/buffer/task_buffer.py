import collections
import json
import math
import threading

import rf.default as default
from rf.buffer.base import BatchBuffer, BufferFullError
from rf.db.redisdb import RedisDB
from rf.struct.task_struct import Task


class TaskBuffer(BatchBuffer):
    """Batch local outputs, errors, and source acknowledgements into one commit."""

    def __init__(self, redis_key):
        super().__init__()
        self._db = RedisDB()
        self._jobs_deque = collections.deque()
        self._del_jobs_deque = collections.deque()
        self._err_jobs_deque = collections.deque()
        self._table_job = default.TAB_JOB.format(redis_key=redis_key)
        self._table_job_err = default.TAB_JOB_ERR.format(redis_key=redis_key)

    def put_job(self, job):
        # Snapshot the payload before accepting it so retries use identical bytes.
        self._append("_jobs_deque", [self._serialize(job)])
        if self.is_full():
            self.flush()

    @staticmethod
    def _serialize(job, receipt=None):
        if not isinstance(job, Task):
            raise TypeError("Submit a Task instance.")
        if not math.isfinite(job.priority):
            raise ValueError("Task priority must be a finite number.")
        return (json.dumps(job.to_dict, allow_nan=False), job.priority, receipt)

    def submit(self, jobs, *, wait=True):
        """Accept one local batch and return its Redis confirmation event.

        A failed write raises when wait is True and leaves the batch for retry.
        A capacity or serialization error rejects the whole batch.
        """
        receipt = threading.Event()
        items = []
        for job in jobs:
            if len(items) >= self.max_records:
                raise BufferFullError("The submission exceeds the buffer capacity.")
            items.append(self._serialize(job, receipt))
        self._append("_jobs_deque", items)
        if not items:
            receipt.set()
        if wait:
            while not receipt.is_set():
                self.flush(raise_errors=True)
        return receipt

    def put_del_job(self, job, *, on_confirm=None, dependencies=()):
        """Buffer source removal until every cross-queue receipt is confirmed."""
        self._append(
            "_del_jobs_deque",
            [
                (
                    job,
                    on_confirm,
                    tuple(event for event in dependencies if not event.is_set()),
                )
            ],
        )

    def put_err_job(self, job, *, source=None, on_confirm=None, dependencies=()):
        """Buffer an error and its optional source acknowledgement as one record."""
        acknowledgement = None
        if source is not None:
            acknowledgement = (
                source,
                on_confirm,
                tuple(event for event in dependencies if not event.is_set()),
            )
        value = (*self._serialize(job)[:2], acknowledgement)
        self._append("_err_jobs_deque", [value])

    def get_jobs_count(self):
        with self._state_lock:
            return (
                len(self._jobs_deque)
                + len(self._del_jobs_deque)
                + len(self._err_jobs_deque)
                + (sum(map(len, self._pending)) if self._pending else 0)
            )

    def _snapshot(self):
        # Capture outputs and acknowledgements under the same lock to preserve order.
        ready, waiting = [], collections.deque()
        for item in self._del_jobs_deque:
            (ready if all(event.is_set() for event in item[2]) else waiting).append(
                item
            )
        errors, waiting_errors = [], collections.deque()
        for item in self._err_jobs_deque:
            ready_error = item[2] is None or all(event.is_set() for event in item[2][2])
            (errors if ready_error else waiting_errors).append(item)
        jobs = list(self._jobs_deque)
        self._jobs_deque.clear()
        self._err_jobs_deque = waiting_errors
        self._del_jobs_deque = waiting
        return (jobs, ready, errors) if jobs or ready or errors else None

    def _write(self, batch):
        jobs, acknowledgements, errors = batch
        acknowledgements = acknowledgements + [
            item[2] for item in errors if item[2] is not None
        ]
        self._db.commit_tasks(
            self._table_job,
            self._table_job_err,
            [item[:2] for item in jobs],
            [item[0] for item in acknowledgements],
            [item[:2] for item in errors],
        )
        for _, on_confirm, _ in acknowledgements:
            if on_confirm is not None:
                on_confirm()
        for _, _, receipt in jobs:
            if receipt is not None:
                receipt.set()
