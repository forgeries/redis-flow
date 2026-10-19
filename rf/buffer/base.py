"""Serialize batch writes and retain failed snapshots for retry."""

import threading

import rf.default as default
from rf.utils.log import log


class BufferFullError(RuntimeError):
    """The buffer rejected new records because its capacity was reached."""


class BatchBuffer(threading.Thread):
    """Manage one in-flight snapshot without discarding writes on failure."""

    def __init__(self):
        super().__init__()
        default.validate_runtime_settings()
        self.max_records = default.BUFFER_MAX_RECORDS
        self._stop_event = threading.Event()
        self._state_lock = threading.RLock()
        self._flush_lock = threading.Lock()
        self._pending = None
        self._is_adding_to_db = False

    def _append(self, queue_name, items):
        """Accept all records together, or reject them without changing the buffer."""
        with self._state_lock:
            if self.get_jobs_count() + len(items) > self.max_records:
                raise BufferFullError(
                    f"Buffer capacity reached ({self.max_records} records). "
                    "Retry after pending writes complete."
                )
            getattr(self, queue_name).extend(items)

    def is_full(self):
        return self.get_jobs_count() >= self.max_records

    def run(self):
        while not self._stop_event.is_set():
            self.flush()
            self._stop_event.wait(1)

    def stop(self):
        self._stop_event.set()

    def is_adding_to_db(self):
        return self._is_adding_to_db

    def flush(self, *, raise_errors=False):
        """Write one snapshot, retaining it on failure. Return whether it succeeded.

        Set raise_errors to propagate the write exception to the caller.
        """
        with self._flush_lock:
            self._is_adding_to_db = True
            try:
                with self._state_lock:
                    if self._pending is None:
                        self._pending = self._snapshot()
                    batch = self._pending
                if batch is not None:
                    self._write(batch)
                    with self._state_lock:
                        self._pending = None
                return True
            except Exception as exc:
                if raise_errors:
                    raise
                log.exception("Task batch failed: %s", exc)
                return False
            finally:
                self._is_adding_to_db = False
