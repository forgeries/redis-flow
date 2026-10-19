import copy
import threading
from collections.abc import Iterable

import rf.default as default
import rf.utils.tools as tools
from rf.base_process import BaseProcess
from rf.buffer import QueueBuffer, TaskBuffer
from rf.collector import Collector
from rf.db.redisdb import RedisDB
from rf.process_control import ProcessControl
from rf.struct import Queue, Task
from rf.utils.log import log
from rf.utils.redis_lock import RedisLock


class Scheduler(threading.Thread):
    """Coordinate collectors, callback workers, buffers, and graceful shutdown."""

    __custom_default__ = {}

    def __init__(
        self,
        redis_key=None,
        thread_count=None,
        begin_callback=None,
        end_callback=None,
        delete_keys=(),
        keep_alive=None,
        auto_start_jobs=None,
        wait_lock=True,
        **kwargs,
    ):
        """Configure a worker for a Redis task namespace.

        Unspecified options use rf.default. delete_keys accepts glob suffixes
        within the namespace, or True to clear its task keys. wait_lock enables
        a nonblocking Redis lock around initial task submission.
        """

        super(Scheduler, self).__init__()

        for key, value in self.__class__.__custom_default__.items():
            setattr(default, key, value)

        self._redis_key = redis_key or default.REDIS_KEY
        if not self._redis_key:
            raise Exception(
                "The task namespace is empty. Set redis_key or rf.default.REDIS_KEY."
            )
        self._processes = []
        self._process_controls = []
        self._process_control_obj = ProcessControl

        self._keep_alive = keep_alive if keep_alive is not None else default.KEEP_ALIVE
        self._auto_start_jobs = (
            auto_start_jobs
            if auto_start_jobs is not None
            else default.PROCESS_AUTO_START_JOBS
        )
        self.wait_lock = wait_lock

        self._begin_callback = (
            begin_callback
            if begin_callback
            else lambda: log.info("redis-flow started.")
        )
        self._end_callback = (
            end_callback if end_callback else lambda: log.info("redis-flow is idle.")
        )

        if thread_count:
            setattr(default, "PROCESS_THREAD_COUNT", thread_count)
            setattr(default, "COLLECTOR_TASK_COUNT", thread_count * 2)
        default.validate_runtime_settings()
        self._thread_count = default.PROCESS_THREAD_COUNT

        self._redisdb = RedisDB()
        self._is_notify_end = False
        self._shutdown_event = threading.Event()
        delete_keys = delete_keys or default.DELETE_KEYS
        if delete_keys:
            self.delete_tables(delete_keys)

        self._task_buffer = TaskBuffer(self._redis_key)
        self._queue_buffer = QueueBuffer()
        self._collector = Collector(
            self._redis_key,
            can_collect=lambda: (
                not (self._task_buffer.is_full() or self._queue_buffer.is_full())
            ),
        )

    def submit(self, tasks, *, wait=True):
        """Submit Tasks to this flow's queue and return a confirmation event.

        Wait for Redis by default. Use wait=False only while the flow runs.
        Stop producers before stopping the flow. Routing overrides are not supported.
        Each batch must fit the available buffer capacity.
        """
        if not wait and (not self.is_alive() or self._shutdown_event.is_set()):
            raise RuntimeError("Asynchronous submission requires a running flow.")

        def local_tasks():
            for task in tasks:
                if not isinstance(task, Task):
                    raise TypeError("submit() accepts Task instances only.")
                if any(
                    value is not None
                    for value in (
                        task.redis_key,
                        task.redis_ip_ports,
                        task.redis_user_pass,
                        task.redis_db,
                    )
                ):
                    raise ValueError(
                        "submit() only accepts tasks for this flow's queue."
                    )
                task = copy.copy(task)
                if task.process_name is None:
                    if len(self._processes) != 1:
                        raise ValueError(
                            "Set process_name when multiple processes are registered."
                        )
                    task.process_name = self._processes[0].name
                yield task

        return self._task_buffer.submit(local_tasks(), wait=wait)

    def flush(self):
        """Confirm buffered writes, raising on failure and retaining them for retry.

        This also works before workers start. Stop producers first if the call
        must finish with empty buffers. It does not wait for task execution.
        """
        while self._queue_buffer.get_jobs_count() or self._task_buffer.get_jobs_count():
            self._queue_buffer.flush(raise_errors=True)
            self._task_buffer.flush(raise_errors=True)

    def add_process(self, process, **kwargs):
        process = process(**kwargs)
        if isinstance(process, BaseProcess):
            self._processes.append(process)
        else:
            raise ValueError("The process class must inherit from rf.BaseProcess.")

    def run(self):
        try:
            self._start()
            while not self._shutdown_event.is_set():
                try:
                    self._tick()
                    if self.all_thread_is_done():
                        if not self._keep_alive:
                            break
                        if not self._is_notify_end:
                            self.worker_end()
                            self._is_notify_end = True
                    else:
                        self._is_notify_end = False
                except Exception as exc:
                    log.exception("Cannot check the flow state: %s", exc)
                self._shutdown_event.wait(1)
        finally:
            self._stop_all_thread()
            if not self._is_notify_end or not self._keep_alive:
                self.worker_end()
            if self._keep_alive:
                for process in self._processes:
                    process.close()

    def _tick(self):
        pass

    def stop(self):
        """Request shutdown while keeping leases active until buffered writes finish.

        Use join(timeout) to bound how long the caller waits.
        """
        self._shutdown_event.set()
        self._collector.stop_collecting()
        for control in self._process_controls:
            control.stop()

    def _add_task(self):

        todo_task_count = self._collector.get_jobs_count()
        if todo_task_count:
            log.info(
                "Found %s pending tasks. Continue these tasks. Skip initial task submission.",
                todo_task_count,
            )
        else:
            for process in self._processes:
                results = process.start_jobs()

                if results is not None and (
                    not isinstance(results, Iterable)
                    or isinstance(results, (str, bytes, dict))
                ):
                    raise TypeError(
                        f"{process.name}.start_jobs must return an iterable or None."
                    )

                for result in results or []:
                    if isinstance(result, Task):
                        result.process_name = result.process_name or process.name
                        if (
                            result.redis_key
                            or result.redis_ip_ports
                            or result.redis_user_pass
                            or result.redis_db
                        ):
                            queue = Queue(
                                redis_key=result.redis_key,
                                redis_ip_ports=result.redis_ip_ports,
                                redis_user_pass=result.redis_user_pass,
                                redis_db=result.redis_db,
                                task=result,
                            )
                            self._queue_buffer.put_job(queue)
                        else:
                            self._task_buffer.put_job(result)
                    elif isinstance(result, Queue):
                        result.Task.process_name = (
                            result.Task.process_name or process.name
                        )
                        self._queue_buffer.put_job(result)
                    else:
                        raise TypeError(
                            f"{process.name}.start_jobs must yield a Task or Queue. "
                            f"Received {type(result).__name__}."
                        )
                self._task_buffer.flush()
                self._queue_buffer.flush()

    def _start(self):

        self._task_buffer.start()
        self._queue_buffer.start()

        self._collector.start()
        self.worker_begin()

        for i in range(self._thread_count):
            process_control = self._process_control_obj(
                self._collector,
                self._task_buffer,
                self._queue_buffer,
            )

            for process in self._processes:
                process_control.add_process(process)

            process_control.start()
            self._process_controls.append(process_control)
        self._schedule_start_jobs()

    def _schedule_start_jobs(self):
        if self._auto_start_jobs and not self._shutdown_event.is_set():
            if self.wait_lock:
                with RedisLock(key=self._redis_key) as lock:
                    if lock.locked:
                        self._add_task()
            else:
                self._add_task()

    def all_thread_is_done(self):
        # Sample repeatedly because work can move between stages during a check.
        for i in range(3):
            if (
                self._collector.is_collector_task()
                or self._collector.get_jobs_count() > 0
            ):
                return False

            for process_control in self._process_controls:
                if not process_control.is_not_task():
                    return False

            if (
                self._task_buffer.get_jobs_count() > 0
                or self._task_buffer.is_adding_to_db()
            ):
                return False

            if (
                self._queue_buffer.get_jobs_count() > 0
                or self._queue_buffer.is_adding_to_db()
            ):
                return False
            tools.delay_time(1)

        return True

    def delete_tables(self, delete_keys):
        if delete_keys is True:
            delete_keys = [self._redis_key + "*"]
        elif not isinstance(delete_keys, (list, tuple)):
            delete_keys = [delete_keys]

        for delete_key in delete_keys:
            if not delete_key.startswith(self._redis_key):
                delete_key = self._redis_key + delete_key
            keys = self._redisdb.getkeys(default.TAB_JOB.format(redis_key=delete_key))
            for key in keys:
                log.debug("Delete Redis key: %s", key)
                self._redisdb.clear(key)

    def _stop_all_thread(self):
        self.stop()
        if self._collector.ident is not None:
            self._collector.join()
        for control in self._process_controls:
            control.join()
        # Workers have stopped producing outputs; renew leases until writes finish.
        for buffer in (self._queue_buffer, self._task_buffer):
            buffer.stop()
            if buffer.ident is not None:
                buffer.join()
        while self._queue_buffer.get_jobs_count() or self._task_buffer.get_jobs_count():
            self._queue_buffer.flush()
            self._task_buffer.flush()
            if (
                self._queue_buffer.get_jobs_count()
                or self._task_buffer.get_jobs_count()
            ):
                tools.delay_time(0.1)
        self._collector.heartbeat_stop()

    def worker_begin(self):
        """Invoke the scheduler and process startup callbacks."""

        if self._begin_callback:
            self._begin_callback()

        for process in self._processes:
            process.start_callback()

    def worker_end(self):
        """Invoke idle callbacks and close processes in finite workers."""
        if self._end_callback:
            self._end_callback()

        for process in self._processes:
            if not self._keep_alive:
                process.close()
            process.end_callback()

    def join(self, timeout=None):
        """Wait up to timeout seconds; return immediately if the thread never started."""
        if not self._started.is_set():
            return

        super().join(timeout)
