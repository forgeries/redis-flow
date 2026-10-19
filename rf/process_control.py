import threading
import traceback
from collections.abc import Iterable

import rf.utils.tools as tools
from rf.buffer.base import BufferFullError
from rf.struct import Queue, Task
from rf.utils.log import log


class ProcessControl(threading.Thread):
    """Execute callbacks and defer source removal until outputs are confirmed."""

    is_no_job = False

    def __init__(self, collector, task_buffer, queue_buffer):
        super(ProcessControl, self).__init__()
        self._collector = collector
        self._task_buffer = task_buffer
        self._queue_buffer = queue_buffer

        self._processes = []
        self._thread_stop = False

    def add_process(self, process):
        self._processes.append(process)

    def run(self):
        while not self._thread_stop:
            job = None
            awaiting_confirmation = False
            try:
                job = self._collector.get_job()
                if self._thread_stop:
                    break
                if not job:
                    if not self.is_no_job:
                        self.is_no_job = True
                    continue
                self.is_no_job = False
                self._collector.add_working_job(job)
                awaiting_confirmation = self.deal_job(job)
            except Exception as e:
                log.exception("Cannot process the task: %s", e)
            finally:
                if job is not None and not awaiting_confirmation:
                    self._collector.remove_working_job(job)

    def is_not_task(self):
        return self.is_no_job

    def deal_job(self, job):
        """Dispatch one task and return True if its acknowledgement has been buffered."""
        job_obj = job["job_obj"]
        receipts = []

        def confirm():
            self._task_buffer.put_del_job(
                job["job_redis"],
                dependencies=receipts,
                on_confirm=lambda: self._collector.remove_working_job(job),
            )

        for process in self._processes:
            if process.name == job_obj.process_name:
                try:
                    if job_obj.callback:
                        callback_parser = (
                            job_obj.callback
                            if callable(job_obj.callback)
                            else tools.get_method(process, job_obj.callback)
                        )
                        results = callback_parser(**job_obj.meta)
                    else:
                        results = process.process(**job_obj.meta)
                    if results is not None and (
                        not isinstance(results, Iterable)
                        or isinstance(results, (str, bytes, dict))
                    ):
                        raise TypeError(
                            f"{process.name}.{job_obj.callback or 'process'} "
                            "must return an iterable or None."
                        )
                    for task in results or []:
                        receipts[:] = [
                            event for event in receipts if not event.is_set()
                        ]
                        if isinstance(task, Task):
                            task.process_name = task.process_name or process.name
                            task.taskid = job_obj.taskid
                            if (
                                task.redis_key
                                or task.redis_ip_ports
                                or task.redis_user_pass
                                or task.redis_db
                            ):
                                queue = Queue(
                                    redis_key=task.redis_key,
                                    redis_ip_ports=task.redis_ip_ports,
                                    redis_user_pass=task.redis_user_pass,
                                    redis_db=task.redis_db,
                                    task=task,
                                )
                                receipts.append(self._queue_buffer.put_job(queue))
                            else:
                                self._task_buffer.put_job(task)
                        elif isinstance(task, Queue):
                            queue = task
                            task = queue.Task
                            task.process_name = task.process_name or process.name
                            task.taskid = job_obj.taskid
                            receipts.append(self._queue_buffer.put_job(queue))
                        else:
                            raise TypeError(
                                f"{process.name}.{job_obj.callback or 'process'} "
                                "must yield a Task or Queue. "
                                f"Received {type(task).__name__}."
                            )
                    confirm()
                    return True
                except BufferFullError:
                    # Keep the source in Redis and let its lease expire for retry.
                    log.warning(
                        "Buffer capacity reached. Leave the source task for retry."
                    )
                    return False
                except Exception as e:
                    log.error("Task callback failed:\n%s", traceback.format_exc())
                    result = process.err_job(job_obj, e)
                    if result:
                        if isinstance(result, Task):
                            result.process_name = result.process_name or process.name
                            try:
                                self._task_buffer.put_err_job(
                                    result,
                                    source=job["job_redis"],
                                    dependencies=receipts,
                                    on_confirm=lambda: (
                                        self._collector.remove_working_job(job)
                                    ),
                                )
                                return True
                            except BufferFullError:
                                log.warning(
                                    "Error buffer is full. Leave the source task for retry."
                                )
                                return False

    def stop(self):
        self._thread_stop = True
