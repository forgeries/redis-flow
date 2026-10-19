import schedule

from rf.base_process import BaseProcess
from rf.scheduler import Scheduler
from rf.utils.log import log
from rf.utils.redis_lock import RedisLock


class TaskFlow(BaseProcess, Scheduler):
    """Combine a process and scheduler, optionally submitting jobs daily at cron_hour."""

    def __init__(
        self,
        cron_hour=None,
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
        super().__init__(
            redis_key,
            thread_count,
            begin_callback,
            end_callback,
            delete_keys,
            keep_alive,
            auto_start_jobs,
            wait_lock,
            **kwargs,
        )
        if not self._processes:
            self._processes.append(self)
        self.cron_hour = cron_hour
        self._cron_scheduler = schedule.Scheduler()
        if self.cron_hour:
            self._keep_alive = True
            self._begin_callback = (
                begin_callback
                if begin_callback
                else lambda: log.info(
                    "Run the flow each day at %s:00 (local time).", self.cron_hour
                )
            )

    def _schedule_start_jobs(self):
        if not self.cron_hour:
            return super()._schedule_start_jobs()
        if self._auto_start_jobs:
            self._cron_scheduler.every().day.at(f"{self.cron_hour}:00").do(
                self._add_task_lock if self.wait_lock else self._add_task
            )

    def _tick(self):
        self._cron_scheduler.run_pending()

    def _add_task_lock(self):
        if self._shutdown_event.is_set():
            return
        with RedisLock(key=self._redis_key) as lock:
            if lock.locked:
                self._add_task()
