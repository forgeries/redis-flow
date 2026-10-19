import json
import threading
import time
from queue import Empty, Queue

import rf.default as default
import rf.utils.tools as tools
from rf.db.redisdb import RedisDB
from rf.struct import Task
from rf.utils.log import log


class Collector(threading.Thread):
    """Prefetch a bounded number of tasks and renew their Redis leases."""

    def __init__(self, redis_key, *, can_collect=None):
        """Create a collector for the task queue identified by redis_key."""

        super(Collector, self).__init__()
        default.validate_runtime_settings()
        self._can_collect = can_collect or (lambda: True)
        self._lease_duration = default.JOB_LOST_TIMEOUT
        self._heartbeat_interval = default.HEARTBEAT_INTERVAL
        self._heartbeat_batch_size = default.HEARTBEAT_BATCH_SIZE
        self._db = RedisDB()

        self._todo_jobs = Queue(maxsize=default.COLLECTOR_TASK_COUNT)
        self._running_jobs = {}
        self._jobs_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._heartbeat_event = threading.Event()
        self._heartbeat_thread = None
        self._is_collector_task = False

        self._table_job = default.TAB_JOB.format(redis_key=redis_key)

    def run(self):
        self.heartbeat_start()
        while not self._stop_event.is_set():
            try:
                self.__input_data()
            except Exception as e:
                log.exception("Cannot collect tasks: %s", e)
                time.sleep(0.1)

            self._is_collector_task = False

    def stop_collecting(self):
        self._stop_event.set()

    def stop(self):
        self.stop_collecting()
        self.heartbeat_stop()

    def __input_data(self):
        available = self._todo_jobs.maxsize - self._todo_jobs.qsize()
        if available <= 0 or not self._can_collect():
            self._stop_event.wait(0.1)
            return

        current_timestamp = tools.get_current_timestamp()

        # Claim due tasks by moving their scores to the lease deadline.
        jobs_list = self._db.zrevrangebyscore_set_score(
            self._table_job,
            priority_min=0,
            priority_max=current_timestamp,
            score=current_timestamp + self._lease_duration,
            count=available,
        )

        if jobs_list:
            self._is_collector_task = True

            self.__put_jobs(jobs_list)
        else:
            time.sleep(0.1)

    def __put_jobs(self, jobs_list):
        for job in jobs_list:
            try:
                job_dict = {
                    "job_obj": Task.from_dict(json.loads(job)),
                    "job_redis": job,
                }
            except Exception as e:
                log.exception("Cannot decode task data: %s. Task: %s", e, job)

                job_dict = None

            if job_dict:
                self.add_working_job(job_dict)
                self._todo_jobs.put(job_dict)

    def get_job(self):
        try:
            job = self._todo_jobs.get(timeout=1)
            return job
        except Empty:
            return None

    def get_jobs_count(self):
        """Return the prefetch count, or the Redis count through the lease horizon."""
        current_timestamp = tools.get_current_timestamp()
        priority_max = current_timestamp + self._lease_duration
        return (
            self._todo_jobs.qsize()
            or self._db.zget_count(self._table_job, 0, priority_max)
            or 0
        )

    def is_collector_task(self):
        return self._is_collector_task

    def add_working_job(self, job):
        # Keep leases alive from prefetch until Redis confirms source removal.
        with self._jobs_lock:
            self._running_jobs[job["job_redis"]] = job

    def remove_working_job(self, job):
        with self._jobs_lock:
            key = job["job_redis"]
            if self._running_jobs.get(key) is job:
                self._running_jobs.pop(key)

    def heartbeat_once(self):
        """Renew prefetched, executing, and unacknowledged tasks that still exist."""
        with self._jobs_lock:
            jobs = list(self._running_jobs.values())
        for offset in range(0, len(jobs), self._heartbeat_batch_size):
            self._db.renew_tasks(
                self._table_job,
                [
                    job["job_redis"]
                    for job in jobs[offset : offset + self._heartbeat_batch_size]
                ],
                tools.get_current_timestamp() + self._lease_duration,
            )

    def heartbeat(self):
        while not self._heartbeat_event.is_set():
            try:
                self.heartbeat_once()
            except Exception as exc:
                log.error("Cannot renew task leases: %s", exc)
            self._heartbeat_event.wait(self._heartbeat_interval)

    def heartbeat_start(self):
        self._heartbeat_thread = threading.Thread(
            target=self.heartbeat, name="rf-heartbeat"
        )
        self._heartbeat_thread.start()

    def heartbeat_stop(self):
        self._heartbeat_event.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join()
