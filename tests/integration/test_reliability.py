import json
import os
import subprocess
import sys
import threading
import time

import pytest
from redis.exceptions import ResponseError

from rf import BaseProcess, Queue, Task, TaskFlow
from rf.buffer import QueueBuffer, TaskBuffer
from rf.db.redisdb import RedisDB
from rf.utils.redis_lock import RedisLock


def wait_until(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("Condition did not become true before timeout")


def test_atomic_commit_and_failed_queue_type(redis_db, namespace):
    client = redis_db.get_redis_obj()
    key, error_key = "rf:job:" + namespace, "rf:err:" + namespace
    client.zadd(key, {"source": 1})
    client.set(error_key, "wrong type")
    with pytest.raises(ResponseError):
        redis_db.commit_tasks(
            key, error_key, [("child", 2)], ["source"], [("error", 2)]
        )
    assert client.zscore(key, "source") == 1
    assert client.zscore(key, "child") is None
    client.delete(error_key)
    redis_db.commit_tasks(key, error_key, [("child", 2)], ["source"], [("error", 2)])
    assert client.zscore(key, "source") is None
    assert client.zscore(key, "child") == 2
    assert client.zscore(error_key, "error") == 2


def test_retry_does_not_reset_an_existing_lease(redis_db, namespace):
    key, error_key = "rf:job:" + namespace, "rf:err:" + namespace
    client = redis_db.get_redis_obj()
    redis_db.commit_tasks(key, error_key, [("child", 1)], [], [])
    client.zadd(key, {"child": 9999999999})
    redis_db.commit_tasks(key, error_key, [("child", 1)], [], [])
    redis_db.zadd_if_absent(key, ["child"], [1])
    assert client.zscore(key, "child") == 9999999999


def test_ambiguous_commit_response_retains_batch(components, namespace, monkeypatch):
    buffer = TaskBuffer(namespace)
    client = components.get_redis_obj()
    key = "rf:job:" + namespace
    client.zadd(key, {"source": 1})
    buffer.put_job(Task("work", {}, taskid="child"))
    confirmed = threading.Event()
    buffer.put_del_job("source", on_confirm=confirmed.set)
    original = components.commit_tasks

    def lost_response(*args):
        original(*args)
        raise ConnectionError("Redis committed; client lost reply")

    monkeypatch.setattr(components, "commit_tasks", lost_response)
    assert buffer.flush() is False
    assert buffer.get_jobs_count() > 0
    assert not confirmed.is_set()
    monkeypatch.setattr(components, "commit_tasks", original)
    assert buffer.flush()
    assert confirmed.is_set()
    assert client.zcard(key) == 1


@pytest.mark.parametrize("action", ["release", "renew"])
def test_old_lock_owner_cannot_modify_new_owner(redis_db, namespace, action):
    client = redis_db.get_redis_obj()
    old = RedisLock(namespace, redis_cli=client)
    new = RedisLock(namespace, redis_cli=client)
    assert old.acquire()
    # Replace the ownership token directly to simulate expiration deterministically.
    client.delete(old.lock_key)
    assert new.acquire()
    token = client.get(new.lock_key)
    assert not getattr(old, action)()
    assert client.get(new.lock_key) == token
    assert new.renew()
    assert new.release()


def test_cross_queue_destination_failure_does_not_ack_source(
    components, namespace, monkeypatch
):
    source = TaskBuffer(namespace)
    target = QueueBuffer()
    destination = RedisDB(url=os.environ["RF_TEST_REDIS_URL"])
    key, dest_key = "rf:job:" + namespace, "destination:" + namespace
    components.get_redis_obj().zadd(key, {"source": 1})
    job = Queue(dest_key, Task("work", {}, taskid="child"))
    job.RedisDB = destination
    receipt = target.put_job(job)
    source.put_del_job("source", dependencies=[receipt])
    original = destination.zadd_if_absent

    def unavailable(*args):
        raise ConnectionError("destination unavailable")

    monkeypatch.setattr(destination, "zadd_if_absent", unavailable)
    assert target.flush() is False
    source.flush()
    assert components.get_redis_obj().zscore(key, "source") is not None
    monkeypatch.setattr(destination, "zadd_if_absent", original)
    target.flush()
    source.flush()
    assert destination.get_redis_obj().zcard(dest_key) == 1
    assert components.get_redis_obj().zscore(key, "source") is None


def test_cron_flow_can_stop_without_leaking_global_jobs(components, namespace):
    import schedule

    global_jobs = list(schedule.jobs)
    flow = TaskFlow(redis_key=namespace, cron_hour="23", auto_start_jobs=True)
    started = threading.Event()
    flow._begin_callback = started.set
    flow.start()
    try:
        assert started.wait(5)
        flow.stop()
        flow.join(8)
        assert not flow.is_alive()
        assert schedule.jobs == global_jobs
        assert not flow._collector._heartbeat_thread.is_alive()
    finally:
        flow.stop()
        flow.join(8)


def test_graceful_stop_drains_outputs_and_joins_threads(components, namespace):
    entered, finish = threading.Event(), threading.Event()
    flow = TaskFlow(redis_key=namespace, keep_alive=True, auto_start_jobs=False)

    class Worker(BaseProcess):
        def work(self):
            entered.set()
            assert finish.wait(5)
            yield Task("next", {}, taskid="next")

    flow.add_process(Worker)
    source = Task("work", {}, process_name="Worker", taskid="source")
    components.get_redis_obj().zadd(
        "rf:job:" + namespace, {json.dumps(source.to_dict): time.time()}
    )
    flow.start()
    try:
        assert entered.wait(5)
        flow.stop()
        flow.join(0.01)
        assert flow.is_alive(), "join(timeout) must return while callback is running"
        finish.set()
        flow.join(8)
        assert not flow.is_alive()
        assert not flow._collector.is_alive()
        assert not flow._collector._heartbeat_thread.is_alive()
        assert all(not worker.is_alive() for worker in flow._process_controls)
        assert not flow._task_buffer.is_alive()
        assert not flow._queue_buffer.is_alive()
        values = components.get_redis_obj().zrange("rf:job:" + namespace, 0, -1)
        assert len(values) == 1
        assert json.loads(values[0])["callback"] == "next"
    finally:
        finish.set()
        flow.stop()
        flow.join(8)


def test_worker_kill_makes_unconfirmed_job_available_again(redis_db, namespace):
    key = "rf:job:" + namespace
    ready = "ready:" + namespace
    value = json.dumps(Task("work", {}, taskid="crash").to_dict)
    redis_db.get_redis_obj().zadd(key, {value: time.time()})
    code = """
import os, time
from rf import collector, default
from rf.db.redisdb import RedisDB
db = RedisDB(url=os.environ["RF_TEST_REDIS_URL"])
collector.RedisDB = lambda: db
default.JOB_LOST_TIMEOUT = 1
default.HEARTBEAT_INTERVAL = 0.1
default.REDIS_SOCKET_TIMEOUT = 0.1
default.REDIS_CONNECT_TIMEOUT = 0.1
c = collector.Collector(os.environ["RF_TEST_NAMESPACE"])
c.start()
job = c.get_job()
if job is None:
    raise RuntimeError("No task acquired")
db.get_redis_obj().set("ready:" + os.environ["RF_TEST_NAMESPACE"], "1")
time.sleep(60)
"""
    env = dict(os.environ, RF_TEST_NAMESPACE=namespace)
    child = subprocess.Popen(
        [sys.executable, "-c", code],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        wait_until(lambda: redis_db.get_redis_obj().exists(ready))
        assert redis_db.get_redis_obj().zscore(key, value) is not None
        child.kill()
        child.wait(timeout=5)
        recovered = []

        def claim():
            recovered.extend(
                redis_db.zrevrangebyscore_set_score(
                    key, 0, time.time(), time.time() + 30, count=1
                )
            )
            return recovered

        wait_until(claim)
        assert recovered == [value]
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)


def test_submit_records_reach_redis_before_return(components, namespace):
    flow = TaskFlow(redis_key=namespace, auto_start_jobs=False)
    receipt = flow.submit([Task("work", {"number": number}) for number in range(10)])
    assert receipt.is_set()
    values = components.get_redis_obj().zrange("rf:job:" + namespace, 0, -1)
    assert len(values) == 10
    assert {json.loads(value)["meta"]["number"] for value in values} == set(range(10))


def test_submit_response_loss_can_be_retried_without_resetting_scores(
    components, namespace, monkeypatch
):
    flow = TaskFlow(redis_key=namespace, auto_start_jobs=False)
    task = Task("work", {})
    original = components.commit_tasks

    def lost_response(*args):
        original(*args)
        raise ConnectionError("Response lost after commit")

    monkeypatch.setattr(components, "commit_tasks", lost_response)
    with pytest.raises(ConnectionError):
        flow.submit([task])
    client = components.get_redis_obj()
    key = "rf:job:" + namespace
    raw = client.zrange(key, 0, -1)[0]
    client.zadd(key, {raw: 9999999999})
    monkeypatch.setattr(components, "commit_tasks", original)
    assert flow.submit([task]).is_set()
    assert client.zcard(key) == 1
    assert client.zscore(key, raw) == 9999999999


def test_async_submission_is_confirmed_by_running_flow(components, namespace):
    flow = TaskFlow(redis_key=namespace, auto_start_jobs=False, keep_alive=True)
    executed = threading.Event()

    class Worker(BaseProcess):
        def work(self):
            executed.set()

    flow.add_process(Worker)
    flow.start()
    try:
        receipt = flow.submit([Task("work", {}, process_name="Worker")], wait=False)
        assert receipt.wait(5)
        assert executed.wait(5)
    finally:
        flow.stop()
        flow.join(8)
    assert not flow.is_alive()


def test_batched_heartbeat_never_recreates_completed_tasks(redis_db, namespace):
    client = redis_db.get_redis_obj()
    key = "rf:job:" + namespace
    client.zadd(key, {"active": 1, "completed": 1})
    client.zrem(key, "completed")
    redis_db.renew_tasks(key, ["active", "completed", "missing"], 100)
    assert client.zscore(key, "active") == 100
    assert client.zcard(key) == 1


def test_heartbeat_recovers_after_short_connection_failure(
    components, namespace, monkeypatch
):
    from rf import collector

    worker = collector.Collector(namespace)
    client = components.get_redis_obj()
    key = "rf:job:" + namespace
    client.zadd(key, {"active": 130})
    worker.add_working_job({"job_redis": "active"})
    original = components.renew_tasks

    def unavailable(*args):
        raise ConnectionError("Temporary connection failure")

    monkeypatch.setattr(components, "renew_tasks", unavailable)
    with pytest.raises(ConnectionError):
        worker.heartbeat_once()
    assert client.zscore(key, "active") == 130
    monkeypatch.setattr(components, "renew_tasks", original)
    monkeypatch.setattr(collector.tools, "get_current_timestamp", lambda: 110)
    worker.heartbeat_once()
    assert client.zscore(key, "active") == 140
    assert components.zrevrangebyscore_set_score(key, 0, 131, 161, count=1) == []
