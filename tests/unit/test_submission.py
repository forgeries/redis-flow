import json

import pytest
from test_buffers import MemoryDB

from rf import BaseProcess, Task, TaskFlow


@pytest.fixture
def flow(monkeypatch):
    from rf import collector, scheduler
    from rf.buffer import queue_buffer, task_buffer

    db = MemoryDB()
    for module in (collector, scheduler, queue_buffer, task_buffer):
        monkeypatch.setattr(module, "RedisDB", lambda: db)
    return TaskFlow(redis_key="unit", auto_start_jobs=False), db


def test_public_submit_confirms_without_starting_workers(flow):
    worker, db = flow
    tasks = [Task("work", {"number": number}) for number in range(10)]
    receipt = worker.submit(tasks)
    assert receipt.is_set()
    assert db.calls == 1
    assert len(db.data["rf:job:unit"]) == 10
    assert tasks[0].process_name is None
    assert all(
        json.loads(value)["process_name"] == "TaskFlow"
        for value in db.data["rf:job:unit"]
    )


def test_async_submit_requires_running_workers(flow):
    worker, db = flow
    with pytest.raises(RuntimeError, match="running flow"):
        worker.submit([Task("work", {})], wait=False)
    assert db.calls == 0
    assert worker._task_buffer.get_jobs_count() == 0


def test_submission_rejects_routing_before_acceptance(flow):
    worker, db = flow
    with pytest.raises(ValueError, match="this flow's queue"):
        worker.submit([Task("work", {}), Task("other", {}, redis_key="other")])
    assert db.calls == 0
    assert worker._task_buffer.get_jobs_count() == 0


def test_submission_requires_process_name_when_ambiguous(flow):
    worker, db = flow
    worker.add_process(BaseProcess)
    with pytest.raises(ValueError, match="process_name"):
        worker.submit([Task("work", {})])
    assert worker.submit([Task("work", {}, process_name="BaseProcess")]).is_set()
    assert len(db.data["rf:job:unit"]) == 1


def test_public_flush_recovers_full_failed_submission_without_workers(flow):
    worker, db = flow
    worker._task_buffer.max_records = 1
    db.failures = 1
    with pytest.raises(ConnectionError):
        worker.submit([Task("work", {})])
    assert worker._task_buffer.is_full()
    worker.flush()
    assert not worker._task_buffer.is_full()
    assert len(db.data["rf:job:unit"]) == 1


def test_public_flush_drains_failed_snapshot_and_newer_batch(flow):
    worker, db = flow
    first = worker._task_buffer.submit([Task("first", {})], wait=False)
    db.failures = 1
    with pytest.raises(ConnectionError):
        worker.flush()
    second = worker._task_buffer.submit([Task("second", {})], wait=False)
    worker.flush()
    assert first.is_set() and second.is_set()
    assert len(db.data["rf:job:unit"]) == 2
