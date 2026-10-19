import json
from unittest.mock import Mock

import pytest
from test_buffers import MemoryDB

from rf import BaseProcess, Queue, Task
from rf.buffer import queue_buffer, task_buffer
from rf.process_control import ProcessControl


def test_executor_wires_destination_receipt_to_source_ack(monkeypatch):
    db = MemoryDB()
    monkeypatch.setattr(task_buffer, "RedisDB", lambda: db)
    monkeypatch.setattr(queue_buffer, "RedisDB", lambda: db)
    source, target = task_buffer.TaskBuffer("unit"), queue_buffer.QueueBuffer()
    collector = Mock()

    class Worker(BaseProcess):
        def work(self):
            yield Queue("destination", Task("next", {}))

    job = Task("work", {}, process_name="Worker", taskid="source")
    raw = json.dumps(job.to_dict)
    db.data["rf:job:unit"][raw] = 1
    execution = {"job_obj": job, "job_redis": raw}
    control = ProcessControl(collector, source, target)
    control.add_process(Worker())
    assert control.deal_job(execution)
    source.flush()
    assert raw in db.data["rf:job:unit"]
    collector.remove_working_job.assert_not_called()
    target.flush()
    source.flush()
    assert raw not in db.data["rf:job:unit"]
    collector.remove_working_job.assert_called_once_with(execution)


@pytest.fixture
def execution(monkeypatch):
    db = MemoryDB()
    monkeypatch.setattr(task_buffer, "RedisDB", lambda: db)
    monkeypatch.setattr(queue_buffer, "RedisDB", lambda: db)
    source = task_buffer.TaskBuffer("unit")
    target = queue_buffer.QueueBuffer()
    control = ProcessControl(Mock(), source, target)
    job = Task("work", {}, process_name="Worker", taskid="source")
    raw = json.dumps(job.to_dict)
    db.data["rf:job:unit"][raw] = 1
    return control, db, {"job_obj": job, "job_redis": raw}


@pytest.mark.parametrize("invalid", [{}, "wrong", 0, None])
def test_invalid_yield_goes_to_error_queue_only_after_write(execution, invalid):
    control, db, job = execution

    class Worker(BaseProcess):
        def work(self):
            yield Task("next", {})
            yield invalid

    control.add_process(Worker())
    assert control.deal_job(job)
    db.failures = 1
    assert not control._task_buffer.flush()
    assert job["job_redis"] in db.data["rf:job:unit"]
    control._collector.remove_working_job.assert_not_called()
    control._task_buffer.flush(raise_errors=True)
    assert job["job_redis"] not in db.data["rf:job:unit"]
    error = json.loads(next(iter(db.data["rf:err:unit"])))
    assert "must yield a Task or Queue" in error["meta"]["err_msg"]
    assert len(db.data["rf:job:unit"]) == 1


@pytest.mark.parametrize("invalid", [False, 0, "", {}])
def test_invalid_return_is_not_treated_as_success(execution, invalid):
    control, db, job = execution

    class Worker(BaseProcess):
        def work(self):
            return invalid

    control.add_process(Worker())
    assert control.deal_job(job)
    control._task_buffer.flush(raise_errors=True)
    assert db.data["rf:err:unit"]


def test_invalid_output_waits_for_prior_cross_queue_delivery(execution):
    control, db, job = execution

    class Worker(BaseProcess):
        def work(self):
            yield Queue("destination", Task("next", {}))
            yield "invalid"

    control.add_process(Worker())
    assert control.deal_job(job)
    control._task_buffer.flush()
    assert job["job_redis"] in db.data["rf:job:unit"]
    db.failures = 1
    assert not control._queue_buffer.flush()
    control._task_buffer.flush()
    assert job["job_redis"] in db.data["rf:job:unit"]
    control._queue_buffer.flush(raise_errors=True)
    control._task_buffer.flush(raise_errors=True)
    assert job["job_redis"] not in db.data["rf:job:unit"]
    assert db.data["destination"] and db.data["rf:err:unit"]


@pytest.mark.parametrize("callback_fails", [False, True])
def test_full_buffer_leaves_source_for_retry(execution, callback_fails):
    control, db, job = execution
    control._task_buffer.max_records = 1
    control._task_buffer.submit([Task("other", {})], wait=False)

    class Worker(BaseProcess):
        def work(self):
            if callback_fails:
                raise ValueError("Injected failure")
            yield Task("next", {})

    control.add_process(Worker())
    assert not control.deal_job(job)
    control._task_buffer.flush(raise_errors=True)
    assert job["job_redis"] in db.data["rf:job:unit"]
    assert not db.data["rf:err:unit"]
