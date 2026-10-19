import json
import threading
from collections import defaultdict

import pytest

from rf import Queue, Task
from rf.buffer import queue_buffer, task_buffer


class MemoryDB:
    def __init__(self):
        self.data = defaultdict(dict)
        self.failures = 0
        self.before_write = None
        self.calls = 0

    def check(self):
        self.calls += 1
        if self.before_write:
            self.before_write()
        if self.failures:
            self.failures -= 1
            raise ConnectionError("injected write failure")

    def zadd(self, key, values, scores):
        self.check()
        self.data[key].update(zip(values, scores))

    def zadd_if_absent(self, key, values, scores):
        self.check()
        for value, score in zip(values, scores):
            self.data[key].setdefault(value, score)

    def zrem(self, key, values):
        self.check()
        for value in values:
            self.data[key].pop(value, None)

    def commit_tasks(self, key, error_key, jobs, done, errors):
        self.check()
        for value, score in jobs:
            self.data[key].setdefault(value, score)
        for value, score in errors:
            self.data[error_key].setdefault(value, score)
        added = {value for value, _ in jobs}
        for value in done:
            if value not in added:
                self.data[key].pop(value, None)


@pytest.fixture
def db(monkeypatch):
    instance = MemoryDB()
    monkeypatch.setattr(task_buffer, "RedisDB", lambda: instance)
    monkeypatch.setattr(queue_buffer, "RedisDB", lambda: instance)
    return instance


def task(number=1):
    return Task("work", {"number": number}, process_name="Worker", taskid=str(number))


@pytest.mark.parametrize("kind", ["task", "queue"])
def test_failed_write_survives_next_flush(db, kind):
    child = task()
    if kind == "task":
        buffer = task_buffer.TaskBuffer("unit")
        key = "rf:job:unit"
        buffer.put_job(child)
    else:
        buffer = queue_buffer.QueueBuffer()
        key = "rf:job:other"
        buffer.put_job(Queue(key, child))
    db.failures = 1
    buffer.flush()
    assert buffer.get_jobs_count() > 0
    assert not buffer.is_adding_to_db()
    assert not db.data[key]
    buffer.flush()
    assert json.dumps(child.to_dict) in db.data[key]
    assert buffer.get_jobs_count() == 0


def test_failed_child_write_cannot_be_bypassed_by_ack(db):
    buffer = task_buffer.TaskBuffer("unit")
    key = "rf:job:unit"
    db.data[key]["source"] = 1
    child = task()
    buffer.put_job(child)
    buffer.put_del_job("source")
    db.failures = 1
    buffer.flush()
    assert "source" in db.data[key]
    buffer.flush()
    assert json.dumps(child.to_dict) in db.data[key]
    assert "source" not in db.data[key]


def test_error_write_failure_preserves_source(db):
    buffer = task_buffer.TaskBuffer("unit")
    db.data["rf:job:unit"]["source"] = 1
    buffer.put_err_job(task())
    buffer.put_del_job("source")
    db.failures = 1
    buffer.flush()
    assert "source" in db.data["rf:job:unit"]
    buffer.flush()
    assert db.data["rf:err:unit"]
    assert "source" not in db.data["rf:job:unit"]


def test_arrivals_during_flush_wait_for_next_batch(db):
    buffer = task_buffer.TaskBuffer("unit")
    buffer.put_job(task(1))
    db.data["rf:job:unit"]["source2"] = 1

    def arrive():
        db.before_write = None
        buffer.put_job(task(2))
        buffer.put_del_job("source2")

    db.before_write = arrive
    buffer.flush()
    assert "source2" in db.data["rf:job:unit"]
    assert json.dumps(task(2).to_dict) not in db.data["rf:job:unit"]
    buffer.flush()
    assert "source2" not in db.data["rf:job:unit"]
    assert json.dumps(task(2).to_dict) in db.data["rf:job:unit"]


def test_concurrent_flushes_do_not_overtake_failed_batch(db):
    buffer = task_buffer.TaskBuffer("unit")
    buffer.put_job(task())
    buffer.put_del_job("source")
    db.data["rf:job:unit"]["source"] = 1
    entered, release = threading.Event(), threading.Event()

    def block():
        entered.set()
        assert release.wait(3)

    db.before_write = block
    db.failures = 1
    first = threading.Thread(target=buffer.flush)
    second = threading.Thread(target=buffer.flush)
    first.start()
    assert entered.wait(3)
    second.start()
    release.set()
    first.join(3)
    second.join(3)
    assert not first.is_alive() and not second.is_alive()
    assert json.dumps(task().to_dict) in db.data["rf:job:unit"]
    assert "source" not in db.data["rf:job:unit"]


def test_cross_queue_failure_keeps_source_until_destination_confirms(db):
    source, target = task_buffer.TaskBuffer("unit"), queue_buffer.QueueBuffer()
    db.data["rf:job:unit"]["source"] = 1
    receipt = target.put_job(Queue("destination", task()))
    confirmed = threading.Event()
    source.put_del_job("source", dependencies=[receipt], on_confirm=confirmed.set)
    source.flush()
    assert not confirmed.is_set()
    db.failures = 1
    target.flush()
    source.flush()
    assert "source" in db.data["rf:job:unit"]
    target.flush()
    source.flush()
    assert db.data["destination"]
    assert "source" not in db.data["rf:job:unit"]
    assert confirmed.is_set()


def test_serialization_failure_is_reported_before_acceptance(db):
    buffer = task_buffer.TaskBuffer("unit")
    invalid = task()
    invalid.meta = {"bad": object()}
    with pytest.raises(TypeError):
        buffer.put_job(invalid)
    assert buffer.get_jobs_count() == 0


def test_flush_can_propagate_errors_and_retry(db):
    buffer = task_buffer.TaskBuffer("unit")
    buffer.put_job(task())
    db.failures = 1
    with pytest.raises(ConnectionError):
        buffer.flush(raise_errors=True)
    assert not buffer.is_adding_to_db()
    assert buffer.flush(raise_errors=True)


def test_cross_queue_retries_only_unconfirmed_destination_groups(db):
    second_db = MemoryDB()
    buffer = queue_buffer.QueueBuffer()
    first_receipt = buffer.put_job(Queue("first", task(1)))
    second_job = Queue("second", task(2))
    second_job.RedisDB = second_db
    second_receipt = buffer.put_job(second_job)
    second_db.failures = 1
    assert buffer.flush() is False
    assert first_receipt.is_set()
    assert not second_receipt.is_set()
    assert buffer.flush()
    assert second_receipt.is_set()
    assert db.calls == 1
    assert second_db.calls == 2


def test_initial_batch_keeps_batching_benefit(db):
    buffer = task_buffer.TaskBuffer("unit")
    for number in range(100):
        buffer.put_job(task(number))
    assert db.calls == 0
    buffer.flush()
    assert db.calls == 1
    assert len(db.data["rf:job:unit"]) == 100


def test_submit_confirms_its_batch_after_an_older_failed_snapshot(db):
    buffer = task_buffer.TaskBuffer("unit")
    buffer.put_job(task(1))
    db.failures = 1
    assert not buffer.flush()
    receipt = buffer.submit([task(2), task(3)])
    assert receipt.is_set()
    assert len(db.data["rf:job:unit"]) == 3
    assert db.calls == 3


def test_submit_validates_the_whole_batch_before_acceptance(db):
    buffer = task_buffer.TaskBuffer("unit")
    invalid = task(2)
    invalid.meta = {"bad": object()}
    with pytest.raises(TypeError):
        buffer.submit([task(1), invalid])
    assert buffer.get_jobs_count() == 0
    assert db.calls == 0


def test_submit_response_loss_does_not_confirm_or_discard_the_batch(db):
    buffer = task_buffer.TaskBuffer("unit")
    receipt = buffer.submit([task()], wait=False)
    original = db.commit_tasks

    def lost_response(*args):
        original(*args)
        raise ConnectionError("Response lost after commit")

    db.commit_tasks = lost_response
    with pytest.raises(ConnectionError):
        buffer.flush(raise_errors=True)
    assert not receipt.is_set()
    assert buffer.get_jobs_count() == 1
    db.commit_tasks = original
    buffer.flush(raise_errors=True)
    assert receipt.is_set()
    assert len(db.data["rf:job:unit"]) == 1


def test_waiting_submit_propagates_write_errors(db):
    buffer = task_buffer.TaskBuffer("unit")
    db.failures = 1
    with pytest.raises(ConnectionError):
        buffer.submit([task()])
    assert buffer.get_jobs_count() == 1
    buffer.flush(raise_errors=True)
    assert len(db.data["rf:job:unit"]) == 1


def test_concurrent_submit_waits_for_each_own_confirmation(db):
    from concurrent.futures import ThreadPoolExecutor

    buffer = task_buffer.TaskBuffer("unit")
    entered, release, accepted = threading.Event(), threading.Event(), threading.Event()

    def block_first_write():
        db.before_write = None
        entered.set()
        assert release.wait(3)

    db.before_write = block_first_write
    original_append = buffer._append

    def observe_acceptance(queue, items):
        original_append(queue, items)
        if buffer.get_jobs_count() == 2:
            accepted.set()

    buffer._append = observe_acceptance
    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(buffer.submit, [task(1)])
        assert entered.wait(3)
        second = workers.submit(buffer.submit, [task(2)])
        try:
            assert accepted.wait(3)
            assert not second.done()
        finally:
            release.set()
        assert first.result(timeout=3).is_set()
        assert second.result(timeout=3).is_set()
    assert len(db.data["rf:job:unit"]) == 2


@pytest.mark.parametrize("kind", ["task", "queue"])
def test_capacity_includes_failed_writes_and_recovers(db, monkeypatch, kind):
    from rf import BufferFullError, default

    monkeypatch.setattr(default, "BUFFER_MAX_RECORDS", 2)
    if kind == "task":
        buffer = task_buffer.TaskBuffer("unit")
        make_job = task
    else:
        buffer = queue_buffer.QueueBuffer()

        def make_job(number):
            return Queue("destination", task(number))

    db.failures = 2
    buffer.put_job(make_job(1))
    buffer.put_job(make_job(2))
    for number in range(3, 20):
        with pytest.raises(BufferFullError):
            buffer.put_job(make_job(number))
    assert buffer.get_jobs_count() == 2
    assert not buffer.flush()
    assert buffer.get_jobs_count() == 2
    assert buffer.flush()
    buffer.put_job(make_job(3))
    assert buffer.flush()
    assert buffer.get_jobs_count() == 0


def test_capacity_rejects_entire_submission(db, monkeypatch):
    from rf import BufferFullError, default

    monkeypatch.setattr(default, "BUFFER_MAX_RECORDS", 2)
    buffer = task_buffer.TaskBuffer("unit")
    buffer.submit([task(1)], wait=False)
    with pytest.raises(BufferFullError):
        buffer.submit([task(2), task(3)], wait=False)
    with pytest.raises(BufferFullError):
        buffer.submit(task(number) for number in range(3))
    assert buffer.get_jobs_count() == 1
    buffer.flush()
    assert list(db.data["rf:job:unit"]) == [json.dumps(task(1).to_dict)]


def test_concurrent_producers_cannot_exceed_capacity(db, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from rf import BufferFullError, default

    monkeypatch.setattr(default, "BUFFER_MAX_RECORDS", 3)
    buffer = task_buffer.TaskBuffer("unit")

    def submit(number):
        try:
            buffer.submit([task(number)], wait=False)
            return True
        except BufferFullError:
            return False

    with ThreadPoolExecutor(max_workers=8) as workers:
        assert sum(workers.map(submit, range(40))) == 3
    assert buffer.get_jobs_count() == 3


def test_full_ack_buffer_can_drain_after_destination_recovers(db, monkeypatch):
    from rf import BufferFullError, default

    monkeypatch.setattr(default, "BUFFER_MAX_RECORDS", 1)
    source, destination = task_buffer.TaskBuffer("unit"), queue_buffer.QueueBuffer()
    db.data["rf:job:unit"]["source"] = 1
    db.failures = 1
    receipt = destination.put_job(Queue("destination", task()))
    source.put_del_job("source", dependencies=[receipt])
    with pytest.raises(BufferFullError):
        source.put_job(task(2))
    assert source.flush()
    assert source.is_full()
    destination.flush(raise_errors=True)
    source.flush(raise_errors=True)
    assert not source.is_full()
    assert "source" not in db.data["rf:job:unit"]


def test_error_and_ack_fit_one_record_and_retain_source_on_failure(db, monkeypatch):
    from rf import default

    monkeypatch.setattr(default, "BUFFER_MAX_RECORDS", 1)
    buffer = task_buffer.TaskBuffer("unit")
    db.data["rf:job:unit"]["source"] = 1
    confirmed = threading.Event()
    buffer.put_err_job(task(), source="source", on_confirm=confirmed.set)
    assert buffer.get_jobs_count() == 1
    db.failures = 1
    assert not buffer.flush()
    assert "source" in db.data["rf:job:unit"]
    assert not confirmed.is_set()
    buffer.flush(raise_errors=True)
    assert confirmed.is_set()
    assert "source" not in db.data["rf:job:unit"]
    assert db.data["rf:err:unit"]
