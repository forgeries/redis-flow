from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from rf import collector, default
from rf.collector import Collector


def test_long_job_is_not_evicted_by_later_jobs(monkeypatch):
    db = SimpleNamespace(renew_tasks=Mock())
    monkeypatch.setattr(collector, "RedisDB", lambda: db)
    monkeypatch.setattr(default, "PROCESS_THREAD_COUNT", 1)
    worker = Collector("unit")
    slow = {"job_redis": "slow"}
    worker.add_working_job(slow)
    for i in range(5):
        fast = {"job_redis": str(i)}
        worker.add_working_job(fast)
        worker.remove_working_job(fast)
    worker.heartbeat_once()
    assert [call.args[1] for call in db.renew_tasks.call_args_list] == [["slow"]]


def test_confirmation_stops_renewal_but_execution_end_does_not(monkeypatch):
    from test_buffers import MemoryDB, task

    from rf.buffer import task_buffer

    db = MemoryDB()
    db.renew_tasks = Mock()
    monkeypatch.setattr(collector, "RedisDB", lambda: db)
    monkeypatch.setattr(task_buffer, "RedisDB", lambda: db)
    worker, buffer = Collector("unit"), task_buffer.TaskBuffer("unit")
    job = {"job_redis": "source"}
    worker.add_working_job(job)
    buffer.put_job(task())
    buffer.put_del_job("source", on_confirm=lambda: worker.remove_working_job(job))
    db.failures = 1
    buffer.flush()
    worker.heartbeat_once()
    assert db.renew_tasks.call_count == 1
    buffer.flush()
    worker.heartbeat_once()
    assert db.renew_tasks.call_count == 1


def test_heartbeat_uses_bounded_batches_and_recovers_after_failure(monkeypatch):
    db = SimpleNamespace(renew_tasks=Mock())
    monkeypatch.setattr(collector, "RedisDB", lambda: db)
    monkeypatch.setattr(default, "HEARTBEAT_BATCH_SIZE", 2)
    worker = Collector("unit")
    for number in range(5):
        worker.add_working_job({"job_redis": str(number)})
    db.renew_tasks.side_effect = ConnectionError("Disconnected")
    with pytest.raises(ConnectionError):
        worker.heartbeat_once()
    db.renew_tasks.reset_mock(side_effect=True)
    worker.heartbeat_once()
    assert [call.args[1] for call in db.renew_tasks.call_args_list] == [
        ["0", "1"],
        ["2", "3"],
        ["4"],
    ]
    worker._running_jobs.clear()
    db.renew_tasks.reset_mock()
    worker.heartbeat_once()
    db.renew_tasks.assert_not_called()


def test_collector_pauses_claiming_while_buffers_are_full(monkeypatch):
    db = SimpleNamespace(zrevrangebyscore_set_score=Mock(return_value=[]))
    monkeypatch.setattr(collector, "RedisDB", lambda: db)
    allowed = False
    worker = Collector("unit", can_collect=lambda: allowed)
    worker._Collector__input_data()
    db.zrevrangebyscore_set_score.assert_not_called()
    allowed = True
    worker._Collector__input_data()
    db.zrevrangebyscore_set_score.assert_called_once()
