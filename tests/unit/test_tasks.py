import json

from rf import Task
from rf.utils.tools import dumps_obj


def test_serialization_preserves_callable_and_wire_payload():
    def receive(value):
        return value

    task = Task(receive, {"value": 42}, process_name="Worker", taskid="source")

    first = task.to_dict
    second = task.to_dict

    assert task.callback is receive
    assert task.callback_name == "receive"
    assert (
        first
        == second
        == {
            "callback": "receive",
            "meta": {"value": 42},
            "process_name": "Worker",
            "taskid": "source",
        }
    )
    assert Task.from_dict(json.loads(json.dumps(first))).to_dict == first


def test_deserialization_preserves_legacy_pickle_input():
    payload = {"callback": "receive", "meta": dumps_obj({"value": 42})}
    original = payload.copy()

    task = Task.from_dict(payload)

    assert task.meta == {"value": 42}
    assert payload == original
    assert isinstance(payload["meta"], bytes)


def test_copy_does_not_mutate_source_or_share_nested_metadata():
    def receive(values):
        return values

    source = Task(receive, {"values": [1]}, taskid="source")
    copied = source.copy()
    copied.meta["values"].append(2)

    assert source.callback is receive
    assert source.meta == {"values": [1]}
    assert copied.callback == "receive"
    assert copied.taskid == source.taskid
