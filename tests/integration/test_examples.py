"""Exercise example entry points against an isolated Redis namespace."""

import importlib

import pytest


@pytest.mark.parametrize(
    "module, arguments, expected",
    [
        ("quickstart", ["--name", "Alice"], ["Hello, Alice!"]),
        (
            "submission",
            ["--count", "3"],
            [
                "Redis confirmed 3 tasks.",
                "Received item 0",
                "Received item 1",
                "Received item 2",
            ],
        ),
        (
            "branching",
            ["--count", "4"],
            [
                "Item 0: accepted",
                "Item 1: reworked",
                "Item 2: accepted",
                "Item 3: reworked",
            ],
        ),
        (
            "cross_queue",
            ["--count", "3"],
            ["Received order 0", "Received order 1", "Received order 2"],
        ),
        ("scheduled", ["--once"], ["Hello, redis-flow!"]),
    ],
)
def test_documented_example_finishes_and_confirms_tasks(
    components, namespace, monkeypatch, capsys, module, arguments, expected
):
    example = importlib.import_module("examples." + module)
    monkeypatch.setattr("sys.argv", [module, "--redis-key", namespace, *arguments])
    example.main()
    output = capsys.readouterr().out
    for message in expected:
        assert output.count(message) == 1
    client = components.get_redis_obj()
    # Check persistence after callbacks finish to catch incomplete acknowledgements.
    assert list(client.scan_iter(match="rf:job:" + namespace + "*")) == []
    assert list(client.scan_iter(match="rf:err:" + namespace + "*")) == []
