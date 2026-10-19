# redis-flow

English | [简体中文](README.zh.markdown)

redis-flow is a lightweight Python library for task workflows backed by Redis.
Use `yield Task(...)` to create follow-up tasks. Workers run callbacks in threads.
Memory buffers group writes into batches to reduce Redis requests.
Redis keeps tasks until the worker confirms their completion.

The project is at version 0.x. Tasks can run more than once.
Design application actions so that repeated execution does not cause incorrect results.
Initial tasks can be lost if the process stops before the buffer writes them to Redis.
Use `flow.submit(tasks)` to wait for Redis confirmation before continuing.
See [Reliability and limits](docs/reliability.markdown) for details.

## Quick start

You need Python 3.10 or later and a standalone Redis server.
Run these commands from the repository root:

```sh
python -m venv .venv
```

Activate the environment:

```sh
# Linux or macOS
source .venv/bin/activate
```

```powershell
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

Install the library. Start a development Redis server. Then run the example:

```sh
python -m pip install -e .
docker run --rm -d --name redis-flow-dev -p 127.0.0.1:6379:6379 redis:7-alpine
python -m examples.quickstart
```

The output includes `Hello, redis-flow!`.
The program exits after it completes the tasks and writes the completion records to Redis.
Run `docker stop redis-flow-dev` when you finish.
This container is for development. It has no persistence setup for production use.

## Define a flow

```python
from rf import Task, TaskFlow


class HelloFlow(TaskFlow):
    def start_jobs(self):
        yield Task(callback=self.greet, meta={"name": "redis-flow"})

    def greet(self, name):
        print(f"Hello, {name}!")


if __name__ == "__main__":
    flow = HelloFlow(redis_key="hello-flow")
    flow.start()
    try:
        while flow.is_alive():
            flow.join(0.2)
    except KeyboardInterrupt:
        flow.stop()
        while flow.is_alive():
            flow.join(0.2)
```

The default Redis address is `127.0.0.1:6379`, with database `0`.
Set `REDIS_IP_PORTS`, `REDIS_USER_PASS`, and `REDIS_DB` to change the connection.
You can also use an importable `settings.py` module.
See [Default settings](rf/default.py) for all options.
Do not commit real passwords or other credentials.

## Submit a confirmed batch

Use `submit()` when the producer must wait for Redis to accept its tasks:

```python
flow = HelloFlow(redis_key="hello-flow", auto_start_jobs=False)
receipt = flow.submit([Task("greet", {"name": "Alice"})])
assert receipt.is_set()
```

This writes the tasks without starting workers. Start the flow separately to process them.
`submit()` accepts local `Task` objects. Set `process_name` if the flow has multiple registered processes.
Each batch must fit the available buffer capacity. The default capacity is 1,000 records per buffer.
`BufferFullError` rejects the whole new batch. Invalid task data also rejects the whole batch.
A Redis write error raises but keeps accepted tasks for retry; the write may already have reached Redis.
Call `flow.flush()` to retry retained writes after recovery, including before workers start.
While a flow is running, `submit(tasks, wait=False)` returns an event that is set after Redis confirms the batch.
Stop external producers before stopping the flow.
Redis restart durability still depends on its persistence settings. Confirmation does not mean the tasks have finished.
See the [submission example](examples/submission.py) for a complete program.

## How tasks run

1. `start_jobs()` creates initial tasks and adds them to a buffer.
2. The buffer writes a batch to Redis. The collector claims tasks and sets their lease deadlines atomically.
3. A worker calls the task method. The method can yield `Task` or `Queue` objects.
4. The task buffer writes outputs and confirms completion. Outputs for another queue must reach their destination first.
5. Heartbeats renew leases until Redis confirms completion. After a process stops, another worker can claim tasks when their leases expire.

To use multiple process classes, inherit from `BaseProcess` and register each class with `flow.add_process(...)`.
Workers with different registered process classes should use separate `redis_key` namespaces.
This prevents a worker from claiming tasks that it cannot handle.
See the [Examples guide](examples/README.markdown) for branches, transfers between queues, and daily schedules.

## Development

```sh
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m pytest tests/unit -q
```

See [Contributing](CONTRIBUTING.markdown) for Redis integration tests, package checks, and release requirements.
See the [Changelog](CHANGELOG.markdown) for changes and API migration notes.

## License and release status

This project is licensed under the [MIT License](LICENSE).
Package publication is disabled until the release requirements are complete.
