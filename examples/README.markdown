# Examples

Run each example from the **repository root** with `python -m examples.<name>`.
Each example demonstrates one part of the API.
Importing an example does not connect to Redis or start threads.

| Example | Purpose | Command | Exit behavior |
| --- | --- | --- | --- |
| [quickstart.py](quickstart.py) | Define and run one task | `python -m examples.quickstart` | Exit after completion |
| [submission.py](submission.py) | Confirm task submission before starting workers | `python -m examples.submission --count 3` | Exit after completion |
| [branching.py](branching.py) | Branch tasks and join the paths | `python -m examples.branching --count 6` | Exit after completion |
| [cross_queue.py](cross_queue.py) | Register processes and transfer tasks between queues | `python -m examples.cross_queue --count 4` | Exit after completion by default |
| [scheduled.py](scheduled.py) | Run a flow each day | `python -m examples.scheduled --at 20:30` | Press Ctrl+C |

## Set up Redis

You need Python 3.10 or later, the installed project, and a standalone Redis server.
Use a separate Redis instance for these examples:

```sh
python -m pip install -e .
docker run --rm -d --name rf-examples-redis -p 127.0.0.1:16379:6379 redis:7-alpine
```

On Linux or macOS:

```sh
export REDIS_IP_PORTS=127.0.0.1:16379
python -m examples.quickstart
```

On Windows PowerShell:

```powershell
$env:REDIS_IP_PORTS = '127.0.0.1:16379'
python -m examples.quickstart
```

Set `REDIS_USER_PASS` and `REDIS_DB` before you run an example if you need a password or another database.
The examples use environment variables. They do not need `examples/settings.py`.
The default Redis address is `127.0.0.1:6379`.
Run `docker stop rf-examples-redis` when you finish.

## Run one task

```sh
python -m examples.quickstart --name Alice
```

The output includes `Hello, Alice!`.
`start_jobs()` creates the initial task. `greet()` handles it.

## Confirm submission before processing

```sh
python -m examples.submission --count 3
```

The example calls `flow.submit(...)` with small batches before starting workers.
It prints `Redis confirmed 3 tasks.` only after Redis accepts all three tasks.
Workers then print `Received item 0` through `Received item 2`, in any order.
`auto_start_jobs=False` prevents additional startup tasks.
Each submitted batch must fit the available buffer capacity.
A connection error can leave a batch accepted but unconfirmed. Repeated submissions can cause repeated execution.

## Branch and join

```sh
python -m examples.branching --count 6
```

Each item passes through `inspect`.
Even item IDs take the `accept` path. Odd IDs take the `rework` path.
Both paths create a `finish` task.
The output includes these lines:

```text
Item 0: accepted
Item 1: reworked
Item 2: accepted
Item 3: reworked
Item 4: accepted
Item 5: reworked
```

Workers use multiple threads. The output order can differ from the item ID order.

## Transfer tasks between queues

```sh
python -m examples.cross_queue --count 4
```

By default, the producer first writes all tasks to the destination queue.
The consumer then runs and exits after it completes the tasks.
The output includes `Received order 0` through `Received order 3`, in any order.
This shows that Redis keeps submitted tasks before the consumer starts.

You can also use two terminals. Use the same Redis settings and `--redis-key` value in both terminals:

```sh
# Terminal 1: Receive tasks. Press Ctrl+C to stop.
python -m examples.cross_queue --role consumer --redis-key example:demo

# Terminal 2: Send four tasks. Then exit.
python -m examples.cross_queue --role producer --redis-key example:demo --count 4
```

The flow namespaces are `<redis-key>:producer` and `<redis-key>:consumer`.
`TaskFlow(redis_key=...)` takes a namespace. `Queue(redis_key=...)` takes the full Redis key.
The example uses `default.TAB_JOB.format(...)` to build that key.

`process_name="Consumer"` must match the registered process class name.
`callback="receive"` must name a method on that class.
`--count` controls how many tasks the producer sends. The consumer does not create initial tasks.

## Run each day

```sh
python -m examples.scheduled --at 20:30
```

The flow runs once each day at the given time, using **local system time**.
It waits for the next scheduled time after startup. It does not run immediately to catch up.
`--at` accepts `HH:MM` and checks the value before connecting to Redis.

To run the same flow immediately:

```sh
python -m examples.scheduled --once
```

`--once` runs the flow now and then exits. It does not test the daily schedule trigger.
Do not use `--at` and `--once` together.

## Repeat or stop a run

All examples support `--help` and `--redis-key`.
Use a separate namespace for independent runs, for example:

```sh
python -m examples.branching --redis-key example:inspection:run2
```

Examples keep unfinished tasks by default. They do not clear Redis.
If you reuse a namespace, the flow can continue tasks from a previous run.
New `--count` or `--name` values do not change tasks that are already queued.
The submission example explicitly submits tasks on each run, even when the queue has unfinished work.

Press Ctrl+C to request graceful shutdown.
The program waits for active tasks and buffered writes to finish.
It can continue waiting while Redis is unavailable.

These examples demonstrate the API. They are not production deployment configurations.
See [Reliability and limits](../docs/reliability.markdown) for initial buffer writes and repeated execution.

The old `eg_05.py` example is now split between `cross_queue.py` and `scheduled.py`.
The old `eg_06.py` branches are now in `branching.py`.
`_runner.py` contains the shared console shutdown code.
