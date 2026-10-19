# Reliability and limits

## Memory buffers and Redis

Memory buffers group writes into batches to reduce Redis requests.
Redis keeps claimed tasks until the worker confirms their completion.
The sorted-set score stores the next time that a worker can claim the task.
A heartbeat extends this deadline while the task is active. This time limit is the task lease.
After a process stops or crashes, another worker can claim unconfirmed tasks when their leases expire.

A successful `put_job()` call means that the local buffer accepted the task.
**It does not mean that Redis received the task.**
Initial tasks can be lost if the process crashes before the first successful buffer write.
Call `flush(raise_errors=True)` when you need to check a write, or make the upstream producer responsible for resubmission.

For public batch submission, use `flow.submit(tasks)`. It waits until Redis confirms that batch.
It can write tasks before workers start. It accepts local `Task` objects without routing overrides.
With multiple registered processes, set each task's `process_name`.
The returned `threading.Event` is set after the write is confirmed, not after task execution.
Use `wait=False` only on a running flow. Wait on the returned event to check that specific batch.
An event wait timeout does not cancel the submission.
Stop producers before stopping the flow; concurrent submission during shutdown is not supported.

Serialization or capacity errors reject the whole submission before it enters the buffer.
A Redis error during a synchronous submission raises and retains the accepted batch for retry.
The server may have applied a write whose response was lost. An exception does not prove that nothing was written.
Running buffer threads retry retained batches. Call `flow.flush()` to retry all retained writes without starting workers.
`flow.flush()` raises on a Redis error and keeps failed writes for another attempt.
It drains destination writes before source acknowledgements. It does not wait for callbacks to finish.
Stop producers before calling it if you need the call to finish with empty buffers.
Resubmitting after an uncertain result can repeat execution. Preserve task identity and keep application actions idempotent.

Each `flush()` call processes one fixed snapshot and returns a success flag.
A failed snapshot is retried before new data.
If another thread adds work during a flush, success does not confirm those later additions.

A buffer keeps its current batch after a failure. It resets its busy flag after an exception.
Pending-work counts include failed batches, error records, and completion records that have not reached Redis.
Each buffer accepts at most `BUFFER_MAX_RECORDS` records, including its failed snapshot.
The default is 1,000 records. Outputs, errors, and source acknowledgements use this capacity.
An error and its attached source acknowledgement count as one record and are committed together.
When capacity is unavailable, `BufferFullError` rejects new records immediately.
Existing records remain available for retry. A caller can retry a rejected submission after pending writes complete.
The collector pauses new claims while either buffer is full. Already prefetched tasks may still run.
If a callback cannot buffer its output or completion, its source stays in Redis for retry after lease expiry.
Outputs accepted earlier in that callback may already be in Redis and can run more than once.
Flushes do not need free capacity, so full buffers can drain after Redis recovers.
The limit counts records, not bytes. Keep payloads small; there is no global byte limit across flows or callers.

## Commit tasks in one queue

`TaskBuffer` captures outputs, completion records, and error records under one lock.
It releases the state lock during the Redis write, so producers can add more work.
Background flushes and explicit flushes run one at a time.
Later completion records cannot pass a failed batch.

On one Redis instance, a Lua script writes outputs and error records before it removes completed source tasks.
Other clients cannot observe an intermediate state.
The script checks key types before it writes data.
This prevents known `WRONGTYPE` errors from causing partial writes.
Redis does not roll back earlier script writes after every possible runtime error.

Inserts use `ZADD NX`. A retry preserves the lease score of an existing task.
Submitting the same message again also preserves its score.
The sorted set identifies duplicates by the complete serialized message, rather than by `taskid` alone.
Give separate executions of the same business input different task IDs.

A generator can yield outputs that are written in separate batches.
The full generator execution is not one transaction.
After a crash, the source task can run again and produce outputs that were already written.

## Transfer tasks between queues

`QueueBuffer` groups outputs by destination connection and queue.
It marks each write as confirmed after the destination accepts the batch.
The source `TaskBuffer` removes the source task only after all destination writes are confirmed.
If a later destination fails, a retry skips groups whose writes are already confirmed.

There is no distributed transaction across separate Redis instances.
A crash after the destination write but before source removal can cause another delivery.
If the destination is unavailable, the source task remains in Redis.
Its worker continues lease renewal while the local process runs normally.

## Repeated execution and persistence

This version does not guarantee exactly-once execution.
A task can run again after its business action succeeds but before Redis confirms completion.
A lost response after a successful commit can also cause a retry.
Make database writes and external API calls idempotent: repeating the action must not cause incorrect results.

Task leases do not yet have ownership tokens or fencing checks.
If a worker loses its connection for longer than the lease, another worker can claim the task.
The old worker can later resume while the new worker is active.
An old completion record can then affect the new execution.
Lock ownership tokens protect locks; they do not provide task ownership checks.

Recovery after a Redis restart depends on its AOF/RDB, replication, and failover settings.
The development and test containers have no persistence setup for production use.

## Leases and heartbeats

Configure these values in the application's `settings.py` before creating flows:

| Setting | Default | Meaning |
| --- | --- | --- |
| `JOB_LOST_TIMEOUT` | `30` | Task lease duration in seconds |
| `HEARTBEAT_INTERVAL` | `5` | Wait between heartbeat rounds, in seconds |
| `HEARTBEAT_BATCH_SIZE` | `1000` | Maximum tasks per renewal command |
| `REDIS_SOCKET_TIMEOUT` | `5` | Default Redis command timeout in seconds |
| `REDIS_CONNECT_TIMEOUT` | `5` | Default Redis connection timeout in seconds |
| `BUFFER_MAX_RECORDS` | `1000` | Maximum retained records per buffer |

Startup rejects nonpositive or nonfinite timing values and invalid integer limits.
The lease must be at least twice the sum of the heartbeat interval, socket timeout, and connection timeout.
This gives a baseline margin. It does not bound Redis client retries, thread stalls, or all commands in a large heartbeat round.
Increase the lease for measured network delays and large workloads. Longer leases also delay recovery after a worker exits.
Heartbeats use bounded `ZADD XX` batches. They renew existing tasks without recreating deleted members.
After a failed round, the next round retries the tasks still tracked by the collector.
Keep overrides on custom Redis clients consistent with the lease settings.

## Callback errors

A callback must return an iterable of `Task` or `Queue` objects, or `None`.
Invalid outputs raise a type error and use the normal error handler. They are not treated as successful completion.
The default handler stores the error and confirms the source only after required destination writes succeed.
If the error write fails or buffer capacity is unavailable, the source remains in Redis.
Previously yielded outputs are not rolled back when a later output is invalid.

## Stop a flow

```python
flow.stop()       # Stop fetching new tasks.
flow.join(30)     # Wait for at most 30 seconds.
if flow.is_alive():
    print("Tasks or pending writes remain.")
```

Graceful shutdown waits for active callbacks, stops buffer threads, and writes remaining data.
Heartbeats stop after these writes finish.
Writes continue to retry while Redis is unavailable.
A callback that never returns can also prevent shutdown; Python cannot safely force that thread to stop.

`join(timeout)` limits only how long the caller waits. It does not stop a running thread.
If the process is killed, unconfirmed tasks depend on lease expiry for recovery.

## Supported environments

- Python 3.10 or later. GitHub CI is configured for Python 3.10 through 3.14.
- Standalone Redis. GitHub CI is configured for Redis 7 and 8.
- Redis Cluster is not supported. Multi-key Lua scripts require keys in the same slot; the default names do not ensure this.
- Workers use threads. Application code and shared process instances must be safe for concurrent use.
