# Changelog

## Unreleased

### Task reliability

- Reject invalid callback outputs through the error handler instead of silently confirming success.
- Add `flow.submit(tasks, wait=True)` with a confirmation event for each local batch.
- Add `flow.flush()` to retry buffered writes even before workers start.
- Bound buffers with `BUFFER_MAX_RECORDS`. Raise `BufferFullError` on capacity exhaustion and pause new claims while buffers are full.
- Keep sources available for retry when callback outputs or completion records cannot fit in a buffer.
- Commit an error and its source acknowledgement as one buffered record.
- Set the default task lease to 30 seconds and heartbeat interval to 5 seconds. Validate timing and capacity settings before startup.
- Renew leases in bounded batches with `ZADD XX`. Replace the internal `zscore_set` helper with `renew_tasks`.
- Keep failed buffer batches for retry. Serialize flushes so that later completion records cannot pass a failed write.
- Use Lua to commit local outputs, error records, and task completion on one Redis instance.
- Preserve existing task scores when an insert is retried.
- Wait for all destination writes before removing a source task that produces outputs for another queue.
- Renew leases for tasks that are prefetched, executing, or waiting for write confirmation.
- Stop tracking a task after Redis confirms its removal.
- Use a new ownership token for each lock acquisition. Check ownership atomically during release and renewal.
- Add graceful shutdown with `stop()`. Fix `join(timeout)` and use a separate scheduler for each flow.
- Keep failed batches, completion records, and error records in pending-work counts.
- Return a success flag from `flush()`. Use `raise_errors=True` to propagate a write error to the caller.

### Configuration and runtime fixes

- Initialize the logger on first use. Hide credentials in Redis connection representations.
- Use environment variables for example connection settings.
- Propagate Redis command failures to buffer callers. Remove per-command PING calls and the unbounded reconnect loop.
- Set default Redis connection and command timeouts to five seconds. Allow callers to override them through `RedisDB`.
- Apply the connection-pool limit to Redis URL connections.
- Preserve the source callback during task serialization. Preserve the input mapping during deserialization.
- Ignore only an absent `settings` module. Report errors inside an existing settings module.
- Fix log rotation for paths that contain multiple dots.
- Support log filenames without a directory and explicit zero values for rotation limits.

### RedisDB API changes

- Remove unused set, list, hash, bitmap, string, and sorted-set wrappers.
- Remove `current_status`, `from_url`, and implicit command forwarding.
- Remove the `prettytable` and `tqdm` dependencies.

Use `RedisDB(url=...)` to connect with a URL.
Use `get_redis_obj()` to obtain the native redis-py client for general Redis commands.
Follow redis-py argument formats. For example, use `client.zadd(key, {member: score})`.

### Development and documentation

- Use the PyPI distribution name `redis-flow`. Keep the `rf` import package for compatibility.
- Add manual PyPI Trusted Publishing after the full CI matrix and package checks.
- Resolve README links to GitHub when building the PyPI description.
- Require Python 3.10 or later. Add automated tests, package checks, and CI workflows.
- Use Ruff for formatting and import order. Run these checks in CI.
- Remove old experiment scripts. Disable package uploads on main-branch pushes.
- Provide examples for a single task, conditional branches, transfers between queues, and daily schedules.
- Use English for documentation, examples, comments, logs, error messages, and CI step names.
- Use `.markdown` files for project documentation. Update document links and package inputs.
- Use short sentences and consistent technical terms for messages and documentation.

- Adopt the MIT License. Include the license text in source and wheel distributions.

The first release under the new process remains pending.
