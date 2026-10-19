# Contributing

## Set up the environment

Use Python 3.10 or later. Run these commands from the repository root:

```sh
python -m venv .venv
# Linux or macOS: source .venv/bin/activate
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m pytest tests/unit -q
```

Unit tests must not connect to Redis. The test fixtures reject these connections.

## Run Redis integration tests

Use a separate Redis instance for tests. Do not use a production connection.

```sh
docker run --rm -d --name rf-test-redis -p 127.0.0.1:16379:6379 redis:7-alpine
```

On Linux or macOS:

```sh
RF_TEST_REDIS_URL=redis://127.0.0.1:16379/0 python -m pytest -q
```

On Windows PowerShell:

```powershell
$env:RF_TEST_REDIS_URL = 'redis://127.0.0.1:16379/0'
python -m pytest -q
```

Run `docker stop rf-test-redis` when you finish.
Integration tests are skipped when `RF_TEST_REDIS_URL` is not set.
If the URL is set but the connection fails, the tests fail.
Tests delete only keys in their own UUID namespace. They do not use `FLUSHDB`.

The tests cover failed buffer writes, concurrent flushes, lost commit responses, and transfers between queues.
They also cover heartbeats, lock ownership, shutdown, and task recovery after a worker process is killed.
Use `Event` objects to coordinate concurrent tests where possible.
When a test must wait for a Redis lease, poll with a fixed deadline.

## Code style

Use Ruff to format code and sort imports.
Comments should explain constraints, limits, and reasons for a decision.
Remove stale descriptions and code that has been commented out.
Preserve public APIs unless a change is intentional and documented.
Add a regression test when you change behavior.

Format local changes with:

```sh
python -m ruff check --fix .
python -m ruff format .
```

## English writing style

Use English for primary documentation, comments, docstrings, logs, error messages, and command-line help.
Translations such as `README.zh.markdown` must stay consistent with the English source.
Use the `.markdown` extension for project documents.
Use a practical subset of [ASD-STE100](https://www.asd-ste100.org/STE_faq.html) for the project's 80% STE target.

- Write short sentences. Use no more than 20 words per instruction.
- Give one instruction per sentence. Use active voice where possible.
- State the problem first. Then give the action that the user can take.
- Use one term for one meaning. Use common words.
- Keep technical terms and API names exact. Examples include Redis, task, queue, lease, buffer, and callback.
- Keep user data and external exception details unchanged.

These rules guide code review. They do not provide a certified compliance score.

## Check a change

Keep each change focused on one problem.
Describe the trigger, the resulting behavior, and the checks you ran.
For a defect, add a test that reproduces the problem before you apply the fix.
Test observable behavior instead of copying implementation details into assertions.

Set `RF_TEST_REDIS_URL` as shown above. Then run:

```sh
python -m ruff check .
python -m ruff format --check .
python -m pytest -q
python -m build
python -m twine check --strict dist/*
```

GitHub CI includes Redis integration tests and wheel installation in a clean environment.
The CNB main-branch pipeline runs style checks, unit tests, and package checks.
After you push a CI change, check the result of the remote run.

## Prepare a release

Pushes and pull requests do not upload packages.
The manual [publishing workflow](.github/workflows/publish.yml) uses PyPI Trusted Publishing.
Follow [Publishing](docs/publishing.markdown) to configure and run it.
Before a release:

- Review third-party attribution and licenses. Preserve required notices.
- Replace any example credentials that were used with real services. Check Git history for exposed credentials.
- Update the version and changelog. The publishing workflow runs the full CI suite on its selected commit.
- Check the PyPI Trusted Publisher and the GitHub `pypi` environment.

The project is at version 0.x. Record public API changes in the changelog.
Do not commit real service credentials. Editing a file does not remove its earlier contents from Git history.
