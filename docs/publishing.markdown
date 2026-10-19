# Publishing to PyPI

The distribution is named `redis-flow`. The Python import package remains `rf`.
Do not install the unrelated PyPI distribution `rf` in the same environment.
The current version is `0.3.6`. Release files cannot be replaced after publication.

## Set up the PyPI account

1. [Register a PyPI account](https://pypi.org/account/register/).
2. Verify its email address.
3. Enable two-factor authentication in the account settings. Save recovery codes privately.
4. Open [Publishing](https://pypi.org/manage/account/publishing/).
5. Add a pending GitHub publisher with these exact values:

| Field | Value |
| --- | --- |
| PyPI project name | `redis-flow` |
| Owner | `forgeries` |
| Repository name | `redis-flow` |
| Workflow name | `publish.yml` |
| Environment name | `pypi` |

The workflow name is its filename, not the displayed workflow title.
A pending publisher creates the project on its first successful upload. It does not reserve the name.
After the project exists, manage its publisher under the project's Publishing settings.
No PyPI password or API token is stored in the repository or GitHub secrets.
See the [PyPI setup guide](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/).

## Set up GitHub

Create an environment named `pypi` in the repository's Settings > Environments.
Limit its deployment branches to `main`.
The upload job requests an OIDC token only after checks and builds finish.
The publisher identity must match the PyPI form above.

## Check a release

1. Update the version in `setup.py` and record changes in the changelog.
2. Push the release changes to `main`.
3. Open Actions > Publish to PyPI > Run workflow.
4. Select `main`. Leave the upload option unchecked for a rehearsal.
5. Confirm that all checks and the build pass. Download the `pypi-distributions` artifact if needed.

The workflow runs the Python 3.10–3.14 and Redis 7/8 matrix.
It builds a source distribution and wheel, then checks them with Twine.
Publishing runs in a separate job. Push and pull request events never upload distributions.

## Publish a release

1. Confirm that the PyPI publisher and GitHub environment are configured.
2. Start the workflow on `main` with the upload option checked.
3. Wait for the upload job to succeed.
4. Open the [PyPI project](https://pypi.org/project/redis-flow/) and verify its version and files.
5. Install the release in a clean environment:

```sh
python -m pip install redis-flow==0.3.6
python -c "from rf import Task, TaskFlow; from importlib.metadata import version; print(version('redis-flow'))"
```

Use a new version for later changes. Do not attempt to replace an existing release file.
If an upload fails, inspect PyPI before retrying; some files may already have been uploaded.
See [Trusted Publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/) for the authentication flow.
