# Development and quality gates

## Pure-Python loop

Most business logic and helper tests run without an Airflow service. On native Windows, this is the supported development loop; full Airflow validation belongs in WSL2/Linux or CI. Git Bash remains a Windows compatibility shell and is not a substitute for WSL2/Linux Airflow validation.

PowerShell setup:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements/dev.txt
python -m pip install --no-deps -e .
```

Quality loop on any development environment:

```bash
pytest
ruff check .
ruff format --check .
python scripts/check_secrets.py
```

On native Windows, the Airflow DAG-integrity/runtime test modules are intentionally skipped before importing Airflow. Those skips are expected and must be covered by the Linux CI gate before release.

The `job_context` Pytest fixture is a normal `JobRunContext`, so job tests do not need scheduler/API server startup. External integrations should be injected/faked at their boundary.

## Full Airflow environment (Linux/macOS/WSL2)

Use Python 3.12 and Airflow 3.3.1 with the matching official constraints. Install Airflow and the Standard provider in one resolver transaction, then install the development tooling/project and verify both dependency metadata and the public SDK surface:

```bash
AIRFLOW_VERSION=3.3.1
PYTHON_VERSION=3.12
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
python -m pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  apache-airflow-providers-standard \
  --constraint "${CONSTRAINT_URL}"
python -m pip install -r requirements/dev.txt
python -m pip install --no-deps -e .
python -m pip check
python -c "from airflow.sdk import DAG, ObjectStoragePath, task; print('airflow.sdk import smoke: OK')"
```

Do not treat an `airflow.sdk` import failure as a template test failure until `pip check` and this SDK smoke pass. A missing public symbol such as `ObjectStoragePath` indicates an inconsistent Airflow Core/Task SDK installation and should be repaired at the environment layer.

Then set a local Airflow home and DAG folder:

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export PYTHONPATH="$PWD/src"
```

Initialize local metadata only when Airflow-level testing is needed:

```bash
airflow db migrate
airflow dags list --local
airflow dags list-import-errors --local
airflow dags reserialize
airflow dags list
airflow dags list-import-errors
```

A healthy project has no DAG import errors in either the direct filesystem parse or the serialized metadata view. The Pytest DAG integrity suite also verifies unique DAG/task IDs, catchup, timezone-aware start dates, tags, and task execution timeouts when Airflow is installed.

Targeted execution:

```bash
airflow dags test example_simple_job 2026-08-21 \
  --dagfile-path "$PWD/dags/example_simple_job.py"
```

The built-in `example_simple_job` is the preferred smoke target because it is deterministic and has no external integration. Do not run a DAG smoke test against real mutable integrations unless the test environment and inputs are explicitly safe. Use `dry_run`/mock Connections when the job supports them.

## Integration tests

Tests that require a real DB/API/browser/SAP system must use the `integration` marker:

```python
import pytest


@pytest.mark.integration
def test_real_dependency(): ...
```

Run them intentionally:

```bash
pytest -m integration
```

They should not be mixed into the default unit-test gate.

## Adding a provider

1. Confirm a community/official provider exists before writing a Hook/operator.
2. Add it under `requirements/optional/` unless every job genuinely needs it.
3. Install it using the same Airflow constraints URL and include the exact `apache-airflow` version in the same pip command so a provider install cannot silently move the core version.
4. Keep provider-specific imports out of DAG top-level code when they are heavy/optional.
5. Add unit tests with fakes plus marked integration tests where a real system is available.

Do not add browser engines, pandas, SAP SDKs, Docker/Kubernetes providers, or database drivers to the core just because a future job may need them.

## Explicit workflow task boundary

Plain Python business functions are not Airflow tasks by themselves. In an explicit workflow, call them from inside `@task` functions. If the function uses the template error hierarchy, apply the shared error adapter at that task boundary:

```python
from airflow.sdk import dag, task

from my_project.jobs.customer_sync import extract
from my_project.runtime import run_with_airflow_error_policy


@dag(...)
def workflow():
    @task(...)
    def extract_task():
        return run_with_airflow_error_policy(extract)

    extract_task()
```

Do not call `extract()` directly in the `@dag` body. That body runs while Airflow constructs the graph, not when a worker executes the task.

## DAG review checklist

Before delivery, verify:

- DAG file is mostly declarations/tasks/dependencies;
- no DB/HTTP/SAP request or expensive discovery runs during import or DAG graph construction;
- plain business functions are called from task/operator execution boundaries, not directly from an `@dag` body;
- `start_date` is deterministic and timezone-aware;
- schedule/catchup are intentional;
- each external call has a client timeout and the task has an execution timeout;
- retry is safe for the job's side effects;
- permanent `NonRetryableJobError` failures are adapted at explicit Python task boundaries;
- overlap/concurrency is intentional;
- Params validate manual inputs and contain no secrets;
- Connections contain credentials/endpoints rather than source code;
- XCom contains only small metadata/identifiers and no credential-bearing artifact URI;
- cross-task files live in shared/external storage;
- logs do not contain credentials, credential-bearing URIs, or sensitive payloads;
- optional dependencies are isolated;
- business logic has unit tests independent of Airflow services.

## Scaffold validation

`bootstrap_project.py` and `new_job.py` use only the standard library. They validate names, avoid shell execution, write files atomically, reject unsafe symlink/control paths, preserve rewritten file permissions during bootstrap, and refuse unsafe overwrites/repeated bootstrap.

A useful end-to-end generator check is:

```bash
cp -R . /tmp/airflow-template-check
cd /tmp/airflow-template-check
python scripts/bootstrap_project.py demo_project
python scripts/new_job.py customer_sync --type simple
python -m compileall -q src dags scripts tests
pytest
```

Use a disposable copy. Do not bootstrap the template repository merely to test the bootstrap.

## CI

`.github/workflows/ci.yml` is prepared as a read-only quality pipeline:

1. Python 3.12;
2. Airflow 3.3.1 installed with official constraints;
3. provider Standard + development tooling;
4. `pip check` + public `airflow.sdk` import smoke;
5. Ruff lint/format;
6. Pytest + coverage/DAG integrity;
7. secret scan, including `.env*` variants and credential-bearing URIs;
8. end-to-end bootstrap + simple/workflow/isolated scaffold generation, compile and Ruff smoke;
9. wheel/package build check;
10. local Airflow metadata migration + DAG import/serialization smoke;
11. end-to-end execution of the deterministic `example_simple_job` with `airflow dags test`.

No credential is hardcoded. Real integration jobs belong in separate environment-specific CI jobs when credentials/test systems exist. For contributors on Windows, this Linux workflow is the release-level Airflow acceptance gate; a local PowerShell run alone is not sufficient for promotion.

## Packaging/delivery hygiene

Before producing a change ZIP or release artifact:

```bash
python scripts/check_secrets.py
python -m compileall -q src dags scripts tests examples
python -m pip wheel --no-deps . --wheel-dir dist
```

Inspect the archive and reject `.git`, `.venv`, caches, `.env*` files other than `.env.example`, logs, local Airflow metadata, or other temporary artifacts. A differential delivery should contain only files actually added or changed relative to the target branch.
