# Development and quality gates

## Pure-Python loop

Most business logic and helper tests run without an Airflow service:

```bash
pytest
ruff check .
ruff format --check .
python scripts/check_secrets.py
```

The `job_context` Pytest fixture is a normal `JobRunContext`, so job tests do not need scheduler/API server startup. External integrations should be injected/faked at their boundary.

## Airflow environment

Use Python 3.12 and Airflow 3.3.1 with the matching official constraints. Then set a local Airflow home and DAG folder:

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export PYTHONPATH="$PWD/src"
```

Initialize local metadata only when Airflow-level testing is needed:

```bash
airflow db migrate
airflow dags list
airflow dags list-import-errors
```

A healthy project has no DAG import errors. The Pytest DAG integrity suite also verifies unique DAG/task IDs, catchup, timezone-aware start dates, tags, and task execution timeouts when Airflow is installed.

Targeted execution:

```bash
airflow dags test <dag_id> <logical-date>
```

Do not run a DAG smoke test against real mutable integrations unless the test environment and inputs are explicitly safe. Use `dry_run`/mock Connections when the job supports them.

## Integration tests

Tests that require a real DB/API/browser/SAP system must use the `integration` marker:

```python
import pytest


@pytest.mark.integration
def test_real_dependency():
    ...
```

Run them intentionally:

```bash
pytest -m integration
```

They should not be mixed into the default unit-test gate.

## Adding a provider

1. Confirm a community/official provider exists before writing a Hook/operator.
2. Add it under `requirements/optional/` unless every job genuinely needs it.
3. Install it using the same Airflow constraints URL as the core environment.
4. Keep provider-specific imports out of DAG top-level code when they are heavy/optional.
5. Add unit tests with fakes plus marked integration tests where a real system is available.

Do not add browser engines, pandas, SAP SDKs, Docker/Kubernetes providers, or database drivers to the core just because a future job may need them.

## DAG review checklist

Before delivery, verify:

- DAG file is mostly declarations/tasks/dependencies;
- no DB/HTTP/SAP request or expensive discovery runs during import;
- `start_date` is deterministic and timezone-aware;
- schedule/catchup are intentional;
- each external call has a client timeout and the task has an execution timeout;
- retry is safe for the job's side effects;
- overlap/concurrency is intentional;
- Params validate manual inputs and contain no secrets;
- Connections contain credentials/endpoints rather than source code;
- XCom contains only small metadata/identifiers;
- cross-task files live in shared/external storage;
- logs do not contain credentials or sensitive payloads;
- optional dependencies are isolated;
- business logic has unit tests independent of Airflow services.

## Scaffold validation

`bootstrap_project.py` and `new_job.py` use only the standard library. They validate names, avoid shell execution, write files atomically, and refuse unsafe overwrites/repeated bootstrap.

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
4. Ruff lint/format;
5. Pytest + coverage/DAG integrity;
6. secret scan;
7. local Airflow metadata migration and DAG import smoke.

No credential is hardcoded. Real integration jobs belong in separate environment-specific CI jobs when credentials/test systems exist.

## Packaging/delivery hygiene

Before producing a change ZIP or release artifact:

```bash
python scripts/check_secrets.py
python -m compileall -q src dags scripts tests examples
```

Inspect the archive and reject `.git`, `.venv`, caches, `.env`, logs, local Airflow metadata, or other temporary artifacts. A differential delivery should contain only files actually added or changed relative to the target branch.
