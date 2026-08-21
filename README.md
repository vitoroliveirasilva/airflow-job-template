# airflow-job-template

A small, production-oriented starter for Python jobs, automations, and data workflows orchestrated by Apache Airflow 3.3.x. It keeps Airflow visible instead of replacing it with another framework.

The template has three paths:

- **Simple Job**: one Python operation where retrying the whole operation is the correct unit.
- **Explicit Workflow**: multiple independent tasks with separate retry/timeout/observability.
- **Isolated Job**: browser, Java, proprietary SDK, conflicting libraries, or another specialized runtime, using Airflow's native isolation mechanisms.

Business logic lives under `src/`; DAG files stay thin; credentials live in Airflow Connections or a Secrets Backend; large payloads live outside XCom.

## Baseline

- Apache Airflow `3.3.x` (`3.3.1` is the development reference)
- Python `3.12` as the development/CI baseline; Airflow 3.3.1 also supports Python 3.13 and 3.14
- Public DAG-authoring/runtime interfaces from `airflow.sdk` where available
- Ruff + Pytest
- Official Airflow constraints for installation

## 1. Bootstrap a concrete project

Run this once before creating jobs. It replaces the generic package name with a project-specific package so multiple DAG bundles do not collide in a shared Airflow environment.

```bash
python scripts/bootstrap_project.py customer_sync
```

For `customer_sync`, the Python package becomes `customer_sync_airflow`. The script updates imports and project metadata, renames the package atomically, never reads/writes `.env*`, and refuses a second bootstrap.

Use lowercase slugs with letters, numbers, `_` or `-`.

## 2. Choose the development mode

Airflow 3.3.1 runs on POSIX-compliant systems. On Windows, use WSL2 or a Linux container for the full Airflow runtime. Native PowerShell is still useful for the fast pure-Python development loop; Git Bash does not turn native Windows into a supported Airflow runtime.

### Windows PowerShell: pure-Python loop

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements/dev.txt
python -m pip install --no-deps -e .
ruff format --check .
ruff check .
pytest
```

The Airflow runtime/DAG-integrity modules are intentionally skipped on native Windows. A green PowerShell run validates the Airflow-independent core, scaffolding, integrations, security helpers, and job logic; it does not replace the Linux/WSL2/CI Airflow acceptance gate.

### Linux/macOS/WSL2: full Airflow environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

## 3. Install Airflow reproducibly

Always install Airflow using the constraints file matching both Airflow and Python. Run this section on Linux, macOS, or WSL2 rather than native Windows.

Linux/macOS/WSL2:

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

Install providers only when a job needs them. For the API -> PostgreSQL example:

```bash
python -m pip install "apache-airflow==$AIRFLOW_VERSION" -r requirements/optional/http.txt --constraint "$CONSTRAINT_URL"
python -m pip install "apache-airflow==$AIRFLOW_VERSION" -r requirements/optional/postgres.txt --constraint "$CONSTRAINT_URL"
python -m pip check
```

`mssql.txt` is available for Microsoft SQL Server. RPA/SAP dependencies are intentionally not invented: install the concrete browser/SDK/provider required by the real runtime.

Do not install `requirements/base.txt` without the matching Airflow constraints. Airflow is an application with a coordinated dependency set; an unconstrained or partially upgraded environment can leave `apache-airflow-core` and `apache-airflow-task-sdk` incompatible. If the SDK smoke above fails, rebuild the virtual environment or reinstall Airflow + the Standard provider together using the single constrained command above before debugging template code.

## 4. Create a job

After bootstrap:

```bash
python scripts/new_job.py customer_sync
python scripts/new_job.py billing_pipeline --type workflow
python scripts/new_job.py portal_update --type isolated
```

Job names must be 2-100 character `snake_case` Python module names and cannot be reserved Python keywords such as `class`, `import`, or `async`.

The default `simple` scaffold creates:

```text
dags/customer_sync.py
src/<project_package>/jobs/customer_sync/__init__.py
src/<project_package>/jobs/customer_sync/job.py
tests/unit/jobs/test_customer_sync.py
```

New jobs start paused from a scheduling perspective (`schedule=None`), with `catchup=False`, `max_active_runs=1`, an explicit execution timeout, no credentials/endpoints, and a placeholder that fails clearly if somebody triggers it before implementation.

For the common case, the workflow is deliberately short:

```text
1. run scaffold
2. implement run()
3. declare the Connection IDs the job needs
4. set the schedule
5. add validated Params if a manual run needs inputs
6. write/adjust tests
7. validate the DAG
8. deliver
```

## Simple Job

A Simple Job is normal Python plus a tiny DAG wrapper:

```python
# src/<package>/jobs/customer_sync/job.py
from <package>.runtime import JobResult, JobRunContext


def run(context: JobRunContext) -> JobResult:
    # Business logic and runtime integrations here
    return JobResult(processed=10)
```

```python
# dags/customer_sync.py
from <package>.jobs.customer_sync import run
from <package>.runtime import JobSpec, build_single_task_dag

SPEC = JobSpec(
    dag_id="customer_sync",
    description="Synchronize customers",
    schedule="0 6 * * *",
    tags=("crm",),
)

dag = build_single_task_dag(spec=SPEC, job_callable=run)
```

The factory only creates one TaskFlow task, applies `JobSpec`/`TaskPolicy`, adapts runtime context and non-retryable failures, and returns small result metadata. It does not know about HTTP, databases, SAP, RPA, or workflow topology.

## Explicit Workflow

Use explicit TaskFlow/operators when steps need independent retries or visibility.
See `dags/example_workflow.py`.

```python
dag_kwargs = SPEC.as_dag_kwargs()
schedule = dag_kwargs.pop("schedule")

@dag(schedule=schedule, **dag_kwargs)
def workflow():
    extracted = extract()
    validated = validate(extracted)
    load(validated)
```

Do not force multi-task workflows through the Simple Job factory. Native operators, sensors, deferrable operators, task groups, and provider operators are first-class options.

When the number of independent units is only known at runtime, use Airflow Dynamic Task Mapping and pass small IDs/references, not whole datasets:

```python
branch_ids = discover_branch_ids()
process_branch.expand(branch_id=branch_ids)
```

## Isolated Jobs

Choose isolation when the task needs browser drivers, Java, proprietary SDKs, conflicting Python libraries, a specialized image, or dedicated resources. Prefer the native mechanism supported by your infrastructure:

1. dedicated queue/pool when workers already exist;
2. `@task.external_python` for a prepared Python environment;
3. `@task.virtualenv` for compatible Python-only dependencies;
4. DockerOperator when Airflow has an approved Docker runtime;
5. KubernetesPodOperator when the deployment already uses Kubernetes;
6. a provider-native operator when it models the integration better.

The generated isolated scaffold intentionally contains a Python-path placeholder rather than pretending that Docker/Kubernetes/queues exist everywhere.

## Configuration: code vs Connection vs Variable vs Param

| Information                 | Place                                | Example                      |
| --------------------------- | ------------------------------------ | ---------------------------- |
| DAG structure               | versioned code                       | schedule, tags, topology     |
| task policy                 | versioned code                       | retries, timeout, pool       |
| credential/endpoint         | Airflow Connection / Secrets Backend | DB password, API host/token  |
| per-run input               | Airflow Param                        | `dry_run`, date range, limit |
| runtime non-secret setting  | Airflow Variable                     | operational batch limit      |
| small task-to-task metadata | XCom                                 | ID, count, artifact URI      |
| large data/artifact         | external/shared storage              | CSV, Parquet, ZIP            |

Keep the same `conn_id` across environments when it represents the same resource role; change the Connection contents, not DAG conditionals.

Use `Param` with JSON Schema validation:

```python
from airflow.sdk import Param

params = {
    "dry_run": Param(False, type="boolean"),
    "limit": Param(1000, type="integer", minimum=1, maximum=10_000),
}
```

Never put a password/token in a Param. Avoid `Variable.get()` at DAG top level; resolve runtime configuration inside a task when needed.

## Integrations

`HttpClient` and `DatabaseClient` are intentionally thin and resolve Hooks/Connections only at task runtime.

HTTP behavior:

- explicit connect/read timeout;
- TLS verification is not disabled;
- no hidden client retry loop, so task-level retries do not multiply unexpectedly;
- `408`, `425`, `429`, and `5xx` are retryable only when the operation is retry-safe;
- GET/HEAD/OPTIONS are retry-safe by default; mutating methods require explicit `retry_safe=True`
  after idempotency has been established;
- functional `4xx` failures are non-retryable;
- pagination helper only models the common page/page-size contract.

Database behavior:

- provider Hook resolved by `conn_id` at runtime;
- explicit transaction boundary with commit/rollback/cleanup;
- parameterized SQL expected;
- batch `executemany` and bounded/streaming reads;
- provider Hook remains accessible for advanced/native behavior.

See `examples/api_to_database/` for an idempotent PostgreSQL UPSERT example. The helper does not try to hide SQL dialect differences.

## Idempotency and retries

A retry repeats the unit of work, so every mutable job needs a strategy.

| Pattern   | Common strategy                                                 |
| --------- | --------------------------------------------------------------- |
| API -> DB | unique key + UPSERT/MERGE, watermark, partition replacement     |
| DB -> API | idempotency key, outbox/status, remote-state check              |
| RPA       | locate by stable ID, check current state, mutate, verify result |
| files     | deterministic name/checksum, processed-file ledger, atomic move |

`RetryableJobError` lets Airflow use normal retries. `NonRetryableJobError` is adapted by the Simple Job factory to an Airflow fail-without-retry exception. Do not blindly retry a mutating RPA/API operation unless repeating it is demonstrably safe.

## Files and XCom

A task may use local temporary files during its own execution. Do not assume `/tmp/a.csv` written by one task exists on the worker that runs the next task. Put cross-task artifacts in storage supported by the deployment and pass only a URI/path/ID through XCom.

`JobResult` intentionally accepts only small scalar execution metadata such as counts, `batch_id`, and `artifact_uri`.

## SAP and RPA

`integrations/sap/` is an extension point, not a fake universal SAP client. Add concrete adapters such as OData/RFC/HANA only when the protocol and runtime are known. SAP GUI belongs to the isolated RPA profile.

For browser automation, keep one browser session inside one task when there is no durable checkpoint between steps. Check state before mutable actions, validate afterward, clean up in `finally`, and store diagnostics outside XCom without exposing credentials/PII. See `examples/rpa/`.

## Logging and security

Use normal Python logging. `log_event()` adds compact execution context, redacts fields whose names indicate credentials/tokens/passwords, redacts credential-bearing URI strings, and avoids serializing arbitrary object representations. `JobResult.artifact_uri` also rejects URIs containing embedded credentials or sensitive token/signature query parameters before they can reach XCom. Do not rely on these safeguards alone: never pass secret payloads to the logger or XCom in the first place.

Security defaults/rules:

- `.env` and local Airflow state are ignored;
- `.env.example` contains no secret values;
- credentials belong in Connections/Secrets Backend;
- SQL values are parameterized;
- TLS verification stays enabled unless a deployment explicitly configures otherwise;
- no `shell=True` helpers or execution of Param text as code;
- scaffold/bootstrap validate names and use filesystem APIs instead of shell interpolation;
- `scripts/check_secrets.py` catches several high-confidence token/private-key patterns before
  packaging/CI.

## Development and validation

Pure job logic should be testable without a scheduler, API server, or metadata database:

```bash
pytest
ruff check .
ruff format --check .
python scripts/check_secrets.py
```

With Airflow installed on Linux/macOS/WSL2:

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export PYTHONPATH="$PWD/src"

airflow db migrate
airflow dags list --local
airflow dags list-import-errors --local
airflow dags reserialize
airflow dags list
airflow dags list-import-errors
```

The local commands prove filesystem parsing; `reserialize` plus the non-local commands prove metadata serialization. For a targeted execution smoke test, use the deterministic built-in DAG:

```bash
airflow dags test example_simple_job 2026-08-21 \
  --dagfile-path "$PWD/dags/example_simple_job.py"
```

Integration tests are separate:

```bash
pytest -m integration
```

The default test run must not call real external systems. See `docs/development.md` for gates and `docs/architecture.md` for the architecture contract.

## Release acceptance

A candidate release is accepted only after the Linux CI pipeline passes the coordinated Airflow installation, `pip check`, public `airflow.sdk` smoke, Ruff, Pytest/coverage and DAG integrity, secret scan, wheel build, local + serialized DAG import checks, and the end-to-end `example_simple_job` execution. Native Windows results are a developer feedback loop, not a substitute for this Airflow runtime gate.

The repository should remain free of `.env`, local Airflow metadata, caches, logs, virtual environments, and generated build artifacts. Keep optional providers opt-in and introduce new abstractions only after repeated real use justifies them.

## Which pattern should I choose?

```text
Can the job run as one Python process and does whole-operation retry make sense?
  yes -> Simple Job
  no
   ↓
Do independent stages need separate retry/timeout/observability?
  yes -> Explicit Workflow
   ↓
Are N independent work items discovered only at runtime?
  yes -> Dynamic Task Mapping
   ↓
Does it need browser/Java/proprietary SDK/conflicting libs?
  yes -> Isolated execution
   ↓
Is it mostly SQL/Bash/provider-native behavior?
  yes -> use the native Airflow operator directly
```

For long waits, prefer sensors and deferrable operators where available. Do not occupy a worker with hours of `sleep()` polling, and do not split a stateful browser session across tasks merely to make a larger graph.

## Repository map

```text
dags/                       thin Airflow definitions
src/<project_package>/
  runtime/                  JobSpec, TaskPolicy, context, errors, Simple Job factory
  integrations/             small runtime integration helpers/extension points
  jobs/                     pure or mostly-pure use-case logic
  observability/            logging helpers
scripts/                    one-time bootstrap, new-job scaffold, secret check
tests/                      unit + DAG integrity + integration marker
examples/                   patterns that should not become core abstractions
requirements/               core/dev plus opt-in providers
docs/                       architecture and development guidance
```

## Non-goals of this first version

No YAML workflow DSL, custom scheduler/executor, universal SAP abstraction, RPA framework, secrets manager, custom metadata database, mandatory Docker/Kubernetes deployment, or internal provider is introduced. Add abstractions later only after real repeated use proves they remove more complexity than they create.

## License

Distributed under the terms of the **[MIT License](LICENSE)**. See the `LICENSE` file for details.
