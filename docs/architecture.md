# Architecture

## Contract

The project has three conceptual layers:

```text
Airflow orchestration
        ↓
Job / use case
        ↓
Integrations / infrastructure
```

`dags/` owns schedule, topology, task boundaries, retry/timeout policy, Params, pools/queues, and Airflow-native behavior. `src/<package>/jobs/` owns business/use-case logic. `integrations/` owns small repeated pieces of runtime I/O. A job may bypass a helper and use a native Hook/operator when that is clearer.

## Parse-time boundary

DAG imports must not perform HTTP/DB/SAP access, heavy filesystem work, `Variable.get()` lookups, or large dynamic discovery. The current integration helpers import/resolve provider Hooks inside runtime calls. Heavy optional SDKs belong inside task code or an isolated environment.

An `@dag` function is Python used to construct the task graph. Its body runs when the DAG factory is called, normally during module import. Plain business functions must therefore be invoked from an `@task` body (or another operator execution boundary), not directly from the `@dag` body.

## Runtime surface

### `JobSpec`

`JobSpec` contains only repeated DAG configuration with useful project defaults:

- deterministic timezone-aware `start_date`;
- `schedule=None` for new/manual-only jobs;
- `catchup=False`;
- `max_active_runs=1`;
- owner, tags, Params and task policy;
- validation for predictable IDs/tags.

It translates directly to DAG kwargs. Exceptional Airflow kwargs can be passed explicitly or a DAG can be authored without the Simple Job factory.

### `TaskPolicy`

`TaskPolicy` centralizes operational defaults (`retries`, retry delay/backoff, execution timeout, optional pool/queue, priority). It deliberately does not mirror every BaseOperator option.

### `JobRunContext` and `JobResult`

`JobRunContext` adapts only the runtime fields normal Python logic needs, making jobs testable with a plain dataclass fixture. It does not reproduce the full Airflow `Context`. Runtime timestamps are validated as timezone-aware and data intervals must be ordered.

`JobResult` carries small scalar metadata only. Large objects belong in external/shared storage. String metadata is bounded to keep accidental oversized XCom values out of the normal path.

`artifact_uri` is metadata rather than a credential carrier: URIs with embedded user/password data or sensitive token/signature query parameters are rejected before XCom serialization.

### Airflow error adapter

`run_with_airflow_error_policy()` is the small Airflow-specific boundary shared by the Simple Job factory and explicit Python task wrappers. It maps `NonRetryableJobError` (including `JobConfigurationError`) to `AirflowFailException`, so permanent failures fail without consuming configured retries. `RetryableJobError` and unexpected exceptions pass through to normal Airflow retry behavior.

The adapter is intentionally not a task factory and does not hide TaskFlow/operator topology.

### Simple Job factory

`build_single_task_dag()` is the only deliberate convenience abstraction over Airflow. It:

1. applies `JobSpec`;
2. creates one TaskFlow task;
3. applies `TaskPolicy`;
4. adapts the current context;
5. invokes the job callable through the shared Airflow error policy;
6. logs start, classified failure, and completion events;
7. returns small result metadata.

It contains no integration detection, no topology engine, and no secret/config loader.

## Explicit workflows

A multi-step DAG remains workflows-as-code. TaskFlow dependencies should be visible in the DAG module and Airflow operators/providers can be used directly. Small Python functions can stay in the job package so they are unit-testable without Airflow.

When a plain Python step uses the template error hierarchy, invoke it from the `@task` body through `run_with_airflow_error_policy()`. This keeps permanent configuration/domain failures from wasting retries without introducing another orchestration abstraction.

Dynamic Task Mapping is preferred when independent work items are discovered at runtime. Discovery runs in a task and returns small identifiers, never a huge dataset.

## Integration boundaries

### HTTP

The helper constructs `HttpHook` only at runtime using a Connection ID, supplies connect/read timeouts, disables the Hook's automatic status exception only so statuses can be classified into project retry intent, and leaves client retries at zero. It does not invent a generic pagination protocol beyond one opt-in page/page-size helper.

HTTP endpoints remain relative to the host stored in the Connection. Request headers and pagination-owned parameters are validated before I/O. Known permanent transport/configuration failures remain non-retryable even when provider code wraps the underlying exception.

### Database

The DB helper uses the Hook selected by an Airflow Connection. It provides explicit DB-API transaction/cleanup, batch writes, bounded fetch, streaming chunks, and exposes the Hook for cases where provider-native features are better. SQL dialect/idempotency strategy remains job-specific.

Calls with no SQL parameters use the DB-API one-argument `execute(sql)` form for driver compatibility. Batch iterables are validated before opening a connection, and bounded collection explicitly closes its streaming iterator when the bound is reached.

### Files

Only same-task local utilities are provided: portable basename validation, checksum, and atomic writes. The project intentionally has no helper that pretends local paths are shared between workers.

### SAP

No universal adapter exists. `integrations/sap/` documents where a concrete OData/RFC/HANA adapter can be added after its real contract is known. SAP GUI is an RPA/isolation concern.

### RPA / specialized dependencies

The template provides patterns, not a browser framework. State check, mutation, verification, diagnostic capture, and cleanup should remain in one task when browser/session continuity matters. The Airflow-native executor/operator appropriate to the deployment owns isolation.

The RPA example mutates only from a known expected state and re-checks state on every attempt. A cleanup failure after a confirmed remote mutation is logged instead of converting that successful side effect into a retry.

## Operational error semantics

- `JobConfigurationError`: permanent configuration/input problem.
- `NonRetryableJobError`: known permanent failure; Airflow task boundaries using the shared adapter stop retries.
- `RetryableJobError`: transient failure; normal Airflow retry policy applies.

The hierarchy is intentionally tiny. It is not a substitute for domain exceptions inside jobs.

## Concurrency and idempotency

`max_active_runs=1` is a conservative default, not a lock manager. Raise it only when the job is safe to overlap. Pools govern scarce external resources; queues select workers only when those queues exist operationally.

Retry safety is designed per job, typically using unique keys/UPSERT, idempotency keys, durable checkpoints, remote-state checks, or deterministic file/checksum rules.

## Security model

The code holds only Connection IDs. Connection contents and deployment secrets are external to the repository. TLS verification remains at secure defaults; SQL values are parameterized; scaffold inputs become validated paths/identifiers rather than shell commands.

Structured logging redacts obvious secret field names and credential-bearing URI strings. Arbitrary object representations are not serialized because client/connection objects can hide credentials in `__str__`/`__repr__`; non-scalar objects are represented only by type name. Non-finite floating-point values are normalized so log lines remain valid JSON, and trusted execution identity cannot be overwritten by caller fields. `JobResult.artifact_uri` rejects common credential-bearing URI forms before they can be returned through XCom. These are defense-in-depth controls, not permission to log secret payloads.

Bootstrap/scaffold tooling refuses symlinked or junction-backed control/package paths where a local rename/write could escape the intended repository structure. Secret scanning does not follow symlinks or junctions outside the repository.

## Platform and validation boundary

The Airflow runtime is a POSIX concern. Native Windows is supported only as a fast Airflow-independent Python development loop; WSL2/Linux or the Ubuntu CI workflow owns Airflow installation, DAG integrity, local/serialized parsing, and end-to-end DAG execution. This keeps platform-specific runtime failures from being confused with failures in pure job code while preserving a single release gate.

The release pipeline validates the coordinated Airflow dependency set, the public `airflow.sdk` surface, lint/format, unit and DAG-integrity tests, secret scanning, wheel construction, DAG parsing/serialization, and deterministic execution of `example_simple_job`.

## Why there is no bigger framework

Airflow already provides DAGs, TaskFlow, operators, Hooks, Params, Variables, Connections, sensors, pools, queues, Dynamic Task Mapping, and isolated execution. Wrapping those broadly would create a second API to learn and eventually block native capabilities. This starter abstracts only repeated boilerplate that has a clear payoff and otherwise lets Airflow remain Airflow.
