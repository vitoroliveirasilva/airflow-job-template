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

`JobRunContext` adapts only the runtime fields normal Python logic needs, making jobs testable with a plain dataclass fixture. It does not reproduce the full Airflow `Context`.

`JobResult` carries small scalar metadata only. Large objects belong in external/shared storage.

### Simple Job factory

`build_single_task_dag()` is the only deliberate convenience abstraction over Airflow. It:

1. applies `JobSpec`;
2. creates one TaskFlow task;
3. applies `TaskPolicy`;
4. adapts the current context;
5. invokes the job callable;
6. maps non-retryable job errors to Airflow fail-without-retry;
7. logs start/completion and returns small metadata.

It contains no integration detection, no topology engine, and no secret/config loader.

## Explicit workflows

A multi-step DAG remains workflows-as-code. TaskFlow dependencies should be visible in the DAG module and Airflow operators/providers can be used directly. Small Python functions can stay in the job package so they are unit-testable without Airflow.

Dynamic Task Mapping is preferred when independent work items are discovered at runtime. Discovery runs in a task and returns small identifiers, never a huge dataset.

## Integration boundaries

### HTTP

The helper constructs `HttpHook` only at runtime using a Connection ID, supplies connect/read timeouts, disables the Hook's automatic status exception only so statuses can be classified into project retry intent, and leaves client retries at zero. It does not invent a generic pagination protocol beyond one opt-in page/page-size helper.

### Database

The DB helper uses the Hook selected by an Airflow Connection. It provides explicit DB-API transaction/cleanup, batch writes, bounded fetch, streaming chunks, and exposes the Hook for cases where provider-native features are better. SQL dialect/idempotency strategy remains job-specific.

### Files

Only same-task local utilities are provided: safe basename validation, checksum, atomic writes. The project intentionally has no helper that pretends local paths are shared between workers.

### SAP

No universal adapter exists. `integrations/sap/` documents where a concrete OData/RFC/HANA adapter can be added after its real contract is known. SAP GUI is an RPA/isolation concern.

### RPA / specialized dependencies

The template provides patterns, not a browser framework. State check, mutation, verification, diagnostic capture, and cleanup should remain in one task when browser/session continuity matters. The Airflow-native executor/operator appropriate to the deployment owns isolation.

## Operational error semantics

- `JobConfigurationError`: permanent configuration/input problem.
- `NonRetryableJobError`: known permanent failure; Simple Job stops retries.
- `RetryableJobError`: transient failure; normal Airflow retry policy applies.

The hierarchy is intentionally tiny. It is not a substitute for domain exceptions inside jobs.

## Concurrency and idempotency

`max_active_runs=1` is a conservative default, not a lock manager. Raise it only when the job is safe to overlap. Pools govern scarce external resources; queues select workers only when those queues exist operationally.

Retry safety is designed per job, typically using unique keys/UPSERT, idempotency keys, durable checkpoints, remote-state checks, or deterministic file/checksum rules.

## Security model

The code holds only Connection IDs. Connection contents and deployment secrets are external to the repository. TLS verification remains at secure defaults; SQL values are parameterized; scaffold inputs become validated paths/identifiers rather than shell commands; logging redacts obvious secret field names and callers must avoid logging sensitive payloads.

## Why there is no bigger framework

Airflow already provides DAGs, TaskFlow, operators, Hooks, Params, Variables, Connections, sensors, pools, queues, Dynamic Task Mapping, and isolated execution. Wrapping those broadly would create a second API to learn and eventually block native capabilities. This starter abstracts only repeated boilerplate that has a clear payoff and otherwise lets Airflow remain Airflow.
