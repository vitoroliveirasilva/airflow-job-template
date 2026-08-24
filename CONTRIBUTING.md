# Contributing to airflow-job-template

Thank you for considering a contribution to `airflow-job-template`.

This repository is a production-oriented starting point for Python jobs, automations, integrations, and data workflows orchestrated by Apache Airflow. Its goal is not to hide Airflow behind another framework. Contributions should make common work safer and easier while keeping Airflow concepts visible and native capabilities available.

This document explains the contribution model, architectural expectations, development workflow, quality gates, and review criteria used by the project.

---

## 1. Project principles

Changes should preserve the following principles.

### Airflow remains the orchestration layer

Use Airflow DAGs, TaskFlow, operators, Hooks, Params, Connections, Variables, pools, queues, sensors, Dynamic Task Mapping, and isolation mechanisms directly when they are the clearest solution.

Do not introduce a second workflow DSL, scheduler abstraction, generic orchestration engine, or broad wrapper API over Airflow.

### DAGs stay thin

`dags/` owns orchestration concerns such as:

- schedules;
- task topology;
- task boundaries;
- retries and execution timeouts;
- Params;
- pools and queues;
- Airflow-native operators and behavior.

Business or use-case logic belongs under `src/<project_package>/jobs/`.

Repeated runtime integration helpers belong under `src/<project_package>/integrations/`.

### Parse time is not runtime

Importing a DAG or constructing its graph must not perform operational work.

Do not perform the following at DAG import time or directly in an `@dag` body:

- HTTP requests;
- database queries;
- SAP calls;
- browser/RPA actions;
- expensive filesystem discovery;
- large dynamic discovery;
- `Variable.get()` calls for runtime configuration;
- provider/SDK initialization that belongs to task execution;
- calls to ordinary business functions that perform work.

Plain Python business functions must execute inside an Airflow task/operator boundary.

### Prefer small, justified abstractions

New abstractions should solve a repeated, demonstrated problem.

A contribution should not add a generic helper only because it may become useful in the future.

If an Airflow or provider-native feature already models the requirement well, prefer it.

### Runtime behavior must be explicit

For external or mutable operations, reviewers should be able to understand:

- timeout behavior;
- retry behavior;
- whether retry is safe;
- idempotency strategy;
- transaction boundaries;
- concurrency assumptions;
- failure classification;
- where credentials and configuration come from;
- what is stored in XCom;
- where large artifacts are persisted.

### Secure defaults are part of the API

Do not weaken secure defaults for convenience.

Credentials belong in Airflow Connections or an approved Secrets Backend. Secrets must not be committed, logged, stored in Params, embedded in artifact URIs, or returned through XCom.

TLS verification remains enabled unless the deployment explicitly controls an exception outside the reusable template.

---

## 2. Repository structure

The main responsibilities are:

```text
dags/
    Thin Airflow definitions, task graph, schedules and orchestration policy.

src/<project_package>/runtime/
    Shared runtime contracts such as JobSpec, TaskPolicy, context adaptation,
    error policy and the intentionally small Simple Job factory.

src/<project_package>/jobs/
    Business and use-case logic designed to be unit-testable independently
    from the scheduler and metadata database.

src/<project_package>/integrations/
    Small runtime I/O helpers and explicit extension points.

src/<project_package>/observability/
    Structured logging and related observability helpers.

scripts/
    Bootstrap, job scaffolding, secret scanning and repository tooling.

examples/
    Demonstrations and patterns that should not automatically become core
    abstractions.

tests/
    Unit, DAG-integrity and explicitly marked integration tests.

requirements/
    Core/development requirements and opt-in integration/provider sets.

docs/
    Architecture and development contracts.
```

Read the following before making architectural changes:

- [`README.md`](README.md)
- [`docs/architecture.md`](docs/architecture.md)
- [`docs/development.md`](docs/development.md)

If this file and the architecture documentation appear to conflict, open an issue or call out the conflict in the pull request instead of silently choosing a new convention.

---

## 3. Branch and pull request model

The repository uses two long-lived branches:

- `dev`: integration branch for active development;
- `prod`: stable branch and default branch of the repository.

### Normal contribution flow

Contributions should normally target `dev`.

```text
feature/fix branch
       ↓
      dev
       ↓
 release/promotion
       ↓
      prod
```

Do not target `prod` directly for normal features, refactors, documentation work, dependency changes, or routine fixes.

Promotion from `dev` to `prod` is a maintainer/release action after the required quality gates are green.

### External contributors

If you do not have write access:

1. fork the repository;
2. create a focused branch in your fork;
3. make and validate the change;
4. open a pull request targeting `dev`.

If you are a collaborator, use a dedicated branch instead of committing directly to `dev` or `prod`.

### Branch naming

No single naming convention is mandatory, but clear names are preferred, for example:

```text
fix/http-timeout-classification
feat/new-job-validation
docs/airflow-installation
test/database-client-transactions
refactor/runtime-error-policy
```

---

## 4. Before opening an issue or pull request

Search existing issues and pull requests first.

For substantial changes, open an issue before implementation when the proposal introduces or changes any of the following:

- core architecture;
- public runtime contracts;
- scaffold output;
- package layout;
- dependency strategy;
- Airflow compatibility baseline;
- provider strategy;
- retry/error semantics;
- configuration model;
- security behavior;
- backwards compatibility;
- a new reusable abstraction;
- a new integration intended to become part of the template core.

Small, well-scoped bug fixes, tests, typo corrections, and documentation improvements may be submitted directly.

---

## 5. What makes a good contribution

A strong contribution has a concrete problem statement and the smallest maintainable solution that addresses it.

Examples of contributions that fit the project well:

- a reproducible bug fix;
- stronger input validation;
- improved failure classification;
- a security hardening change;
- better testability;
- a missing test for an existing contract;
- documentation that prevents a likely operational mistake;
- a scaffold improvement with clear repeated value;
- compatibility work required by a supported Airflow/Python version;
- an integration helper proven useful across multiple jobs;
- a correction that makes Airflow-native behavior more visible rather than less visible.

Changes that usually require stronger justification:

- new framework layers;
- generic factories for many unrelated Airflow concepts;
- global dependency additions for optional integrations;
- new retry loops inside clients;
- automatic magic configuration;
- large helpers with behavior inferred from naming or runtime inspection;
- platform assumptions that do not hold across Airflow deployments;
- wrappers around standard operators/Hooks without repeated concrete benefit.

---

## 6. Development environment

### Python baseline

Python `3.12` is the development and CI baseline.

The repository currently validates against Apache Airflow `3.3.1` using the official Airflow constraints matching Python `3.12`.

Do not update the supported Airflow or Python baseline incidentally inside an unrelated pull request.

### Windows PowerShell: fast pure-Python loop

Native Windows is supported for the Airflow-independent development loop.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r requirements/dev.txt
python -m pip install --no-deps -e .
```

Then run:

```powershell
ruff format --check .
ruff check .
pytest
python scripts/check_secrets.py
```

Airflow runtime/DAG-integrity modules may be intentionally skipped on native Windows.

A green native Windows run does **not** replace the Linux/WSL2/CI Airflow acceptance gate.

### Linux, macOS or WSL2: full Airflow environment

Create the environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

Install Airflow with the matching official constraints:

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

Do not debug template code around an inconsistent Airflow installation. If `pip check` or the public `airflow.sdk` smoke fails, repair the environment first.

---

## 7. Required local quality checks

At minimum, run:

```bash
ruff format --check .
ruff check .
pytest
python scripts/check_secrets.py
```

Before proposing a formatting change, you may apply formatting locally with:

```bash
ruff format .
```

A contribution should not mix unrelated formatting churn with functional changes.

### Compile check

For changes touching generated files, imports, scaffolding, or package structure, also run:

```bash
python -m compileall -q src dags scripts tests examples
```

### Package build

For changes affecting packaging or project metadata:

```bash
python -m pip wheel --no-deps . --wheel-dir dist
```

The project should produce one valid wheel without accidentally packaging temporary/local artifacts.

---

## 8. Full Airflow validation

Changes that affect DAGs, runtime contracts, Airflow imports, providers, schedules, serialization, task policy, or scaffolded DAG output should also pass full Airflow validation.

Configure a local Airflow home:

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export PYTHONPATH="$PWD/src"
```

Then:

```bash
airflow db migrate

airflow dags list --local
airflow dags list-import-errors --local

airflow dags reserialize
airflow dags list
airflow dags list-import-errors
```

There should be no DAG import errors in either the direct filesystem view or serialized metadata view.

Run the deterministic smoke DAG when appropriate:

```bash
airflow dags test example_simple_job 2026-08-21 \
  --dagfile-path "$PWD/dags/example_simple_job.py"
```

Do not use a real mutable external integration as an acceptance smoke test unless the environment and inputs are explicitly safe.

---

## 9. CI acceptance gate

The GitHub Actions quality workflow is the release-level acceptance gate.

It currently validates:

1. Python 3.12;
2. Airflow 3.3.1 with official constraints;
3. Standard provider and development dependencies;
4. `pip check`;
5. public `airflow.sdk` import smoke;
6. Ruff lint;
7. Ruff formatting;
8. Pytest with coverage;
9. DAG integrity;
10. secret scanning;
11. end-to-end bootstrap;
12. generation of simple, workflow and isolated jobs;
13. compilation of generated code;
14. generated-project Ruff/test smoke;
15. wheel build;
16. local DAG parsing;
17. Airflow metadata migration;
18. DAG serialization;
19. serialized DAG import checks;
20. end-to-end execution of `example_simple_job`.

A pull request should not be merged while required CI is failing unless the failure has been conclusively identified as unrelated infrastructure breakage and the maintainer documents that decision.

Do not bypass or weaken a quality gate merely to make a pull request green.

---

## 10. Testing expectations

### Unit tests

Business logic should be testable without starting the scheduler, API server, or Airflow metadata database.

Use plain fixtures and fakes/mocks at integration boundaries.

When fixing a bug, add a regression test whenever practical.

A useful regression test should fail before the fix and pass after it.

### DAG integrity tests

Changes to DAG construction should preserve project invariants such as:

- unique DAG and task IDs;
- deterministic timezone-aware start dates;
- intentional `catchup`;
- valid tags;
- task execution timeouts;
- safe import-time behavior.

### Integration tests

Tests that require a real API, DB, browser, SAP system, or another external service must use the `integration` marker:

```python
import pytest


@pytest.mark.integration
def test_real_dependency():
    ...
```

Run explicitly:

```bash
pytest -m integration
```

Real integration tests must not silently join the default test gate.

### Test data

Never put real credentials, production records, private customer data, internal secrets, or sensitive payloads in fixtures.

Use synthetic/minimal data.

---

## 11. DAG contribution checklist

When changing or adding a DAG, verify all of the following:

- the DAG file remains mostly orchestration declarations, tasks and dependencies;
- importing the DAG does not access DB, HTTP, SAP, browser, or other external systems;
- graph construction does not run ordinary business operations;
- plain Python business functions execute inside task/operator boundaries;
- `start_date` is deterministic and timezone-aware;
- schedule is intentional;
- `catchup` is intentional;
- task execution timeout exists where appropriate;
- network clients have explicit timeouts;
- retry behavior matches the operation;
- mutable retries are demonstrably safe;
- permanent configuration/domain failures do not waste retries;
- concurrency/overlap is intentional;
- Params validate manual input;
- Params contain no secrets;
- credentials/endpoints belong in Connections/Secrets Backend;
- XCom contains small metadata/identifiers, not datasets;
- cross-task files live in shared/external storage;
- logs do not expose credentials or sensitive payloads;
- optional/heavy dependencies do not become unnecessary DAG import-time dependencies.

---

## 12. Retry, idempotency and side effects

Retries repeat work.

For every mutable operation, the contribution must make retry safety understandable.

Common acceptable strategies include:

| Workload | Typical strategy                                           |
| -------- | ---------------------------------------------------------- |
| API → DB | unique key, UPSERT/MERGE, watermark, partition replacement |
| DB → API | idempotency key, durable outbox/status, remote-state check |
| RPA      | locate stable object, check state, mutate, verify state    |
| Files    | deterministic name, checksum, ledger, atomic move/write    |

Do not add hidden client-level retry loops that multiply Airflow task retries.

HTTP mutation methods should only be classified as retry-safe when idempotency has actually been established.

A successful external mutation must not be converted into an automatic retry merely because cleanup or diagnostic capture later failed.

---

## 13. Error semantics

The shared operational error hierarchy is intentionally small.

- `JobConfigurationError`: permanent configuration/input problem;
- `NonRetryableJobError`: known permanent failure;
- `RetryableJobError`: transient failure that may use normal Airflow retries.

When explicit Python workflows use this hierarchy, apply the shared Airflow error adapter at the task boundary.

Do not expand the hierarchy into a universal domain exception taxonomy.

Domain-specific exceptions may remain inside individual jobs.

---

## 14. Connections, Variables, Params and XCom

Use the correct Airflow mechanism for the information being represented.

| Information                | Expected location            |
| -------------------------- | ---------------------------- |
| DAG structure              | versioned code               |
| task policy                | versioned code               |
| secret/credential/endpoint | Connection / Secrets Backend |
| per-run validated input    | Param                        |
| runtime non-secret setting | Variable                     |
| small task metadata        | XCom                         |
| large artifact/data        | shared/external storage      |

Avoid environment-specific `if production: ...` logic in reusable DAGs when the same resource role can be represented by a consistent Connection ID with environment-specific contents.

Do not use `Variable.get()` at import time to build runtime behavior.

---

## 15. Integration contributions

### HTTP

HTTP changes should preserve:

- explicit connect/read timeouts;
- TLS verification;
- no hidden retry multiplication;
- clear retryability classification;
- validation before network I/O;
- relative endpoint use against Connection-managed hosts;
- no secret leakage through logs/errors.

Do not add a universal pagination abstraction without a concrete repeated contract.

### Database

Database changes should preserve:

- parameterized values;
- explicit transaction boundaries;
- commit/rollback/cleanup;
- driver-compatible DB-API usage;
- bounded or streaming reads for potentially large result sets;
- job-specific SQL dialect and idempotency decisions.

Do not turn the shared helper into an ORM or generic database framework.

### SAP

There is intentionally no universal SAP adapter.

Add OData/RFC/HANA/other adapters only when a real protocol contract exists.

SAP GUI automation belongs to an RPA/isolated execution profile.

### RPA and specialized runtimes

Do not make browser engines, Java runtimes, proprietary SDKs, Docker, Kubernetes, or other heavyweight integrations unconditional core dependencies.

Prefer Airflow-native isolation appropriate to the deployment.

---

## 16. Adding dependencies or providers

A new dependency must have a concrete reason.

Before adding an Airflow provider:

1. verify whether an official/community provider already exists;
2. keep it under `requirements/optional/` unless every generated project genuinely requires it;
3. install it with the same Airflow constraints URL;
4. include the pinned Airflow version in the same resolver transaction;
5. keep heavy/optional imports out of top-level DAG code;
6. add unit tests using fakes;
7. add marked integration tests only when a real test environment exists.

Do not add packages because they may be useful someday.

Dependency changes should explain their security, maintenance, package-size, platform and compatibility implications.

---

## 17. Scaffold and bootstrap changes

Changes to `scripts/bootstrap_project.py` or `scripts/new_job.py` deserve extra review because they mutate/generate project files.

Preserve the following properties:

- validated names and paths;
- no shell interpolation/execution;
- atomic writes where applicable;
- refusal of unsafe overwrites;
- refusal of repeated bootstrap;
- protection against symlink/junction/control-path escapes;
- no reading/writing of secret `.env*` data during bootstrap;
- generated code compiles;
- generated code passes Ruff;
- generated unit tests pass.

Use a disposable copy for generator testing.

Example:

```bash
cp -R . /tmp/airflow-template-check
cd /tmp/airflow-template-check

python scripts/bootstrap_project.py demo_project
python scripts/new_job.py customer_sync --type simple
python scripts/new_job.py billing_pipeline --type workflow
python scripts/new_job.py portal_update --type isolated

python -m compileall -q src dags scripts tests examples
ruff check .
ruff format --check .
pytest
```

Do not bootstrap the working template repository just to test the bootstrap.

---

## 18. Security requirements

Contributions must not:

- commit `.env` files containing secrets;
- include credentials or tokens in examples/tests;
- disable TLS verification as a reusable default;
- construct SQL using untrusted string interpolation;
- expose credential-bearing URIs in logs or XCom;
- log arbitrary connection/client objects;
- execute user-controlled shell strings;
- turn Param content into executable code;
- follow repository tooling symlinks into uncontrolled paths;
- introduce insecure defaults under the assumption that downstream projects will fix them.

Run:

```bash
python scripts/check_secrets.py
```

before opening a pull request.

If you discover a vulnerability, follow [`SECURITY.md`](SECURITY.md) instead of opening a public issue.

---

## 19. Logging and observability

Use standard Python logging and the project's structured logging utilities where appropriate.

Logs should communicate operational state without becoming a secret transport.

Do not log:

- passwords;
- tokens;
- cookies;
- Authorization headers;
- private keys;
- raw Connection objects;
- full sensitive payloads;
- credential-bearing URLs;
- unnecessary personal data.

Preserve trusted execution identity fields. Caller-provided fields must not overwrite run identity.

Avoid serializing arbitrary object `repr()` output because library objects may include credentials.

---

## 20. Documentation expectations

Update documentation when a change affects:

- installation;
- supported versions;
- scaffold usage;
- generated layout;
- public APIs;
- configuration;
- retry/error behavior;
- security behavior;
- dependency strategy;
- environment assumptions;
- user-visible commands;
- backwards compatibility.

Documentation examples should be executable or intentionally pseudocode-like, not deceptively close to runnable code with missing safety constraints.

Prefer explaining why a rule exists when violating it could create operational risk.

---

## 21. Backwards compatibility

Identify compatibility impact explicitly.

A change may be breaking when it:

- removes or renames a public import;
- changes scaffold output in a way that invalidates existing workflows;
- changes default schedules/retry behavior;
- changes configuration names or semantics;
- removes a supported execution pattern;
- changes accepted values or validation;
- changes package structure consumed by downstream projects;
- changes runtime result/error contracts.

Breaking changes require:

1. a clear justification;
2. migration guidance;
3. updated tests;
4. updated documentation;
5. explicit mention in the pull request.

Do not hide a breaking change inside a refactor.

---

## 22. Commit guidance

Keep commits understandable and scoped.

There is no mandatory commit-message standard, but messages should describe the change clearly.

Prefer:

```text
Fix non-retryable HTTP status classification
Add regression tests for bootstrap path validation
Document isolated job dependency strategy
```

Avoid:

```text
update
fix
changes
stuff
final
```

Do not include generated caches, virtual environments, local Airflow databases, logs, build artifacts, or editor state.

---

## 23. Pull request expectations

A pull request should answer:

- What problem is being solved?
- Why does the project need this change?
- What is the chosen solution?
- What alternatives were considered when architecture is affected?
- What is the compatibility impact?
- What is the security impact?
- What tests were added or updated?
- Which local quality gates were run?
- Does the change require full Airflow validation?
- Does it change generated projects?
- Does documentation need to change?

Keep the PR focused.

Avoid combining a feature, large refactor, dependency upgrade, formatting sweep, and unrelated cleanup in the same change.

Use the repository pull request template completely. `N/A` is better than silently deleting a relevant section.

---

## 24. Review criteria

Maintainers may request changes when a contribution:

- duplicates an Airflow-native feature without sufficient benefit;
- creates unnecessary framework surface;
- performs external work at parse time;
- weakens security defaults;
- adds hidden retries;
- makes mutable retry safety unclear;
- introduces an unconditional optional dependency;
- makes business logic harder to test without Airflow;
- stores large/sensitive data in XCom;
- assumes local files are shared across workers;
- hides a breaking change;
- lacks regression coverage for a bug fix;
- causes CI or DAG serialization failures;
- introduces environment-specific behavior into reusable core code without justification.

A technically working implementation is not automatically a good fit for the template. Maintainability and operational predictability are part of correctness.

---

## 25. Licensing

By submitting a contribution, you agree that your contribution may be distributed under the repository's [MIT License](LICENSE).

By participating in this project, you also agree to follow the repository's [Code of Conduct](CODE_OF_CONDUCT.md).

Thank you for helping keep the template small, explicit, secure, testable, and useful.
