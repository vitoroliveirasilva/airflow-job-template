## Summary

<!-- Explain the change in a few precise sentences. What does this PR do? -->

## Problem / motivation

<!-- What concrete problem does this solve? Why is the change needed in the reusable template? -->

## Solution

<!-- Describe the chosen approach and the important implementation decisions. -->

## Scope

<!-- List the intentionally changed areas. Call out anything deliberately left out. -->

- 

## Change classification

Check all that apply:

- [ ] Bug fix
- [ ] Security hardening
- [ ] New reusable capability
- [ ] Integration/provider change
- [ ] Scaffold/bootstrap change
- [ ] Runtime/architecture change
- [ ] Refactor with no intended behavior change
- [ ] Tests
- [ ] Documentation
- [ ] CI/tooling
- [ ] Dependency change
- [ ] Breaking change

---

## Architecture review

### Layer affected

Check all that apply:

- [ ] `dags/` orchestration
- [ ] `runtime/`
- [ ] `jobs/`
- [ ] `integrations/`
- [ ] `observability/`
- [ ] `scripts/`
- [ ] `examples/`
- [ ] `requirements/` / packaging
- [ ] `.github/` / CI
- [ ] Documentation only

### Architectural compatibility

Confirm or explain `N/A` below:

- [ ] DAG files remain thin and focused on orchestration.
- [ ] No DB/HTTP/SAP/browser/heavy discovery was added at DAG import time.
- [ ] Plain business functions execute inside task/operator boundaries rather than directly in an `@dag` body.
- [ ] The change uses Airflow/provider-native capabilities where they are clearer than a new abstraction.
- [ ] Any new abstraction solves a demonstrated repeated problem and has intentionally narrow scope.
- [ ] Business logic remains testable without starting Airflow services where practical.

Architecture notes / `N/A`:

---

## Runtime behavior

For changes that perform or orchestrate external work, describe the operational contract.

### Timeouts

<!-- Network timeout, task execution timeout, or N/A. -->

### Retry behavior

<!-- Which failures are retryable/permanent? Are there hidden client-level retries? -->

### Idempotency / side-effect safety

<!-- Explain why repeated Airflow attempts are safe for mutable work, or N/A. -->

### Concurrency

<!-- Can multiple runs/tasks overlap safely? Explain controls or N/A. -->

---

## Configuration and data boundaries

Confirm or explain `N/A`:

- [ ] Credentials/endpoints are stored in Connections/Secrets Backend rather than source code.
- [ ] Params are validated and contain no secrets.
- [ ] Runtime Variables are not resolved at DAG import time.
- [ ] XCom contains only small metadata/identifiers.
- [ ] Large/cross-task artifacts use appropriate shared/external storage.
- [ ] Local worker files are not assumed to exist on another worker.
- [ ] SQL values are parameterized where applicable.
- [ ] TLS verification remains enabled by default.

Notes / `N/A`:

---

## Security review

- [ ] No credentials, tokens, passwords, private keys, cookies, Authorization values, or sensitive real data were added.
- [ ] Logs/errors do not expose secret-bearing objects or credential-bearing URLs.
- [ ] No unsafe shell interpolation or execution of runtime text as code was introduced.
- [ ] Filesystem changes validate paths/names and preserve repository-boundary protections where applicable.
- [ ] New dependencies/providers were evaluated for necessity and are optional when they are not universally required.
- [ ] `python scripts/check_secrets.py` passes.

Security considerations / `N/A`:

---

## Testing

### Tests added or changed

<!-- Describe regression, unit, DAG-integrity, integration, or generator tests. -->

### Required local checks

Mark each check you actually ran:

- [ ] `ruff format --check .`
- [ ] `ruff check .`
- [ ] `pytest`
- [ ] `python scripts/check_secrets.py`

Additional checks when relevant:

- [ ] `python -m compileall -q src dags scripts tests examples`
- [ ] `python -m pip wheel --no-deps . --wheel-dir dist`
- [ ] `python -m pip check`
- [ ] public `airflow.sdk` import smoke
- [ ] `airflow dags list --local`
- [ ] `airflow dags list-import-errors --local`
- [ ] `airflow dags reserialize`
- [ ] serialized `airflow dags list-import-errors`
- [ ] `airflow dags test example_simple_job 2026-08-21 ...`
- [ ] disposable bootstrap/scaffold end-to-end smoke
- [ ] marked integration tests (`pytest -m integration`)

### Test evidence

```text

```

---

## Scaffold / generator impact

- [ ] This PR does not change generated project output.
- [ ] Generated output changed and the change is intentional.
- [ ] Simple Job generation was validated.
- [ ] Explicit Workflow generation was validated.
- [ ] Isolated Job generation was validated.
- [ ] Bootstrap behavior was validated in a disposable copy.

Generated-output notes / `N/A`:

---

## Dependency / provider impact

<!--
If this PR adds, removes, or upgrades a dependency/provider, explain why it is required,
why it is core vs optional, constraints compatibility, runtime implications, and security/maintenance impact.
Otherwise write N/A.
-->

N/A

---

## Backwards compatibility

- [ ] No known breaking change.
- [ ] Breaking change is intentional and documented.

Potentially affected public imports, scaffold output, config, behavior, or downstream projects:

<!-- Write "None known" when applicable. -->

### Migration

<!-- Required only for breaking changes. -->

N/A

---

## Documentation

- [ ] No documentation change is required.
- [ ] README updated.
- [ ] `docs/architecture.md` updated.
- [ ] `docs/development.md` updated.
- [ ] examples updated.
- [ ] migration/compatibility guidance added.
- [ ] security/contribution documentation updated.

Documentation notes:

---

## Related issues

<!-- Examples: Closes #123, Fixes #123, Related to #123 -->

---

## Final author checklist

- [ ] The PR is focused and does not include unrelated cleanup/format churn.
- [ ] I reviewed my own diff.
- [ ] I added/updated tests when behavior changed.
- [ ] I documented user-facing or architectural changes.
- [ ] I did not weaken a quality/security gate merely to make CI pass.
- [ ] I explicitly documented any breaking change.
- [ ] I considered retry/idempotency for mutable operations.
- [ ] I considered Airflow parse-time vs runtime boundaries.
- [ ] I considered whether a native Airflow/provider capability is preferable to new framework surface.
- [ ] Required CI is expected to pass before merge.

## Additional reviewer context

<!-- Anything else reviewers need to know. -->
