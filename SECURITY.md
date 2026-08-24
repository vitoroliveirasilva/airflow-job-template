# Security Policy

Security is part of the design contract of `airflow-job-template`.

The template is intended to become the starting point for automation and data workloads that may interact with databases, APIs, enterprise systems, browsers, files, and credentials. A weak default in a template can be reproduced in many downstream projects, so security issues in reusable behavior are treated seriously.

This policy explains supported code, how to report a vulnerability, what information to include, the project's security boundaries, and what contributors should expect during handling.

---

## 1. Supported versions and branches

The repository is maintained as a rolling template rather than as multiple concurrently supported release series.

| Version / branch                                | Security support            | Purpose                               |
| ----------------------------------------------- | --------------------------- | ------------------------------------- |
| `prod`                                          | Supported                   | Current stable/default template       |
| `dev`                                           | Best effort / pre-release   | Active integration and development    |
| Older commits or copied revisions               | Not supported               | Historical snapshots                  |
| Downstream projects generated from the template | Not automatically supported | Maintained by their respective owners |

Security fixes are normally developed and validated through the development flow and promoted to `prod` after the appropriate quality gates pass.

If a vulnerability affects both `dev` and `prod`, the stable branch is the primary supported target.

A downstream project may remain vulnerable after this repository is fixed if it has copied an older revision. Consumers are responsible for evaluating and applying relevant upstream fixes.

---

## 2. Reporting a vulnerability

**Do not open a public GitHub issue, discussion, pull request, or commit that reveals an unpatched vulnerability.**

Use GitHub's private vulnerability reporting / Security Advisory workflow for this repository when the **Report a vulnerability** option is available in the repository's **Security** section.

If private vulnerability reporting is not available, avoid publishing exploit details publicly. Contact the repository owner through a private channel available on their GitHub profile and provide only enough information to establish a secure reporting channel.

Do not include real third-party credentials, production secrets, customer information, or unrelated sensitive data in a report.

---

## 3. What to include in a report

A useful report should contain as much of the following as is safely available:

- a concise title;
- affected branch, commit or file;
- affected component;
- vulnerability type;
- technical description;
- prerequisites;
- reproducible steps or proof of concept;
- expected security boundary;
- observed behavior;
- realistic impact;
- whether exploitation requires trusted/local access;
- whether exploitation crosses a privilege, repository or worker boundary;
- suggested remediation, if known;
- whether the issue appears to be actively exploited;
- relevant logs with secrets and personal data removed.

If the issue involves a dependency, include:

- package name;
- affected version;
- advisory/CVE/GHSA identifier when known;
- whether the vulnerable code path is actually reachable from this project;
- the safe version or mitigation when known.

A minimal reproducible example is preferred over a large archive containing unrelated application data.

---

## 4. High-value security areas

Reports are particularly useful when they affect reusable project behavior in any of these areas.

### Secret and credential exposure

Examples:

- committed credentials;
- secrets exposed by generated files;
- credentials appearing in structured logs;
- sensitive Connection data appearing in exceptions;
- credential-bearing URLs accepted as artifact metadata;
- secrets passed through Params or XCom;
- secret scanning bypasses;
- generated `.env` behavior that exposes sensitive values.

### Filesystem safety

Examples:

- path traversal;
- arbitrary file overwrite;
- unsafe archive extraction;
- symlink/junction escape;
- bootstrap/scaffold writes outside the expected repository root;
- control-path replacement;
- unsafe temporary file behavior;
- race conditions that defeat intended atomic writes.

### Command or code execution

Examples:

- shell injection;
- unsafe subprocess construction;
- executing Param/Variable content as code;
- template/scaffold input reaching an interpreter unexpectedly;
- unsafe deserialization;
- import-time execution from untrusted values.

### Database security

Examples:

- SQL injection;
- unsafe identifier handling represented as value parameterization;
- credentials embedded in source;
- transaction behavior that exposes inconsistent/unauthorized state;
- unbounded result handling that creates a practical resource-exhaustion path.

### HTTP and transport security

Examples:

- TLS verification disabled by default;
- credentials sent to unintended hosts;
- redirect behavior leaking Authorization data;
- sensitive headers logged;
- missing validation that creates SSRF-like behavior within the reusable helper;
- retry logic that repeats unsafe mutations.

### Airflow-specific boundaries

Examples:

- secrets placed in Params;
- secrets or large sensitive data placed in XCom;
- untrusted runtime data executed during DAG parsing;
- import-time calls leaking credentials;
- unsafe Connection handling;
- generated DAGs that bypass expected retry/failure security semantics;
- task isolation assumptions that expose host resources unexpectedly.

### Supply chain and CI

Examples:

- workflow token over-permission;
- untrusted code execution with elevated GitHub token capabilities;
- dependency confusion;
- unsafe artifact handling;
- mutable/untrusted action references where repository policy expects pinned actions;
- a packaging path that unintentionally includes secrets or local state.

---

## 5. Security model and expected boundaries

Understanding the intended model helps distinguish vulnerabilities from unsupported usage.

### Credentials are external to source code

Reusable code should contain Connection IDs or configuration references, not credential values.

Credential contents belong in:

- Airflow Connections;
- an approved Airflow Secrets Backend;
- environment/deployment secret management where appropriate.

The repository's examples, tests and source code must remain usable without real credentials.

### DAG parsing is not an operational trust boundary

DAG import/graph construction should remain side-effect free with respect to external systems.

Database access, HTTP calls, SAP calls, browser actions and runtime configuration access belong inside execution boundaries.

A change that makes untrusted or secret-bearing runtime data execute during import is considered security-relevant.

### Params are not a secret store

Params are validated per-run inputs and can be visible in Airflow interfaces/metadata.

Do not use Params for passwords, tokens, private keys or other secrets.

### XCom is for small execution metadata

Do not use XCom for:

- credentials;
- entire datasets;
- large files;
- sensitive payloads;
- credential-bearing artifact URIs.

Large artifacts should live in deployment-supported external/shared storage and tasks should exchange only the small reference required to locate them.

### Local worker files are not shared storage

A path written by one task may not exist on another worker.

Security-sensitive designs must not assume a local file implicitly crosses task/worker boundaries.

### Logging is defense in depth, not a secret store

The structured logging helpers attempt to redact obvious secret field names and credential-bearing URIs and avoid arbitrary object representations.

These controls do not make it acceptable to deliberately pass secret payloads to logging.

Callers must avoid logging sensitive material in the first place.

---

## 6. Current security-oriented design requirements

The project expects reusable changes to preserve these properties:

- TLS verification remains enabled by default;
- SQL values are parameterized;
- no generic `shell=True` execution helper;
- user/scaffold names are validated before becoming paths or identifiers;
- bootstrap/scaffold operations use filesystem APIs instead of shell interpolation;
- sensitive filesystem paths are protected from symlink/junction escape cases;
- secret scanning does not intentionally follow paths outside the repository;
- runtime credentials remain outside source code;
- log fields that look like secrets are redacted;
- arbitrary object representations are not serialized into structured logs;
- credential-bearing artifact URIs are rejected before entering normal XCom metadata;
- HTTP retry intent does not silently multiply with hidden client retries;
- permanent configuration failures can fail without consuming normal retries;
- mutable retry behavior must be explicitly safe.

A report demonstrating a bypass of one of these intended controls is in scope.

---

## 7. Severity considerations

The project does not publish a custom scoring system.

Impact is evaluated using factors such as:

- confidentiality impact;
- integrity impact;
- availability impact;
- default exploitability;
- privileges required;
- user interaction required;
- whether the issue escapes the repository/project root;
- whether the issue crosses task/worker boundaries;
- whether real secrets can be exposed;
- whether a generated downstream project inherits the flaw;
- whether exploitation is possible in the default/recommended configuration;
- whether the issue requires intentionally unsupported or insecure configuration.

A flaw in reusable scaffold/runtime behavior may receive more attention than an equivalent problem limited to an optional example because template behavior can propagate into many projects.

---

## 8. Dependency vulnerabilities

A CVE/GHSA affecting an installed package does not always imply that this repository has an exploitable vulnerability.

Dependency reports should distinguish:

1. package is present;
2. vulnerable version is installed by the project;
3. vulnerable feature/code path is reachable;
4. project configuration makes exploitation realistic.

Reports with reachability information are especially valuable.

For Apache Airflow or provider vulnerabilities that are entirely upstream and not caused or amplified by this template, report them to the relevant upstream security process.

This repository may still update constraints/versions/documentation when an upstream issue affects the supported baseline.

---

## 9. Airflow and provider vulnerabilities

Apache Airflow and its providers are third-party projects.

A report belongs here when the problem is caused by this repository, for example:

- unsafe use of a provider API;
- insecure template defaults;
- credentials exposed by a helper;
- retry behavior creating repeated side effects;
- unsafe DAG generation;
- packaging/configuration that activates an insecure path.

A vulnerability that reproduces unchanged in upstream Airflow/provider code should normally be reported upstream.

Do not publicly disclose a previously unknown upstream vulnerability here merely to demonstrate that the template is affected.

---

## 10. Out of scope

The following are generally not vulnerabilities in this repository by themselves:

- an operator deliberately configuring `verify=False` in a downstream private project;
- weak credentials chosen by a downstream deployment;
- insecure Airflow RBAC configuration unrelated to template code;
- unpatched servers outside this repository;
- a malicious DAG author who already has arbitrary code execution in the Airflow DAG codebase;
- denial of service requiring intentionally supplying an obviously enormous local test fixture with trusted repository write access;
- vulnerabilities that only exist after removing documented security checks;
- secret leakage caused solely by a downstream job explicitly logging its own raw secret;
- unsupported third-party modifications;
- generic scanner output without evidence that the finding applies to reachable project behavior.

Even for an out-of-scope report, maintainers may still improve documentation or hardening if the report reveals a realistic misuse pattern.

---

## 11. Handling and disclosure

When a private report is received, the expected process is:

1. acknowledge and reproduce the report;
2. determine affected branches/components;
3. assess impact and exploitability;
4. identify whether upstream coordination is necessary;
5. develop a fix privately when disclosure would increase risk;
6. add regression tests where practical;
7. run the normal security and CI quality gates;
8. promote the corrected code through the repository's release flow;
9. publish an advisory when appropriate;
10. credit the reporter if they want public credit and disclosure is appropriate.

The exact timing depends on severity, complexity, upstream dependencies and availability of a safe fix.

Please avoid public disclosure until a reasonable remediation/coordination process has occurred.

---

## 12. Security fix quality bar

A security fix should not only hide the immediate symptom.

When practical, it should include:

- root-cause correction;
- regression test;
- validation of adjacent input paths;
- documentation update if the expected usage changes;
- compatibility/migration note if the secure fix is breaking;
- confirmation that logging does not expose the exploit input;
- confirmation that generated projects receive the corrected behavior when applicable.

A fix must not weaken unrelated security controls simply to preserve backwards compatibility.

---

## 13. Testing security-sensitive changes

Depending on the affected area, useful checks include:

```bash
ruff format --check .
ruff check .
pytest
python scripts/check_secrets.py
python -m compileall -q src dags scripts tests examples
```

Changes affecting Airflow behavior should also pass the Linux/WSL2/CI Airflow acceptance path, including DAG import/serialization checks.

Changes affecting scaffold/bootstrap security should include disposable-copy tests that attempt invalid names, unsafe paths, repeated bootstrap, and symlink/junction boundary cases where the platform supports them.

Never test a security fix against production credentials or external systems unless you are authorized to do so and the test is explicitly safe.

---

## 14. Secret scanning limitations

`python scripts/check_secrets.py` is a project-level defense-in-depth check.

It is not a complete secret-detection product and does not guarantee that a repository is free of secrets.

Contributors remain responsible for reviewing changes before publishing them.

If you accidentally commit a real credential:

1. treat the credential as compromised;
2. revoke or rotate it at the source;
3. remove it from the current tree;
4. evaluate repository history and downstream copies;
5. do not assume deleting the latest file version makes the credential safe again.

Do not post the compromised secret in an issue while asking for help.

---

## 15. Security contacts in issue templates

Security reports should not use normal bug-report templates.

The issue-template chooser intentionally directs suspected vulnerabilities back to this policy.

If private vulnerability reporting is enabled, use the repository's Security interface instead of a public issue.

---

## 16. Responsible use

Only test systems and resources you own or are explicitly authorized to assess.

This policy describes how to report vulnerabilities in this repository; it does not grant authorization to test third-party Airflow deployments, APIs, databases, SAP systems, websites, browsers, or infrastructure.

---

Thank you for reporting security issues responsibly and for helping prevent insecure defaults from propagating into downstream projects.
