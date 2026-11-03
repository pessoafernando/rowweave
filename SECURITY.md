---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Reporting a Vulnerability

CI resolves the complete reactor dependency tree, including test and runtime
scopes, and checks it against [OSV](https://osv.dev/). Missing modules, malformed
responses and service failures fail the security job. There are no suppressed
advisories. Reproduce the scan with:

```bash
mvn -B -ntp dependency:tree > dependency-tree.log
python3 tools/oss_dependency_security_gate.py dependency-tree.log
```

The scan requires network access and reflects the advisory database at query
time. It complements runtime security tests and does not establish that every
dependency or execution path is free of vulnerabilities.

Report suspected security issues privately through the repository
[security advisory form](https://github.com/pessoafernando/rowweave/security/advisories/new).
Do not open a public issue for an unpatched vulnerability. Coordinate disclosure
with the maintainers until a fix and advisory are available.

## Supported versions

| Version | Support status | Where fixes land |
|---|---|---|
| `0.2.0` source preview | Evaluation only; no artifact is published | Default branch |

There are no earlier supported releases and no backport policy. Supply fixes or
validation against the current default branch.

Please include the version or source commit, the runtime and deployment mode,
host JDK and operating system, the configuration or cluster topology that
triggers the issue, and a self-contained reproduction. For a cross-host or
recovery issue, include the minimal topology and the redacted persisted-state
layout. Omit credentials, certificates, key material, endpoints, and other
secrets from the report and attachments.

Please include the version, the configuration or cluster topology that triggers
the issue, and a reproduction. We respond within five working days with a
triage outcome.

## Trust boundaries

RowWeave runs inside a host process and across a cluster of worker JVMs. These
are the security boundaries in scope:

* the control-plane RPC between job client, master and worker, which requires
  mutual TLS and pinned certificates in cluster mode;
* the shuffle data plane between workers, including connection authentication
  and frame validation;
* the persisted formats on disk: spill workspaces, run-state stores, object
  storage quality records, release and deployment manifests. Untrusted bytes in
  any of these formats must fail closed;
* the deserialisation whitelist: only registered types are reconstructed, and
  an object stream that names anything else is rejected;
* the classpath deployment scanner, which rejects shadowed or unsigned
  operator code;
* expression bytecode generation, whose generated classes are bounded by a
  size and depth budget.

Host credentials, storage buckets and database endpoints supplied through
configuration are treated as secrets and are never written to logs, metrics or
persisted plans.

Test fixtures use synthetic keys and sample endpoints only. They are not valid
credentials and must never be reused in production.
