---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Support

Version `0.2.1` is a source-only public preview. There is no published Maven
artifact yet, the API can change before `1.0.0`, and fixes target the default
branch. Support is evaluation-only: the project does not provide an SLA or
backported patch releases.

## Where to report

* A wrong result, recovery failure, resource leak, startup failure, or
  documentation defect: [Bug report](.github/ISSUE_TEMPLATE/bug_report.yml).
* A missing capability or a request to widen a boundary:
  [Feature request](.github/ISSUE_TEMPLATE/feature_request.yml).
* A suspected vulnerability: follow [SECURITY.md](SECURITY.md) through the
  private advisory form. Never open a public issue for an unpatched
  vulnerability.

## Support scope

The [capability matrix](docs/product/reference/capability-matrix.md) is the
authority for supported behavior and stable refusals. [Roadmap](ROADMAP.md)
records open work and is not a support claim. Issues are triaged against the
current preview and default branch; older local builds are supported only by
first reproducing the behavior on the current source.

A good bug report includes the exact source or artifact identity, the Batch or
Stream runtime, deployment mode, host JDK and operating system, cluster
topology when relevant, a self-contained reproduction, expected and observed
results, and redacted logs or diagnostics. Security reports have additional
private-handling requirements in [SECURITY.md](SECURITY.md).
