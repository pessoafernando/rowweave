---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Documentation maintenance

This page defines when product documentation changes, who reviews that change,
and which check must pass before merge. The [capability
matrix](capability-matrix.md) remains the authority for supported behavior; a
documentation edit cannot turn planned work into support.

## Ownership

A documentation change is owned by the author and reviewed by the maintainer
responsible for the affected surface. The reviewer confirms that scope,
defaults, limits, failures, security guidance, release status, and examples
match the tested implementation. Documentation-only changes still require that
review.

## Change triggers

| Surface | Update documentation when | Required reviewer evidence |
|---|---|---|
| Public API | A class, method, signature, type boundary, compatibility statement, approved example, or public error changes. | API inventory, relevant API tests, compilation of changed examples, and quality suite. |
| Configuration | A property, family, default, valid value, scope, validation, or observable effect changes. | Configuration parity, targeted configuration evidence, and quality suite. |
| Runtime behavior | Output, ordering, null policy, resource behavior, admission, recovery, lifecycle, or refusal behavior changes. | Relevant behavior tests and the parity checks in the quality suite. |
| Monitoring | A dashboard view, route, payload field, property, metric name, retention rule, or security boundary changes. | Monitor contract tests, operations-page review, and quality suite. Monitoring remains advisory. |
| Release or support | Contract metadata, maturity, version visibility, compatibility, governance, publication guidance, or a support claim changes. | Release-visibility checks, repository URL gate, governance review, and quality suite. |
| Runnable example | An API, module, dependency, command, expected output, or prerequisite used by an example changes. | Compilation or execution against the documented mode and quality suite. |
| Translation | The final English body, contract metadata, or translation manifest changes. | Canonical-content binding, locale manifest, source-language review, and translation parity. |

## Workflow

1. Change the implementation, test, or authoritative page first; do not state
   behavior that the current tests do not support.
2. Update every page that repeats the changed contract, then link competing
   material to the authoritative route.
3. Apply [editorial standards](documentation-style.md) and release metadata
   from [release visibility](release-visibility.md).
4. Run `bash tools/oss_docs_quality_gate.sh`.
5. For a translation, update it in the same coherent change, recompute the
   English-content digest, and run
   `python3 tools/oss_docs_translation_gate.py`.
6. Record the commands and reported counts in the pull request.

The [documentation audit](documentation-audit.md) records open gaps and
completed evidence. Close an audit row only when its page and checks are green.

## Continuous integration

Every push to the default branch and every pull request runs the complete
documentation quality suite. The suite checks structure, links, code anchors,
language, identity boundaries, example references, translation parity,
repository URL consistency, and contract parity for configuration, errors,
formats, execution, cluster, recovery, resources, Batch, Stream, and
monitoring material. The language check covers the whole repository: the
source-language gate scans every tracked file, and runs in the same suite and
in the CI surface job, so a Chinese comment anywhere fails the build rather
than the review.

Maintainers configure branch protection to require the `Documentation
maintenance` check. A failing or incomplete check blocks merge. Do not bypass
it for wording-only changes: wording can change a support claim, security
boundary, default, or release statement.
