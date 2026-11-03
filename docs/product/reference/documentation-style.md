---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Documentation style and validation

This page defines the editorial contract for repository documentation. It
applies to canonical product pages, runbooks, contributor instructions, release
notes, issue templates, and code samples. The [capability
matrix](capability-matrix.md) remains the authority for supported behavior.

## Language and voice

* Use English in canonical documentation. Use concise American spelling and
  grammar.
* Use English in comments, javadoc, and shipped resource text inside a source
  tree. A data literal that exists to prove multi-byte or non-ASCII handling is
  not prose and keeps its characters.
* Prefer active voice, present tense, and direct instructions: "Start the
  Worker", not "The Worker should be started".
* Use one first-level title per page. Use sentence case for headings.
* Keep the first paragraph task-oriented. A reader should know what the page
  does and when to use it.
* Use `you` for reader actions and `RowWeave` for the product.
* Avoid superlatives, unstated benchmarks, and claims that are not backed by a
  capability, test, or qualification page.
* Do not use product-generation labels, development-history narrative, internal
  task identifiers, local machine paths, or private credentials.
* Use neutral data vocabulary in examples. Prefer terms such as `category`,
  `quantity`, `metric`, `region`, `customerKey`, and `value` over vocabulary
  tied to one vertical industry.

## Canonical terminology

| Use | Do not use interchangeably | Meaning |
|---|---|---|
| row | record, line, tuple | One value sequence governed by a `DataSchema`. |
| schema | metadata, layout | Named fields, types, and ordering used by rows. |
| job | application, session | One submitted execution graph and its lifecycle. |
| graph | pipeline, flow | Sources, operators, and sinks admitted as one unit. |
| source | input | A supported provider that supplies rows to a graph. |
| sink | output | A supported writer that receives rows at the graph boundary. |
| operator | transformation | An admitted graph node that transforms or routes rows. |
| breaker | blocking operator | An operator with a bounded working set and spill path. |
| attempt | run, execution | One execution instance of a job or assigned stage. |
| checkpoint | snapshot, backup | Durable state used for recovery under a lifecycle contract. |
| savepoint | checkpoint | A controlled stopping point with explicit restore semantics. |

Use exact public type and method names in code style. Use `DataSet` for the
Batch composition API and `DataStream` for Stream composition. Do not invent
synonyms for public API names.

## Page patterns

### Quickstart

A quickstart must contain, in order:

1. A one-paragraph outcome statement.
2. Requirements and prerequisites.
3. Copyable steps.
4. Complete runnable code or a link to a runnable example.
5. The expected result.
6. How to verify the result.
7. A limited next-step route.

Do not combine Batch and Stream quickstarts into one code walkthrough.

### Reference page

A reference page must define scope, authority, syntax or API shape, parameters,
defaults, behavior, failure or refusal behavior, and at least one representative
example when the surface accepts user input. Configuration tables must state
property, type, default, valid values, scope, and observable effect.

### Operations page

An operations page must state prerequisites, explicit commands or API calls,
expected evidence, shutdown or cleanup, and failure actions. Every exposed port,
credential source, certificate, file path, and process role must be explicit.

### Runbook

A runbook must state preconditions, procedure, verification, stop conditions,
and rollback or recovery. It must distinguish operator actions from automated
runtime behavior.

## Code samples

* Tag every fenced block: `java`, `xml`, `bash`, `properties`, `json`, `http`,
  or `text`.
* A sample described as runnable must be complete enough to compile and run or
  must link to a runnable file.
* A fragment must be labeled as a fragment and must not include pretend command
  output.
* Use imports in the first Java sample on a page. Later Java fragments on the
  same page may omit duplicated imports when the page says they continue the
  preceding example.
* Use checked, supported constructors and methods. Do not document reflection,
  runtime internals, or package-private helpers.
* Commands must be copyable, must not depend on local paths, and must show
  required arguments rather than hiding them in an alias.
* Show expected output only when it is deterministic enough to be useful; label
  variable values such as ports, identifiers, and elapsed time.
* Every new example requires compilation. If the page says it runs, run it and
  compare the stated result. If it exercises a public API contract, add or reuse
  a targeted test.

## Support and maturity language

* State behavior only at the level certified by the capability matrix.
* Use "supported", "not supported", "refused", or "deferred" precisely.
* Do not describe roadmap work as available.
* Use `0.2.0` as the initial public preview version when maturity matters.
* Apply the required contract metadata from
  [release visibility](release-visibility.md); do not mix support claims with
  roadmap or frozen-release routes.
* A page may state a limitation more narrowly than the matrix, but never more
  broadly.
* Use "stable" only for explicitly frozen contracts, identifiers, or refusal
  behavior.

## Warnings and failure guidance

* Use plain sentences rather than decoration. Start a warning with `Important:`
  when ignoring it can lose data, weaken security, or exhaust resources.
* Name the condition, consequence, and safe action.
* Do not tell users to disable admission, authentication, resource limits, or
  validation as a workaround.
* For a typed failure, tell the reader where to find the code, what it means,
  and what to change.

## Links and accessibility

* Link each page type to one authoritative route. Avoid competing copies of a
  contract.
* Check every relative link and anchor before commit.
* Provide a text alternative for every screenshot or diagram.
* Do not use color alone to distinguish job or node state.
* Keep tables narrow enough to scan; move long guidance to prose.
* Use descriptive link labels rather than "here".

## Accessibility and density review

Review at least one quickstart, one reference page, one architecture page, and
one operations page for every documentation release. Keep the review
evidence-based:

* Use one first-level title, and heading order does not skip a heading level.
* Keep the page language, main landmark, section navigation, and skip link
  intact.
* Use descriptive link text, and resolve every destination and heading anchor.
* Give every table column header `scope="col"`.
* Make horizontally scrollable tables and code blocks keyboard reachable with
  `tabindex="0"`.
* Provide a text alternative for every image or diagram; never replace the
  operative steps with an image.
* Make status meaning explicit in words so it is not identified by color alone.
* Preserve a visible focus indicator and respect reduced-motion preference.
* Keep introductory and explanatory paragraphs task-oriented and no more than 75 words.
* Prefer prose for decisions and consequences; use tables only for parallel facts.

## Required validation

Change triggers, page ownership, and pull-request evidence are defined in
[documentation maintenance](documentation-maintenance.md). Run the complete
documentation suite from the repository root before commit:

```bash
bash tools/oss_docs_quality_gate.sh
```

Then run the applicable targeted checks:

| Change | Required evidence |
|---|---|
| Documentation only | Language, identity, content boundary, translation parity, relative-link, and anchor checks. |
| Translation | Locale manifest and canonical-content revision checks plus source-language semantic review. |
| Configuration documentation | Property-to-implementation parity check and targeted configuration test when practical. |
| Public API example | Approved vocabulary or signature check plus compilation. |
| Runnable example | Execution against the documented mode and comparison with the stated result. |
| Monitor documentation | Monitor contract tests and dashboard/API verification when behavior or routes change. |
| Release or governance page | Repository URL, license, notice, identity, and publication-metadata checks. |

The documentation audit is the maintainer-facing backlog. Update it when a page
moves, a contract changes, or a documented gap is closed.
