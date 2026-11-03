---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Release and documentation visibility

This page defines which contract a documentation page describes. Use it when
you choose a version to evaluate, report an issue, or publish a release.

## Contract taxonomy

| `doc-contract` | `doc-channel` | Scope |
|---|---|---|
| `preview-0.2.0` | `default-branch` | Current documentation, quickstarts, references, operations, and current runbooks. |
| `rowweave-1.0.0` | `frozen-release` | The immutable release packet, portable integration boundary, rolling upgrade, and store migration procedures. |

`0.2.0` is a source-only public preview. Its API can change before `1.0.0`,
support is evaluation-only, and fixes target the default branch. The current
[capability matrix](capability-matrix.md) remains the support authority.

`rowweave-1.0.0` is the explicit frozen-release exception. It is the immutable
packet documented by the [release contract](versioned-release-v1.md). Do not
mix its signed manifest, portable integration, or upgrade procedures with a
local development build unless the release admission procedure says otherwise.

## Required metadata

Every curated Markdown page starts with front matter that names its contract:

```text
---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
```

A frozen-release page uses:

```text
---
doc-contract: rowweave-1.0.0
doc-channel: frozen-release
---
```

A development-planning page remains bound to the current preview contract and
adds `doc-status: development-planning`. Development planning is not a support claim.
[Roadmap](../../../ROADMAP.md) and the documentation audit use this status;
the capability matrix decides what today's release supports.

## Page routing

The site shows the declared contract on every page. Product quickstarts and
references describe the current preview. The
[external integration guide](../guides/external-platform-integration.md),
[rolling upgrade](../../runbooks/rolling-upgrade-v1.md), and
[store migration](../../runbooks/store-migration.md) pages describe the frozen
release contract. The [Stream runbook](../../runbooks/stream-runtime.md)
describes the current preview runtime.

For evaluation, use the preview contract as one consistent source tree. For a
production integration that consumes a frozen packet, verify the packet before
following its release-contract route. Do not combine expected results from
different contracts.

## Issue and release routing

When reporting behavior, include the contract shown by the page, the source or
artifact identity, and the runtime. Release work must preserve this visibility:
a new page receives preview metadata by default, a frozen-release page requires
an explicit release identity, and development planning never widens the
capability matrix.

## Retained compatibility vocabulary

The public vocabulary names current product concepts. One word is kept on
purpose: `legacy` inside two plan artifact compatibility identifiers.

| Token | Declared at | Read by |
|---|---|---|
| `rowweave.payload.trusted-legacy-plan-graph.v1` | `io/rowweave/artifact/PlanArtifact.java:16-17` | `io/rowweave/artifact/PlanArtifactValidator.java:105` |
| `rowweave.operator-id.legacy-positional.v1` | `io/rowweave/artifact/PlanArtifact.java:18-19` | `io/rowweave/artifact/PlanArtifactValidator.java:101` |

In these names, `legacy` marks a version boundary inside the persisted plan
format. It does not refer to an earlier product or an earlier document set.
Both tokens describe how a payload is encoded and how operator identities are
derived, so a plan written by one runtime version stays readable by a later
one. That makes the spelling part of the durable format, protected in the same
way as the schema version, and a rename is a format decision rather than a
wording cleanup. Changing either token breaks every artifact already written
under the old value, so both are frozen.

The validator treats the two names as known required features, so an artifact
that asks for anything else is refused, and the payload format comparison
selects `TRUSTED_LEGACY_PLAN_GRAPH` only when that feature is present.

`io/rowweave/artifact/LegacyPlanArtifactAdapter.java:15` is the only public
type whose name carries the word. It converts a live Batch plan graph into the
artifact envelope, and it refuses a payload that disagrees with its own
metadata with `LEGACY_DESCRIPTOR_MISMATCH` as documented in
[errors and refusals](errors-and-refusals.md). The validator refuses a
non-portable carrier for public v1 execution with `LEGACY_NOT_PUBLIC_V1`.
Neither class nor code is part of the frozen API surface. Both are
source-visible, listed with the rest of the `io.rowweave.artifact` package in
[the API inventory](api-inventory.md), and a documented refusal code in
[errors and refusals](errors-and-refusals.md).
