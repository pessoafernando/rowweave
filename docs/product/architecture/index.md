---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Architecture reading map

This page is the routing layer for architecture questions. Each row names one
authoritative entry point; that page owns the detailed contract and its tested
evidence. Read the [capability matrix](../reference/capability-matrix.md)
first when the question is whether a behavior is supported rather than how the
runtime works.

## Route by question

| Question | Authoritative entry | Boundary answered there |
|---|---|---|
| Why are results deterministic? | [Pure push execution model](pure-push-model.md#deterministic-semantics) | Whole-graph execution, push stages, shared subtrees, ordering boundaries, decimal and null semantics. |
| Why does memory remain bounded? | [Resource and backpressure](resources.md#batch-managed-memory-and-spill) | Process envelopes, Worker slots, managed-memory owners, spill quotas, channels, credit, and task leases. |
| How are roles placed and scheduled? | [Cluster topology and scheduling](cluster.md#compilation-capacity-and-placement) | Client, Master, Worker, registry, artifacts, placement, assignment, transport, and failure isolation. |
| How does recovery find durable state? | [Recovery and state authority](recovery.md#authority-map) | Run attempts, fences, retry, output manifests, checkpoints, savepoints, CDC frontier, and store migration. |
| How does Stream use one lifecycle? | [Stream behavior](../reference/stream-behavior.md) | One portable DAG, one task factory, input gates, output routing, state/time, checkpoint, restore, and rescale contracts. |
| Where is extension allowed? | [Capability matrix](../reference/capability-matrix.md#capability-boundaries) | Supported public extension surfaces, stable refusals, and controls that are not part of the release contract. |

## Invariants behind every route

A job is one submitted graph, never a sequence of sink-specific programs.
Pass-through execution pushes row by row; only bounded, admitted working sets
materialize state, and those sets have spill paths. Placement, memory, threads,
descriptors, channels, and disk are admitted before execution.

Durable control decisions are identity-bound and fenced. Monitoring is advisory
and read-only. A mode, operator count, or topology shape never converts an
unsupported plan into a supported one; capability and resource admission remain
the authority.

Use [data-plane limits](../reference/data-plane-limits.md) for the certified
socket boundary and [monitoring](../operations/monitoring.md) for operational
evidence. These are companion views of the same runtime, not alternative
semantic authorities.
