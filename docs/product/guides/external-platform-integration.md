---
doc-contract: rowweave-1.0.0
doc-channel: frozen-release
---
# Integrating an external platform through the versioned public API

An external orchestrator integrates through the portable public v1 boundary. It does not construct `PlanGraph`, serialize `Source`/`Sink`/lambda objects, access registry/store internals, or depend on RPC enum ordinals.

## Build and submit

Build a closed `PortableBatchPlan` from supported source, operator and sink bindings, then wrap it with `PortablePlanArtifact.wrap` as a `PlanArtifact` whose payload format is `PORTABLE_BATCH_PLAN_V1`. Provider/function deployment references must pass the signed contract and deployment-set checks before any connector or user class is opened.

```java
PortableBatchPlan plan = buildPortablePlan(); // closed v1 IR, includes rowweave.runtime.batch.v1
PlanArtifact artifact = PortablePlanArtifact.wrap("ledger-close-v1", plan);

RunSubmission request = new RunSubmission(
        runId, attemptId, publicationId,
        3, deadlineEpochMillis, submittedAtMillis);

try (ClusterJobClient client = new ClusterJobClient(masterHost, masterPort)) {
    ExecutionReport accepted = client.submitRun(artifact, request);
    RunSnapshot run = client.queryRun(runId);
    RunExecutionEventPage events = client.queryRunEvents(runId, 0L, 200);
    BatchSinkOutcome sink = client.queryBatchSinkOutcome(runId);
    QualityGateDecision quality = client.queryQualityGateDecision(runId);
}
```

Generate `runId`, `attemptId` and `publicationId` before the first call and persist them on the calling platform side. A transport timeout does not justify new identities: retry the same submission, then query the same Run/publication. The server uses these identities for idempotency, attempt fencing and durable outcome reconciliation.

## Query and recovery

Poll `queryRun(runId)` for the durable Run state. Page `queryRunEvents(runId, afterSequence, limit)` using the returned `nextAfterSequence`; events are diagnostic evidence, while the Run and commit authorities decide truth. Read `queryBatchSinkOutcome(runId)` and `queryQualityGateDecision(runId)` before exposing a result. For bounded CDC, submit the public `IncrementalRunSubmission` and read `queryIncrementalFrontier(feedId)`; do not advance the caller's own cursor independently of the committed frontier.

On timeout, disconnect or Master replacement:

1. reconnect to the elected Master;
2. query the original `runId` and `publicationId`;
3. replay only the exact original request when it is absent or explicitly retryable;
4. preserve the original `attemptId`; replacement and whole-attempt retry are authority decisions;
5. treat a conflicting identity or digest as an integration error, never as a reason to overwrite state.

## Version admission

Pin the exact signed release manifest in each platform deployment. A compatible epoch-1 optional revision may roll only after the N-1/N matrix passes. Unknown required capabilities, legacy Java payloads, unsigned providers, release digest drift and decimal-semantics drift must fail before Run creation. Use the [release contract](../reference/versioned-release-v1.md) and [rolling runbook](../../runbooks/rolling-upgrade-v1.md) as the deployment checklist.

The method signatures, identity rules, and refusal codes above are backed by
`ClusterJobClientDurableRunTest`, `ClusterBatchSinkOutcomeProtocolTest`, and
`ClusterQualityGateDecisionProtocolTest`.
