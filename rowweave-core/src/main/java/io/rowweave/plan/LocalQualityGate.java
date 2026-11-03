package io.rowweave.plan;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.ManifestFileSink;
import io.rowweave.QualityEvidenceReadGuard;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGatePolicy;
import io.rowweave.QualityPreparedEvidence;
import io.rowweave.QualityPreparedEvidenceReader;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageTerminalReceipt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Ephemeral/local use of the same sealed-member reader and exact evaluator as durable R3. */
public final class LocalQualityGate {
    private static final long READ_TIMEOUT_NANOS = TimeUnit.HOURS.toNanos(1L);

    private LocalQualityGate() {
    }

    static void enforce(
            PlanGraph graph, ManifestFileSink[] outputs,
            String publicationId, long dataGeneration) {
        List<CommitManifest.Member> members = new ArrayList<>(outputs.length);
        for (ManifestFileSink output : outputs) {
            members.add(output.installedMember());
        }
        enforce(graph, members, publicationId, dataGeneration,
                System.nanoTime() + READ_TIMEOUT_NANOS);
    }

    /**
     * Applies the local/ephemeral quality gate to immutable members installed by
     * Worker stages. The caller still owns rollback until this method returns.
     */
    public static void enforce(
            PlanGraph graph, List<CommitManifest.Member> installedMembers,
            String publicationId, long dataGeneration, long deadlineNanos) {
        ManifestFileSink[] outputs = qualityOutputs(graph);
        if (!hasQualityBinding(outputs)) return;
        TypedQualityPublicationPlanValidator.validateFullGraph(graph);
        if (outputs.length != 3) {
            throw new IllegalStateException(
                    "QUALITY_PUBLICATION_INVALID: local quality gate requires three members");
        }

        java.util.Map<String, CommitManifest.Member> membersByName =
                new java.util.LinkedHashMap<>();
        for (CommitManifest.Member member : installedMembers) {
            if (membersByName.put(member.name(), member) != null) {
                throw new IllegalStateException(
                        "QUALITY_PUBLICATION_INVALID: duplicate installed quality member");
            }
        }
        if (membersByName.size() != outputs.length) {
            throw new IllegalStateException(
                    "QUALITY_PUBLICATION_INVALID: installed quality members are incomplete");
        }

        RunAttemptFence fence = new RunAttemptFence(
                "local-quality-" + publicationId,
                "local-attempt-" + publicationId, 1L);
        List<StageAssignmentRecord> records = new ArrayList<>(outputs.length);
        for (int i = 0; i < outputs.length; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, i + 1L, i, 0,
                    StageAssignmentRole.SINK, "local-worker",
                    "127.0.0.1", 1, -1, "local-incarnation"));
        }
        QualityGatePolicy policy = outputs[0].qualityGatePolicy();
        if (policy == null) {
            throw new IllegalStateException(
                    "QUALITY_PUBLICATION_INVALID: incomplete local quality binding");
        }
        List<QualityGateAssignmentBinding.MemberBinding> memberBindings =
                new ArrayList<>(outputs.length);
        for (int i = 0; i < outputs.length; i++) {
            memberBindings.add(new QualityGateAssignmentBinding.MemberBinding(
                    i, outputs[i].qualityGateRole(),
                    BatchSinkDescriptorCodec.digest(
                            outputs[i].batchSinkDescriptor())));
        }
        QualityGateAssignmentBinding qualityBinding =
                new QualityGateAssignmentBinding(
                        outputs[0].qualityGatePolicyDigest(), memberBindings);
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "local-artifact-" + publicationId, 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                records, qualityBinding);
        StageAssignmentSnapshot assignments = StageAssignmentSnapshot.initial(manifest);
        List<byte[]> commands = new ArrayList<>(outputs.length);
        for (int i = 0; i < outputs.length; i++) {
            byte[] command = BatchSinkDescriptorCodec.digest(
                    outputs[i].batchSinkDescriptor());
            commands.add(command);
            StageAssignmentToken token = assignments.token(i);
            assignments = assignments.withAcceptance(
                    new StageAssignmentAcceptance(token, command, 0L));
            assignments = assignments.withTerminal(StageTerminalReceipt.succeeded(
                    token, command, 0L, 0L, 0L));
        }

        ManifestCommitJournalSnapshot journal =
                ManifestCommitJournalSnapshot.initial(assignments);
        for (int i = 0; i < outputs.length; i++) {
            ManifestFileSink output = outputs[i];
            QualityGatePolicy candidate = output.qualityGatePolicy();
            if (candidate == null) {
                throw new IllegalStateException(
                        "QUALITY_PUBLICATION_INVALID: incomplete local quality binding");
            }
            CommitManifest.Member member = membersByName.get(output.memberName());
            if (member == null) {
                throw new IllegalStateException(
                        "QUALITY_PUBLICATION_INVALID: installed quality member is missing: "
                                + output.memberName());
            }
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, records.get(i), output.qualityGateRole(),
                    output.batchSinkDescriptor());
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    output.domainId(), ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), 0L);
            journal = journal.withMember(ManifestMemberReceipt.sealed(
                    assignments.token(i), commands.get(i), output.domainId(),
                    dataGeneration, publicationId, member, 0L, locator));
        }

        QualityEvidenceReadGuard guard = QualityEvidenceReadGuard.until(
                deadlineNanos, () -> false);
        QualityPreparedEvidence evidence = QualityPreparedEvidenceReader.read(
                graph, journal, guard);
        QualityGateDecision decision = QualityGateDecision.evaluate(
                manifest.artifactId(), fence, publicationId, policy, evidence);
        if (decision.outcome() == QualityGateDecision.Outcome.FAIL) {
            throw new IllegalStateException("QUALITY_GATE_REJECTED");
        }
    }

    private static ManifestFileSink[] qualityOutputs(PlanGraph graph) {
        List<ManifestFileSink> outputs = new ArrayList<>();
        for (PlanNode node : graph.nodes()) {
            if (node.kind() == PlanNode.Kind.SINK
                    && node.output() instanceof ManifestFileSink
                    && ((ManifestFileSink) node.output()).hasQualityGateBinding()) {
                outputs.add((ManifestFileSink) node.output());
            }
        }
        return outputs.toArray(new ManifestFileSink[0]);
    }

    private static boolean hasQualityBinding(ManifestFileSink[] outputs) {
        boolean any = false;
        for (ManifestFileSink output : outputs) {
            if (output.hasQualityGateBinding()) any = true;
        }
        return any;
    }
}
