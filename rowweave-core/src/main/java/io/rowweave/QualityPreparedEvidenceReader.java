package io.rowweave;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.TypedQualityPublicationPlanValidator;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkLocatorCodec;
import io.rowweave.run.QualityGateCommitJournalSnapshot;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.sched.stream.SourceRecordPosition;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

/** bounded reader for sealed immutable quality evidence. */
public final class QualityPreparedEvidenceReader {
    private static final int MAX_ENCODED_ROW_BYTES = 4 * 1024;
    private static final String MANIFEST_FILE_PREFIX = "manifest-file:";
    private static final String VALID = TypedQualityManifest.VALID_SINK_UID;
    private static final String REJECTED = TypedQualityManifest.REJECTED_SINK_UID;
    private static final String SUMMARY = TypedQualityManifest.SUMMARY_SINK_UID;

    private QualityPreparedEvidenceReader() {
    }

    public static QualityPreparedEvidence read(
            PlanGraph graph, ManifestCommitJournalSnapshot journal,
            QualityEvidenceReadGuard guard) {
        if (guard == null) {
            throw new IllegalArgumentException("quality evidence read guard is required");
        }
        try {
            guard.check();
            QualityPreparedEvidence evidence = readChecked(graph, journal, guard);
            guard.check();
            return evidence;
        } catch (QualityEvidenceReadGuard.Terminated terminated) {
            throw terminated;
        } catch (IllegalStateException failure) {
            if (failure.getMessage() != null
                    && failure.getMessage().startsWith("QUALITY_EVIDENCE_INVALID:")) {
                throw failure;
            }
            throw invalid("prepared evidence is invalid", failure);
        } catch (RuntimeException failure) {
            throw invalid("prepared evidence is invalid", failure);
        } catch (IOException failure) {
            guard.check();
            throw invalid("prepared evidence is invalid", failure);
        }
    }

    public static QualityPreparedEvidence read(
            PlanGraph graph, QualityGateCommitJournalSnapshot journal,
            QualityEvidenceReadGuard guard) {
        if (guard == null) {
            throw new IllegalArgumentException("quality evidence read guard is required");
        }
        try {
            guard.check();
            QualityPreparedEvidence evidence = readChecked(graph, journal, guard);
            guard.check();
            return evidence;
        } catch (QualityEvidenceReadGuard.Terminated terminated) {
            throw terminated;
        } catch (IllegalStateException failure) {
            if (failure.getMessage() != null
                    && failure.getMessage().startsWith("QUALITY_EVIDENCE_INVALID:")) {
                throw failure;
            }
            throw invalid("prepared evidence is invalid", failure);
        } catch (RuntimeException failure) {
            throw invalid("prepared evidence is invalid", failure);
        } catch (IOException failure) {
            guard.check();
            throw invalid("prepared evidence is invalid", failure);
        }
    }

    private static QualityPreparedEvidence readChecked(
            PlanGraph graph, ManifestCommitJournalSnapshot journal,
            QualityEvidenceReadGuard guard) throws IOException {
        guard.check();
        TypedQualityPublicationPlanValidator.validateFullGraph(graph);
        if (journal == null || !journal.allExpectedMembersSealed()
                || journal.expectedMemberCount() != 3 || journal.memberCount() != 3) {
            throw invalid("exactly three sealed member receipts are required");
        }
        return readChecked(graph, receipts(journal), guard);
    }

    private static QualityPreparedEvidence readChecked(
            PlanGraph graph, QualityGateCommitJournalSnapshot journal,
            QualityEvidenceReadGuard guard) throws IOException {
        guard.check();
        TypedQualityPublicationPlanValidator.validateFullGraph(graph);
        if (journal == null || !journal.allMembersSealed()) {
            throw invalid("exactly three sealed member receipts are required");
        }
        return readChecked(graph, receipts(journal.members()), guard);
    }

    private static QualityPreparedEvidence readChecked(
            PlanGraph graph, TreeMap<String, ManifestMemberReceipt> receipts,
            QualityEvidenceReadGuard guard) throws IOException {
        Map<String, ManifestFileSink> outputs = outputs(graph);
        QualityGatePolicy policy = outputs.get(VALID).qualityGatePolicy();
        DiagnosticProof diagnosticProof = new DiagnosticProof(graph, guard);
        SourceProof sourceProof = new SourceProof(graph, guard);
        List<QualityPreparedMemberEvidence> members = new ArrayList<>(3);
        for (String role : roles()) {
            ManifestFileSink output = outputs.get(role);
            ManifestMemberReceipt receipt = receipts.get(role);
            verifyBinding(role, output, receipt);
            members.add(new QualityPreparedMemberEvidence(role,
                    receipt.member().name(), receipt.member().length(),
                    receipt.member().sha256(), sha256(
                    PreparedSinkLocatorCodec.encode(receipt.preparedLocator()))));
        }

        ManifestMemberReceipt summaryReceipt = receipts.get(SUMMARY);
        SummaryCounts counts = readSummary(outputs.get(SUMMARY),
                summaryReceipt, policy, diagnosticProof, guard);
        List<QualityExemplar> exemplars = readRejected(outputs.get(REJECTED),
                receipts.get(REJECTED), counts.rejectedTotal,
                policy.exemplarLimit(), diagnosticProof, sourceProof, guard);
        return new QualityPreparedEvidence(counts.acceptedTotal,
                counts.rejectedTotal, counts.observations, exemplars, members);
    }

    private static Map<String, ManifestFileSink> outputs(PlanGraph graph) {
        TreeMap<String, ManifestFileSink> outputs = new TreeMap<>();
        for (PlanNode node : graph.nodes()) {
            if (node.kind() != PlanNode.Kind.SINK) continue;
            if (node.output() == null
                    || node.output().getClass() != ManifestFileSink.class) {
                throw invalid("quality sink output is noncanonical");
            }
            ManifestFileSink output = (ManifestFileSink) node.output();
            String role = output.qualityGateRole();
            if (!isRole(role) || outputs.put(role, output) != null) {
                throw invalid("quality sink roles are incomplete or duplicated");
            }
        }
        if (outputs.size() != 3 || !outputs.keySet().containsAll(roles())) {
            throw invalid("exactly three quality sink roles are required");
        }
        return outputs;
    }

    private static TreeMap<String, ManifestMemberReceipt> receipts(
            ManifestCommitJournalSnapshot journal) {
        List<ManifestMemberReceipt> selected = new ArrayList<>();
        TreeMap<String, ManifestMemberReceipt> receipts = new TreeMap<>();
        int count = journal.assignments().manifest().assignmentCount();
        for (int i = 0; i < count; i++) {
            ManifestMemberReceipt receipt = journal.member(i);
            if (receipt != null) selected.add(receipt);
        }
        return receipts(selected);
    }

    private static TreeMap<String, ManifestMemberReceipt> receipts(
            List<ManifestMemberReceipt> selected) {
        TreeMap<String, ManifestMemberReceipt> receipts = new TreeMap<>();
        for (ManifestMemberReceipt receipt : selected) {
            if (receipt == null) continue;
            if (!receipt.hasPreparedLocator()) {
                throw invalid("sealed member has no prepared locator");
            }
            String role = receipt.preparedLocator().attemptIdentity().sinkOperatorId();
            if (!isRole(role) || receipts.put(role, receipt) != null) {
                throw invalid("prepared locator roles are incomplete or duplicated");
            }
        }
        if (receipts.size() != 3 || !receipts.keySet().containsAll(roles())) {
            throw invalid("exactly three prepared locator roles are required");
        }
        return receipts;
    }

    private static void verifyBinding(String role, ManifestFileSink output,
                                      ManifestMemberReceipt receipt) {
        if (receipt == null) throw invalid("quality member receipt is missing");
        CommitManifest.Member member = receipt.member();
        PreparedSinkLocator locator = receipt.preparedLocator();
        SinkAttemptIdentity identity = locator.attemptIdentity();
        String expectedPath = "data/" + receipt.generation() + "/"
                + output.memberName() + ".data";
        byte[] descriptorDigest = BatchSinkDescriptorCodec.digest(
                output.batchSinkDescriptor());
        if (!role.equals(output.qualityGateRole())
                || !role.equals(identity.sinkOperatorId())
                || !output.domainId().equals(receipt.domainId())
                || !output.domainId().equals(locator.commitDomainId())
                || !output.memberName().equals(member.name())
                || !expectedPath.equals(member.relativeDataPath())
                || !SchemaFingerprint.of(output.getSchema()).equals(
                member.schemaFingerprint())
                || locator.capabilityProfile()
                != BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1
                || !MessageDigest.isEqual(descriptorDigest,
                identity.descriptorDigest())
                || locator.dataLength() != member.length()
                || !MessageDigest.isEqual(locator.dataHash(), member.sha256())) {
            throw invalid("prepared member does not match its Plan binding");
        }
    }

    private static SummaryCounts readSummary(
            ManifestFileSink output, ManifestMemberReceipt receipt,
            QualityGatePolicy policy,
            DiagnosticProof diagnosticProof,
            QualityEvidenceReadGuard guard) throws IOException {
        if (!SchemaFingerprint.of(TypedQualityManifest.summarySchema()).equals(
                receipt.member().schemaFingerprint())) {
            throw invalid("summary member schema is noncanonical");
        }
        TreeMap<QualityReasonKey, Long> observations = new TreeMap<>();
        for (QualityGatePolicy.ReasonThreshold threshold : policy.reasonThresholds()) {
            observations.put(threshold.reason(), 0L);
        }
        long accepted = -1L;
        long rejected = -1L;
        long reasonSum = 0L;
        QualityReasonKey previous = null;
        int phase = 0;
        try (VerifiedLineReader lines = open(output, receipt, guard)) {
            String line;
            while ((line = lines.nextLine()) != null) {
                DataRow row = decode(TypedQualityManifest.summarySchema(), line, guard);
                requireSummaryShape(row);
                String kind = (String) row.get(0);
                long count = (Long) row.get(5);
                if (count < 0L) throw invalid("summary count is negative");
                if ("ACCEPTED_TOTAL".equals(kind)) {
                    if (phase != 0 || accepted >= 0L || !emptyReason(row)) {
                        throw invalid("accepted total is missing, repeated, or out of order");
                    }
                    accepted = count;
                    phase = 1;
                } else if ("REJECTED_REASON".equals(kind)) {
                    if (phase != 1) {
                        throw invalid("rejected reason is out of canonical order");
                    }
                    if (count == 0L) {
                        throw invalid("rejected reason count must be positive");
                    }
                    QualityReasonKey reason = reason(row);
                    diagnosticProof.requireAdmitted(reason);
                    if (previous != null && previous.compareTo(reason) >= 0) {
                        throw invalid("rejected reasons are duplicated or unordered");
                    }
                    previous = reason;
                    reasonSum = Math.addExact(reasonSum, count);
                    if (observations.containsKey(reason)) {
                        observations.put(reason, count);
                    }
                } else if ("REJECTED_TOTAL".equals(kind)) {
                    if (phase != 1 || rejected >= 0L || !emptyReason(row)) {
                        throw invalid("rejected total is missing, repeated, or out of order");
                    }
                    rejected = count;
                    phase = 2;
                } else {
                    throw invalid("summary row kind is unknown");
                }
            }
            lines.verifyComplete();
        } catch (ArithmeticException overflow) {
            throw invalid("summary count overflows", overflow);
        }
        if (phase != 2 || accepted < 0L || rejected < 0L
                || reasonSum != rejected) {
            throw invalid("summary totals and reason counts are inconsistent");
        }
        return new SummaryCounts(accepted, rejected, observations);
    }

    private static List<QualityExemplar> readRejected(
            ManifestFileSink output, ManifestMemberReceipt receipt,
            long expectedRows, int limit,
            DiagnosticProof diagnosticProof, SourceProof sourceProof,
            QualityEvidenceReadGuard guard) throws IOException {
        if (!SchemaFingerprint.of(rejectedMeta()).equals(
                receipt.member().schemaFingerprint())) {
            throw invalid("rejected member schema is noncanonical");
        }
        PriorityQueue<QualityExemplar> heap = new PriorityQueue<>(
                Math.max(1, limit), Collections.reverseOrder());
        Set<QualityExemplar> retained = new HashSet<>(Math.max(1, limit * 2));
        long actualRows = 0L;
        try (VerifiedLineReader lines = open(output, receipt, guard)) {
            String line;
            while ((line = lines.nextLine()) != null) {
                DataRow row = decode(rejectedMeta(), line, guard);
                QualityReasonKey reason = requireRejectedShape(row, sourceProof);
                diagnosticProof.requireAdmitted(reason);
                actualRows = Math.addExact(actualRows, 1L);
                if (limit == 0) continue;
                QualityExemplar candidate = QualityExemplar.of(
                        (String) row.get(0), (String) row.get(1),
                        (Long) row.get(2), (String) row.get(3));
                if (retained.contains(candidate)) continue;
                if (heap.size() < limit) {
                    heap.add(candidate);
                    retained.add(candidate);
                } else if (candidate.compareTo(heap.peek()) < 0) {
                    retained.remove(heap.remove());
                    heap.add(candidate);
                    retained.add(candidate);
                }
            }
            lines.verifyComplete();
        } catch (ArithmeticException overflow) {
            throw invalid("rejected row count overflows", overflow);
        }
        if (actualRows != expectedRows) {
            throw invalid("rejected row count differs from summary");
        }
        List<QualityExemplar> result = new ArrayList<>(heap);
        Collections.sort(result);
        return result;
    }

    private static VerifiedLineReader open(
            ManifestFileSink output, ManifestMemberReceipt receipt,
            QualityEvidenceReadGuard guard) throws IOException {
        guard.check();
        String domainId = output.domainId();
        if (!domainId.startsWith(MANIFEST_FILE_PREFIX)) {
            throw invalid("prepared member has a non-file domain identity");
        }
        Path domain = Paths.get(domainId.substring(MANIFEST_FILE_PREFIX.length()));
        QualityEvidencePathAccess.OpenedMember opened =
                QualityEvidencePathAccess.open(domain, receipt.generation(),
                        output.memberName(), receipt.member().length(), guard);
        return new VerifiedLineReader(opened, guard,
                receipt.member().length(), receipt.member().sha256());
    }

    private static DataRow decode(
            DataSchema meta, String line, QualityEvidenceReadGuard guard) {
        guard.check();
        try {
            DataRow row = FileSink.decodeRow(meta, line);
            if (!line.equals(FileSink.encodeRowText(meta, row))) {
                throw invalid("quality row text is noncanonical");
            }
            guard.check();
            return row;
        } catch (QualityEvidenceReadGuard.Terminated terminated) {
            throw terminated;
        } catch (RuntimeException failure) {
            throw invalid("quality row encoding is invalid", failure);
        }
    }

    private static void requireSummaryShape(DataRow row) {
        if (row == null || row.size() != 6
                || row.get(0) == null || row.get(0).getClass() != String.class
                || row.get(1) == null || row.get(1).getClass() != String.class
                || row.get(2) == null || row.get(2).getClass() != String.class
                || row.get(3) == null || row.get(3).getClass() != Integer.class
                || row.get(4) == null || row.get(4).getClass() != String.class
                || row.get(5) == null || row.get(5).getClass() != Long.class) {
            throw invalid("summary row shape is invalid");
        }
    }

    private static boolean emptyReason(DataRow row) {
        return "".equals(row.get(1)) && "".equals(row.get(2))
                && Integer.valueOf(-1).equals(row.get(3))
                && "".equals(row.get(4));
    }

    private static QualityReasonKey reason(DataRow row) {
        try {
            return QualityReasonKey.of((String) row.get(1), (String) row.get(2),
                    (Integer) row.get(3), (String) row.get(4));
        } catch (RuntimeException failure) {
            throw invalid("summary reason key is noncanonical", failure);
        }
    }

    private static QualityReasonKey requireRejectedShape(
            DataRow row, SourceProof sourceProof) {
        if (row == null || row.size() != 7
                || row.get(0) == null || row.get(0).getClass() != String.class
                || row.get(1) == null || row.get(1).getClass() != String.class
                || row.get(2) == null || row.get(2).getClass() != Long.class
                || (Long) row.get(2) < 0L
                || row.get(3) == null || row.get(3).getClass() != String.class
                || row.get(4) == null || row.get(4).getClass() != String.class
                || row.get(5) == null || row.get(5).getClass() != Integer.class
                || row.get(6) == null || row.get(6).getClass() != String.class) {
            throw invalid("rejected row shape is invalid");
        }
        try {
            QualityExemplar.of((String) row.get(0), (String) row.get(1),
                    (Long) row.get(2), (String) row.get(3));
            sourceProof.requireAdmitted((String) row.get(0), (String) row.get(1));
            return QualityReasonKey.of((String) row.get(3), (String) row.get(4),
                    (Integer) row.get(5), (String) row.get(6));
        } catch (RuntimeException failure) {
            throw invalid("rejected row metadata is noncanonical", failure);
        }
    }

    private static DataSchema rejectedMeta() {
        return new DataSchema(
                new Field("source_operator_uid", DataType.StringType),
                new Field("split_identity_sha256", DataType.StringType),
                new Field("split_local_ordinal", DataType.LongType),
                new Field("quality_error_code", DataType.StringType),
                new Field("quality_rule_id", DataType.StringType),
                new Field("quality_field_index", DataType.IntegerType),
                new Field("quality_detail", DataType.StringType));
    }

    private static boolean isRole(String role) {
        return VALID.equals(role) || REJECTED.equals(role) || SUMMARY.equals(role);
    }

    private static List<String> roles() {
        List<String> roles = new ArrayList<>(3);
        roles.add(VALID);
        roles.add(REJECTED);
        roles.add(SUMMARY);
        return roles;
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static IllegalStateException invalid(String detail) {
        return new IllegalStateException("QUALITY_EVIDENCE_INVALID: " + detail);
    }

    private static IllegalStateException invalid(String detail, Throwable cause) {
        return new IllegalStateException(
                "QUALITY_EVIDENCE_INVALID: " + detail, cause);
    }

    /** Frozen bounded split membership derived only from immutable Plan descriptors. */
    private static final class SourceProof {
        private final String sourceUid;
        private final Set<String> splitDigests;

        private SourceProof(PlanGraph graph, QualityEvidenceReadGuard guard) {
            guard.check();
            PlanNode found = null;
            boolean object = false;
            for (PlanNode node : graph.nodes()) {
                if ("objectQualityRoute".equals(node.typeId())) object = true;
                if (node.kind() != PlanNode.Kind.SOURCE) continue;
                if (found != null) {
                    throw invalid("quality Plan must have one canonical Source");
                }
                found = node;
            }
            if (found == null) {
                throw invalid("quality Plan has no canonical Source");
            }
            sourceUid = found.param(PlanNode.OPERATOR_UID_PARAM);
            if (sourceUid == null || sourceUid.isEmpty()) {
                throw invalid("quality Source UID is missing");
            }
            if (found.sourceSplitDescriptors().isEmpty()) {
                throw invalid("quality Source has no exact split descriptor");
            }
            if (object) {
                for (Source input : found.inputs()) {
                    guard.check();
                    if (input == null || input.getClass() != ObjectStorageQualitySource.class
                            || !sourceUid.equals(
                            ((ObjectStorageQualitySource) input).sourceOperatorUid())) {
                        throw invalid("Object quality Source wrapper is noncanonical");
                    }
                }
            }
            Set<String> admitted = new HashSet<>(
                    found.sourceSplitDescriptors().size() * 2);
            for (int i = 0; i < found.sourceSplitDescriptors().size(); i++) {
                guard.check();
                BatchSourceSplitDescriptor descriptor = object
                        ? ((ObjectStorageQualitySource) found.inputs().get(i))
                        .originalSource().batchSourceSplit()
                        : found.sourceSplitDescriptors().get(i);
                admitted.add(SourceRecordPosition.forSplit(sourceUid, descriptor)
                        .at(0L).splitIdentitySha256());
            }
            splitDigests = Collections.unmodifiableSet(admitted);
            guard.check();
        }

        private void requireAdmitted(String uid, String splitDigest) {
            if (!sourceUid.equals(uid)) {
                throw invalid("rejected row Source UID differs from the Plan");
            }
            if (!splitDigests.contains(splitDigest)) {
                throw invalid("rejected row split differs from the Plan");
            }
        }
    }

    /** Frozen bounded membership table derived only from validated Plan metadata. */
    private static final class DiagnosticProof {
        private final Set<QualityReasonKey> admittedReasons;

        private DiagnosticProof(PlanGraph graph, QualityEvidenceReadGuard guard) {
            guard.check();
            PlanNode classifier = null;
            boolean object = false;
            for (PlanNode node : graph.nodes()) {
                if ("objectQualityRoute".equals(node.typeId())) object = true;
                if ("rowClassify".equals(node.typeId())) classifier = node;
            }
            Set<QualityReasonKey> admitted = new HashSet<>();
            if (object) {
                admitted.add(QualityReasonKey.of(
                        "QUALITY_RAW_UTF8_INVALID", "OBJECT_ROW_DECODE_V1", -1, "UTF8"));
                admitted.add(QualityReasonKey.of(
                        "QUALITY_RAW_CR_INVALID", "OBJECT_ROW_DECODE_V1", -1, "CR"));
                admitted.add(QualityReasonKey.of(
                        "QUALITY_RAW_ROW_LIMIT_EXCEEDED", "OBJECT_ROW_DECODE_V1", -1,
                        "ROW_BYTES"));
                admitted.add(QualityReasonKey.of(
                        "QUALITY_RAW_FORMAT_INVALID", "OBJECT_ROW_DECODE_V1", -1,
                        "ROWWEAVE_ROW_TEXT_V1"));
                admittedReasons = Collections.unmodifiableSet(admitted);
                guard.check();
                return;
            }
            if (classifier == null || classifier.childCount() != 1) {
                throw invalid("quality classifier is missing");
            }
            DataSchema inputMeta = graph.node(classifier.child(0)).outMeta();
            admitted.add(QualityReasonKey.of(
                    "QUALITY_ROW_WIDTH_MISMATCH", "", -1, "ROW_WIDTH"));
            for (int i = 0; i < inputMeta.getFieldCount(); i++) {
                guard.check();
                if (inputMeta.getField(i).getDataType() == DataType.NullType) {
                    throw invalid("quality classifier input contains NullType");
                }
                admitted.add(QualityReasonKey.of(
                        "QUALITY_VALUE_TYPE_MISMATCH", "", i,
                        inputMeta.getField(i).getDataType().name()));
            }
            List<String> ruleIds = classifier.listParam("ruleIds");
            List<String> ruleKinds = classifier.listParam("ruleKinds");
            List<String> fieldIndices = classifier.listParam("fieldIndices");
            if (ruleIds.size() != ruleKinds.size()
                    || ruleIds.size() != fieldIndices.size()) {
                throw invalid("quality classifier rule descriptors are inconsistent");
            }
            for (int i = 0; i < ruleIds.size(); i++) {
                guard.check();
                try {
                    int field = Integer.parseInt(fieldIndices.get(i));
                    if (!Integer.toString(field).equals(fieldIndices.get(i))) {
                        throw new IllegalArgumentException("noncanonical field index");
                    }
                    RowValidation.Kind.valueOf(ruleKinds.get(i));
                    admitted.add(QualityReasonKey.of(
                            "QUALITY_RULE_VIOLATION", ruleIds.get(i), field,
                            ruleKinds.get(i)));
                } catch (RuntimeException malformed) {
                    throw invalid("quality classifier rule descriptor is invalid", malformed);
                }
            }
            admittedReasons = Collections.unmodifiableSet(admitted);
            guard.check();
        }

        private void requireAdmitted(QualityReasonKey reason) {
            if (!admittedReasons.contains(reason)) {
                throw invalid("quality diagnostic tuple is outside the Plan closed set");
            }
        }
    }

    private static final class SummaryCounts {
        private final long acceptedTotal;
        private final long rejectedTotal;
        private final Map<QualityReasonKey, Long> observations;

        private SummaryCounts(long acceptedTotal, long rejectedTotal,
                              Map<QualityReasonKey, Long> observations) {
            this.acceptedTotal = acceptedTotal;
            this.rejectedTotal = rejectedTotal;
            this.observations = new LinkedHashMap<>(observations);
        }
    }

    /** Strict LF-delimited reader that verifies the exact opened bytes at EOS. */
    private static final class VerifiedLineReader implements AutoCloseable {
        private final InputStream input;
        private final QualityEvidencePathAccess.OpenedMember opened;
        private final QualityEvidenceReadGuard guard;
        private final long expectedLength;
        private final byte[] expectedHash;
        private final MessageDigest digest;
        private final ByteArrayOutputStream line =
                new ByteArrayOutputStream(MAX_ENCODED_ROW_BYTES);
        private long bytesRead;
        private boolean eof;

        private VerifiedLineReader(QualityEvidencePathAccess.OpenedMember opened,
                                   QualityEvidenceReadGuard guard,
                                   long expectedLength, byte[] expectedHash) {
            this.opened = opened;
            this.guard = guard;
            this.input = new BufferedInputStream(
                    Channels.newInputStream(opened.channel()));
            this.expectedLength = expectedLength;
            this.expectedHash = expectedHash.clone();
            this.digest = digest();
        }

        private String nextLine() throws IOException {
            guard.check();
            if (eof) return null;
            line.reset();
            while (true) {
                guard.check();
                int value = input.read();
                guard.check();
                if (value < 0) {
                    eof = true;
                    if (line.size() != 0) {
                        throw invalid("quality member has an unterminated row");
                    }
                    return null;
                }
                bytesRead = Math.addExact(bytesRead, 1L);
                if (bytesRead > expectedLength) {
                    throw invalid("quality member exceeds its sealed length");
                }
                digest.update((byte) value);
                if (value == '\n') return strictUtf8(line.toByteArray());
                if (value == '\r') {
                    throw invalid("quality member contains a raw CR");
                }
                if (line.size() >= MAX_ENCODED_ROW_BYTES) {
                    throw invalid("quality member row exceeds its byte bound");
                }
                line.write(value);
            }
        }

        private void verifyComplete() {
            guard.check();
            if (!eof || bytesRead != expectedLength
                    || !MessageDigest.isEqual(digest.digest(), expectedHash)) {
                throw invalid("quality member length or SHA-256 differs at EOS");
            }
            try {
                opened.verifyUnchanged();
            } catch (IOException failure) {
                throw invalid("quality member path changed during read", failure);
            }
            guard.check();
        }

        @Override
        public void close() throws IOException {
            opened.close();
        }

        private static MessageDigest digest() {
            try {
                return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 unavailable", impossible);
            }
        }

        private static String strictUtf8(byte[] bytes) {
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes)).toString();
            } catch (CharacterCodingException malformed) {
                throw invalid("quality member row is not strict UTF-8", malformed);
            }
        }
    }
}
