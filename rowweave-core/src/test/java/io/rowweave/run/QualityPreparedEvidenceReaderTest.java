package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.Source;
import io.rowweave.ManifestFileSink;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.QualityExemplar;
import io.rowweave.QualityEvidenceReadGuard;
import io.rowweave.QualityGatePolicy;
import io.rowweave.QualityPreparedEvidence;
import io.rowweave.QualityPreparedEvidenceReader;
import io.rowweave.QualityReasonKey;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.CommitManifest;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.sched.stream.SourceRecordPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** sealed immutable evidence is streamed and canonically sampled. */
class QualityPreparedEvidenceReaderTest {

    private static final long DATA_GENERATION = 17L;
    private static final DataSchema INPUT_META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));
    private static final QualityReasonKey NULL_FIELD = QualityReasonKey.of(
            "QUALITY_RULE_VIOLATION", "adaptor.required", 0, "NOT_NULL");
    private static final QualityReasonKey BAD_PERIOD = QualityReasonKey.of(
            "QUALITY_RULE_VIOLATION", "period.range", 1,
            "INTEGER_RANGE_CLOSED");
    private static final QualityReasonKey BAD_PERIOD_TYPE = QualityReasonKey.of(
            "QUALITY_VALUE_TYPE_MISMATCH", "", 1, "IntegerType");
    private static final QualityReasonKey OBJECT_CR = QualityReasonKey.of(
            "QUALITY_RAW_CR_INVALID", "OBJECT_ROW_DECODE_V1", -1, "CR");
    private static final QualityReasonKey OBJECT_FORMAT = QualityReasonKey.of(
            "QUALITY_RAW_FORMAT_INVALID", "OBJECT_ROW_DECODE_V1", -1,
            "ROWWEAVE_ROW_TEXT_V1");
    private static final QualityReasonKey OBJECT_UTF8 = QualityReasonKey.of(
            "QUALITY_RAW_UTF8_INVALID", "OBJECT_ROW_DECODE_V1", -1, "UTF8");

    /** maximum admissible Plan shape must not time out on cache misses. */
    @Test
    @Tag("profile")
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void maximumPlanAlternatingCacheMissesMeetEvidenceDeadline(@TempDir Path temp)
            throws Exception {
        int rejectedCount = Integer.getInteger(
                "rowweave.quality.evidence.profile.rows", 10_001);
        long deadlineSeconds = Long.getLong(
                "rowweave.quality.evidence.profile.deadlineSeconds", 45L);
        assertTrue(rejectedCount > 0);
        assertTrue(deadlineSeconds > 0L);

        ProfileShape shape = maximumProfileShape(temp.resolve("maximum-plan"));
        List<DataRow> rejected = new ArrayList<>(rejectedCount);
        long firstCount = 0L;
        long lastCount = 0L;
        for (int i = 0; i < rejectedCount; i++) {
            boolean last = (i & 1) != 0;
            rejected.add(rejected("quality.source",
                    last ? shape.lastSplitDigest : shape.firstSplitDigest,
                    i, last ? shape.lastReason : shape.firstReason));
            if (last) lastCount++; else firstCount++;
        }
        List<DataRow> summary = Arrays.asList(
                summary("ACCEPTED_TOTAL", "", "", -1, "", 0L),
                reason(shape.firstReason, firstCount),
                reason(shape.lastReason, lastCount),
                summary("REJECTED_TOTAL", "", "", -1, "", rejectedCount));
        Fixture fixture = fixture(shape.graph, temp.resolve("maximum-plan"),
                summary, rejected, false);

        long started = System.nanoTime();
        QualityPreparedEvidence evidence = QualityPreparedEvidenceReader.read(
                fixture.graph, fixture.journal, QualityEvidenceReadGuard.until(
                        started + TimeUnit.SECONDS.toNanos(deadlineSeconds),
                        () -> false));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - started);

        assertEquals(rejectedCount, evidence.rejectedTotal());
        assertEquals(QualityGatePolicy.MAX_REASON_THRESHOLDS,
                evidence.reasonObservations().size());
        assertEquals(QualityGatePolicy.MAX_EXEMPLARS, evidence.exemplars().size());
        assertTrue(elapsedMillis < TimeUnit.SECONDS.toMillis(deadlineSeconds),
                "maximum Plan evidence scan exceeded SLA: " + elapsedMillis + "ms");
        System.out.println("maximum-plan alternating-cache-miss evidence rows="
                + rejectedCount + " elapsed=" + elapsedMillis + "ms deadline="
                + deadlineSeconds + "s");
    }

    @Test
    void readsCountsAndConfiguredReasonsWithoutOpeningValidMember(@TempDir Path temp)
            throws Exception {
        QualityGatePolicy policy = policy(3);
        List<DataRow> rejected = rejectedRows();
        Fixture fixture = fixture(temp.resolve("evidence"), policy,
                summaryRows(2L, 5L, 3L, 2L), rejected, false);

        QualityPreparedEvidence evidence = QualityPreparedEvidenceReader.read(
                fixture.graph, fixture.journal, guard());

        assertEquals(2L, evidence.acceptedTotal());
        assertEquals(5L, evidence.rejectedTotal());
        assertEquals(3L, evidence.observedReason(NULL_FIELD));
        assertEquals(2L, evidence.observedReason(BAD_PERIOD));
        assertEquals(Arrays.asList(
                QualityExemplar.of("quality.source", typedSplitDigest(), 0L,
                        "QUALITY_RULE_VIOLATION"),
                QualityExemplar.of("quality.source", typedSplitDigest(), 1L,
                        "QUALITY_RULE_VIOLATION"),
                QualityExemplar.of("quality.source", typedSplitDigest(), 7L,
                        "QUALITY_RULE_VIOLATION")), evidence.exemplars());
        assertEquals(3, evidence.members().size());
        assertFalse(Files.exists(fixture.validPath),
                "valid payload must be bound by receipt, never scanned");
    }

    @Test
    void canonicalTopKIgnoresPhysicalOrderAndFreshAttemptIdentity(@TempDir Path temp)
            throws Exception {
        List<DataRow> firstOrder = rejectedRows();
        List<DataRow> reverseOrder = new ArrayList<>(firstOrder);
        Collections.reverse(reverseOrder);
        Fixture first = fixture(temp.resolve("p1"), policy(4),
                summaryRows(2L, 5L, 3L, 2L), firstOrder, false);
        Fixture retry = fixture(temp.resolve("p4-retry"), policy(4),
                summaryRows(2L, 5L, 3L, 2L), reverseOrder, true);

        QualityPreparedEvidence left = QualityPreparedEvidenceReader.read(
                first.graph, first.journal, guard());
        QualityPreparedEvidence right = QualityPreparedEvidenceReader.read(
                retry.graph, retry.journal, guard());

        assertEquals(left.acceptedTotal(), right.acceptedTotal());
        assertEquals(left.rejectedTotal(), right.rejectedTotal());
        assertEquals(left.reasonObservations(), right.reasonObservations());
        assertEquals(left.exemplars(), right.exemplars());
    }

    @Test
    void rejectsHashLengthShapeOrderAndCountCorruption(@TempDir Path temp)
            throws Exception {
        Fixture drift = fixture(temp.resolve("drift"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        Files.write(drift.rejectedPath, "corrupt".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND);
        assertCorrupt(drift);

        Fixture sameLengthDrift = fixture(temp.resolve("same-length-drift"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        byte[] changed = Files.readAllBytes(sameLengthDrift.rejectedPath);
        changed[0] = changed[0] == 'z' ? (byte) 'y' : (byte) 'z';
        Files.write(sameLengthDrift.rejectedPath, changed);
        assertCorrupt(sameLengthDrift);

        List<DataRow> badSummary = summaryRows(2L, 6L, 3L, 2L);
        Fixture countMismatch = fixture(temp.resolve("count"), policy(2),
                badSummary, rejectedRows(), false);
        assertCorrupt(countMismatch);

        List<DataRow> duplicateReason = summaryRows(2L, 5L, 3L, 2L);
        duplicateReason.add(2, reason(NULL_FIELD, 1L));
        Fixture duplicate = fixture(temp.resolve("duplicate"), policy(2),
                duplicateReason, rejectedRows(), false);
        assertCorrupt(duplicate);

        List<DataRow> unordered = summaryRows(2L, 5L, 3L, 2L);
        Collections.swap(unordered, 0, 1);
        Fixture wrongOrder = fixture(temp.resolve("order"), policy(2),
                unordered, rejectedRows(), false);
        assertCorrupt(wrongOrder);

        QualityReasonKey impossibleForPlan = QualityReasonKey.of(
                "QUALITY_RULE_VIOLATION", "period.range", 1,
                "LONG_RANGE_CLOSED");
        List<DataRow> foreignReason = new ArrayList<>(Arrays.asList(
                summary("ACCEPTED_TOTAL", "", "", -1, "", 2L),
                reason(impossibleForPlan, 5L),
                summary("REJECTED_TOTAL", "", "", -1, "", 5L)));
        List<DataRow> foreignRejected = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            foreignRejected.add(rejected("quality.source", typedSplitDigest(), i,
                    impossibleForPlan));
        }
        Fixture outsideClosedSet = fixture(temp.resolve("closed-set"), policy(2),
                foreignReason, foreignRejected, false);
        assertCorrupt(outsideClosedSet);

        Fixture noncanonical = fixture(temp.resolve("noncanonical"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        String summaryText = new String(Files.readAllBytes(noncanonical.summaryPath),
                StandardCharsets.UTF_8);
        Files.write(noncanonical.summaryPath,
                (summaryText.substring(0, summaryText.length() - 2) + "+5\n")
                        .getBytes(StandardCharsets.UTF_8));
        assertCorrupt(reseal(noncanonical, "typed-quality.summary",
                noncanonical.summaryPath));

        Fixture linked = fixture(temp.resolve("linked"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        Path linkedTarget = temp.resolve("linked-rejected-target.data");
        Files.move(linked.rejectedPath, linkedTarget);
        Files.createSymbolicLink(linked.rejectedPath, linkedTarget);
        assertCorrupt(linked);

        List<DataRow> wrongShape = new ArrayList<>(rejectedRows());
        wrongShape.set(0, rejected("quality.source", typedSplitDigest(), -1L,
                NULL_FIELD));
        Fixture malformed = fixture(temp.resolve("shape"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), wrongShape, false);
        assertCorrupt(malformed);
    }

    @Test
    void rejectsEvidenceOutsideTheUniqueSourceAndExactSplit(@TempDir Path temp)
            throws Exception {
        List<DataRow> foreignUidRows = new ArrayList<>(rejectedRows());
        foreignUidRows.set(0, rejected("foreign.source", typedSplitDigest(), 7L,
                BAD_PERIOD));
        Fixture foreignUid = fixture(temp.resolve("foreign-uid"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), foreignUidRows, false);
        assertCorrupt(foreignUid);

        List<DataRow> foreignSplitRows = new ArrayList<>(rejectedRows());
        foreignSplitRows.set(0, rejected("quality.source", hex('f'), 7L,
                BAD_PERIOD));
        Fixture foreignSplit = fixture(temp.resolve("foreign-split"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), foreignSplitRows, false);
        assertCorrupt(foreignSplit);
    }

    @Test
    void rejectsAPlanAncestorReplacedByASymlink(@TempDir Path temp)
            throws Exception {
        Path anchor = temp.resolve("trusted-anchor");
        Fixture fixture = fixture(anchor.resolve("domain"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        Path relocated = temp.resolve("relocated-anchor");
        Files.move(anchor, relocated);
        Files.createSymbolicLink(anchor, relocated);

        assertCorrupt(fixture);
    }

    @Test
    void enforcesPathTrustAndEncodedRowBound(@TempDir Path temp)
            throws Exception {
        Fixture untrusted = fixture(temp.resolve("untrusted"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        Files.setPosixFilePermissions(untrusted.rejectedPath.getParent(),
                PosixFilePermissions.fromString("rwxrwxrwx"));
        assertCorrupt(untrusted);

        Fixture oversized = fixture(temp.resolve("oversized"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        byte[] row = new byte[4 * 1024 + 2];
        Arrays.fill(row, (byte) 'A');
        row[row.length - 1] = '\n';
        Files.write(oversized.rejectedPath, row);
        assertCorrupt(reseal(oversized, "typed-quality.rejected",
                oversized.rejectedPath));
    }

    @Test
    void holdsMaximumObservationsAndUniqueCanonicalTopK(@TempDir Path temp)
            throws Exception {
        List<DataRow> rejected = new ArrayList<>();
        String split = typedSplitDigest();
        for (long ordinal = 63L; ordinal >= 0L; ordinal--) {
            rejected.add(rejected("quality.source", split, ordinal, BAD_PERIOD));
        }
        rejected.add(rejected("quality.source", split, 0L, BAD_PERIOD));
        rejected.add(rejected("quality.source", split, 63L, BAD_PERIOD));
        Fixture fixture = fixture(temp.resolve("maximum"), maximumPolicy(),
                summaryRows(0L, 66L, 0L, 66L), rejected, false);

        QualityPreparedEvidence evidence = QualityPreparedEvidenceReader.read(
                fixture.graph, fixture.journal, guard());

        assertEquals(256, evidence.reasonObservations().size());
        assertEquals(64, evidence.exemplars().size());
        assertEquals(0L, evidence.exemplars().get(0).splitLocalOrdinal());
        assertEquals(63L, evidence.exemplars().get(63).splitLocalOrdinal());
    }

    @Test
    void actualP1P2P4OutputsHaveOneLogicalEvidenceProjection(@TempDir Path temp) {
        QualityPreparedEvidence oracle = null;
        for (int parallelism : new int[]{1, 2, 4}) {
            Path domain = temp.resolve("actual-p" + parallelism);
            ExecutionEnvironment session = new ExecutionEnvironment("quality-evidence-p" + parallelism,
                    "quality evidence actual output");
            session.setParallelism(parallelism);
            RowValidation validation = RowValidation.builder(INPUT_META)
                    .notNull("adaptor.required", 0)
                    .integerRange("period.range", 1, 1, 12).build();
            DataRowClassification classification = session.fromSource(
                            new CollectionSource(INPUT_META, "split-a",
                                    Arrays.asList(row("A", 1), row(null, 2))),
                            new CollectionSource(INPUT_META, "split-b",
                                    Collections.singletonList(row("B", 13))),
                            new CollectionSource(INPUT_META, "split-c",
                                    Collections.singletonList(row("C", 3))),
                            new CollectionSource(INPUT_META, "split-d",
                                    Collections.singletonList(row("D", 4))))
                    .uid("quality.actual.source")
                    .classifyRowsWithSourcePosition(validation);
            classification.writeToManifest(TypedQualityManifest.of(
                            domain.toString(), "valid", "rejected", "summary")
                    .withQualityGate(actualPolicy()));
            PlanGraph graph = session.toPlanGraph();
            session.execute();

            CommitManifest manifest = new ManifestFileStore(domain).latest();
            QualityPreparedEvidence evidence = QualityPreparedEvidenceReader.read(
                    graph, journal(graph, manifest, false), guard());
            assertEquals(3L, evidence.acceptedTotal());
            assertEquals(2L, evidence.rejectedTotal());
            assertEquals(1L, evidence.observedReason(NULL_FIELD));
            assertEquals(1L, evidence.observedReason(BAD_PERIOD));
            assertEquals(0L, evidence.observedReason(BAD_PERIOD_TYPE));
            if (oracle == null) {
                oracle = evidence;
            } else {
                assertEquals(oracle.acceptedTotal(), evidence.acceptedTotal());
                assertEquals(oracle.rejectedTotal(), evidence.rejectedTotal());
                assertEquals(oracle.reasonObservations(), evidence.reasonObservations());
                assertEquals(oracle.exemplars(), evidence.exemplars());
            }
        }
    }

    @Test
    void readsObjectQualityClosedDiagnostics(@TempDir Path temp) throws Exception {
        Path domain = temp.resolve("object-evidence");
        QualityGatePolicy policy = QualityGatePolicy.builder()
                .reasonThreshold(OBJECT_CR, 5L).exemplarLimit(2).build();
        PlanGraph graph = objectGraph(domain, policy);
        List<DataRow> summary = Arrays.asList(
                summary("ACCEPTED_TOTAL", "", "", -1, "", 1L),
                reason(OBJECT_CR, 1L),
                reason(OBJECT_FORMAT, 1L),
                reason(OBJECT_UTF8, 1L),
                summary("REJECTED_TOTAL", "", "", -1, "", 3L));
        List<DataRow> rejected = Arrays.asList(
                rejected("object.source", objectSplitDigest(), 2L, OBJECT_UTF8),
                rejected("object.source", objectSplitDigest(), 0L, OBJECT_CR),
                rejected("object.source", objectSplitDigest(), 1L, OBJECT_FORMAT));
        Fixture fixture = fixture(graph, domain, summary, rejected, false);

        QualityPreparedEvidence evidence = QualityPreparedEvidenceReader.read(
                graph, fixture.journal, guard());

        assertEquals(1L, evidence.acceptedTotal());
        assertEquals(3L, evidence.rejectedTotal());
        assertEquals(1L, evidence.observedReason(OBJECT_CR));
        assertEquals(Arrays.asList(
                QualityExemplar.of("object.source", objectSplitDigest(), 0L,
                        "QUALITY_RAW_CR_INVALID"),
                QualityExemplar.of("object.source", objectSplitDigest(), 1L,
                        "QUALITY_RAW_FORMAT_INVALID")), evidence.exemplars());
    }

    @Test
    void mandatoryGuardClassifiesDeadlineAndCancellation(@TempDir Path temp)
            throws Exception {
        Fixture fixture = fixture(temp.resolve("guard"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);

        QualityEvidenceReadGuard.Terminated deadline = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> QualityPreparedEvidenceReader.read(fixture.graph,
                        fixture.journal, QualityEvidenceReadGuard.until(
                                System.nanoTime() - 1L, () -> false)));
        assertEquals(QualityEvidenceReadGuard.Termination.DEADLINE_EXCEEDED,
                deadline.termination());

        QualityEvidenceReadGuard cancelled = guard();
        cancelled.cancel();
        QualityEvidenceReadGuard.Terminated cancellation = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> QualityPreparedEvidenceReader.read(
                        fixture.graph, fixture.journal, cancelled));
        assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                cancellation.termination());

        QualityEvidenceReadGuard.Terminated both = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> QualityPreparedEvidenceReader.read(fixture.graph,
                        fixture.journal, QualityEvidenceReadGuard.until(
                                System.nanoTime() - 1L, () -> true)));
        assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                both.termination());
    }

    @Test
    void cancellationAtDecodeCheckpointKeepsStableTermination(@TempDir Path temp)
            throws Exception {
        Fixture fixture = fixture(temp.resolve("decode-cancel"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        QualityEvidenceReadGuard guard = QualityEvidenceReadGuard.until(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(30L),
                () -> stackContains("QualityPreparedEvidenceReader", "decode"));

        QualityEvidenceReadGuard.Terminated terminated = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> QualityPreparedEvidenceReader.read(
                        fixture.graph, fixture.journal, guard));

        assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                terminated.termination());
    }

    @Test
    void cancellationAfterMetadataCheckpointKeepsStableTermination(@TempDir Path temp)
            throws Exception {
        Fixture fixture = fixture(temp.resolve("metadata-cancel"), policy(2),
                summaryRows(2L, 5L, 3L, 2L), rejectedRows(), false);
        AtomicInteger guardedBasicChecks = new AtomicInteger();
        QualityEvidenceReadGuard guard = QualityEvidenceReadGuard.until(
                System.nanoTime() + TimeUnit.SECONDS.toNanos(30L), () ->
                        stackContains("QualityEvidencePathAccess",
                                "guardedBasicAttributes")
                                && guardedBasicChecks.incrementAndGet() == 2);

        QualityEvidenceReadGuard.Terminated terminated = assertThrows(
                QualityEvidenceReadGuard.Terminated.class,
                () -> QualityPreparedEvidenceReader.read(
                        fixture.graph, fixture.journal, guard));

        assertEquals(QualityEvidenceReadGuard.Termination.CANCELLED,
                terminated.termination());
    }

    @Test
    void canonicalSummaryRejectsZeroCountReason(@TempDir Path temp)
            throws Exception {
        List<DataRow> summary = summaryRows(2L, 5L, 3L, 2L);
        summary.add(summary.size() - 1, reason(BAD_PERIOD_TYPE, 0L));
        Fixture fixture = fixture(temp.resolve("zero-reason"), policy(2),
                summary, rejectedRows(), false);

        assertCorrupt(fixture);
    }

    private static boolean stackContains(String classSuffix, String method) {
        for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
            if (frame.getClassName().endsWith(classSuffix)
                    && method.equals(frame.getMethodName())) return true;
        }
        return false;
    }

    private static void assertCorrupt(Fixture fixture) {
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> QualityPreparedEvidenceReader.read(
                        fixture.graph, fixture.journal, guard()));
        assertTrue(failure.getMessage().startsWith("QUALITY_EVIDENCE_INVALID:"),
                failure::getMessage);
    }

    private static QualityGatePolicy policy(int exemplars) {
        return QualityGatePolicy.builder()
                .maxRejectedRows(10)
                .reasonThreshold(NULL_FIELD, 10)
                .reasonThreshold(BAD_PERIOD, 10)
                .exemplarLimit(exemplars)
                .build();
    }

    private static QualityEvidenceReadGuard guard() {
        return QualityEvidenceReadGuard.until(System.nanoTime()
                + TimeUnit.SECONDS.toNanos(30L), () -> false);
    }

    private static QualityGatePolicy actualPolicy() {
        return QualityGatePolicy.builder()
                .maxRejectedRows(10)
                .reasonThreshold(NULL_FIELD, 10)
                .reasonThreshold(BAD_PERIOD, 10)
                .reasonThreshold(BAD_PERIOD_TYPE, 10)
                .exemplarLimit(3)
                .build();
    }

    private static QualityGatePolicy maximumPolicy() {
        QualityGatePolicy.Builder builder = QualityGatePolicy.builder()
                .reasonThreshold(NULL_FIELD, Long.MAX_VALUE)
                .reasonThreshold(BAD_PERIOD, Long.MAX_VALUE)
                .exemplarLimit(64);
        for (int i = 0; i < 254; i++) {
            builder.reasonThreshold(QualityReasonKey.of(
                    "QUALITY_RULE_VIOLATION", "maximum.rule." + i,
                    0, "NOT_NULL"), Long.MAX_VALUE);
        }
        return builder.build();
    }

    private static ProfileShape maximumProfileShape(Path domain) {
        int fieldCount = RowValidation.MAX_FIELDS - 8;
        Field[] fields = new Field[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fields[i] = new Field(String.format("field_%04d", i),
                    DataType.StringType);
        }
        DataSchema meta = new DataSchema(fields);
        RowValidation.Builder validation = RowValidation.builder(meta);
        QualityReasonKey firstReason = null;
        QualityReasonKey lastReason = null;
        QualityGatePolicy.Builder policy = QualityGatePolicy.builder()
                .maxRejectedRows(Long.MAX_VALUE)
                .maxRejectedPartsPerMillion(1_000_000)
                .exemplarLimit(QualityGatePolicy.MAX_EXEMPLARS);
        for (int i = 0; i < RowValidation.MAX_RULES; i++) {
            String ruleId = String.format("rule.%04d", i);
            int field = i % fieldCount;
            validation.notNull(ruleId, field);
            QualityReasonKey reason = QualityReasonKey.of(
                    "QUALITY_RULE_VIOLATION", ruleId, field, "NOT_NULL");
            if (i == 0) firstReason = reason;
            if (i == RowValidation.MAX_RULES - 1) lastReason = reason;
            if (i < QualityGatePolicy.MAX_REASON_THRESHOLDS - 1
                    || i == RowValidation.MAX_RULES - 1) {
                policy.reasonThreshold(reason, Long.MAX_VALUE);
            }
        }

        Source[] inputs = new Source[4_096];
        String firstSplitDigest = null;
        String lastSplitDigest = null;
        for (int i = 0; i < inputs.length; i++) {
            CollectionSource input = new CollectionSource(meta,
                    String.format("profile-split-%04d", i),
                    Collections.emptyList());
            inputs[i] = input;
            String digest = SourceRecordPosition.forSplit(
                    "quality.source", input.batchSourceSplit()).at(0L)
                    .splitIdentitySha256();
            if (i == 0) firstSplitDigest = digest;
            if (i == inputs.length - 1) lastSplitDigest = digest;
        }

        ExecutionEnvironment session = new ExecutionEnvironment(
                "maximum-quality-evidence", "maximum quality evidence");
        session.setParallelism(65_536);
        DataRowClassification classification = session.fromSource(inputs)
                .uid("quality.source")
                .classifyRowsWithSourcePosition(validation.build());
        classification.writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary")
                .withQualityGate(policy.build()));
        return new ProfileShape(session.toPlanGraph(), firstSplitDigest,
                lastSplitDigest, firstReason, lastReason);
    }

    private static List<DataRow> summaryRows(
            long accepted, long rejected, long nullFieldCount, long badPeriod) {
        List<DataRow> rows = new ArrayList<>();
        rows.add(summary("ACCEPTED_TOTAL", "", "", -1, "", accepted));
        if (nullFieldCount > 0L) rows.add(reason(NULL_FIELD, nullFieldCount));
        if (badPeriod > 0L) rows.add(reason(BAD_PERIOD, badPeriod));
        rows.add(summary("REJECTED_TOTAL", "", "", -1, "", rejected));
        return rows;
    }

    private static DataRow reason(QualityReasonKey key, long count) {
        return summary("REJECTED_REASON", key.errorCode(), key.ruleId(),
                key.fieldIndex(), key.detail(), count);
    }

    private static DataRow summary(String kind, String code, String rule,
                                int field, String detail, long count) {
        DataRow row = new DataRow(6);
        row.setValues(new Object[]{kind, code, rule, field, detail, count});
        return row;
    }

    private static List<DataRow> rejectedRows() {
        String split = typedSplitDigest();
        return Arrays.asList(
                rejected("quality.source", split, 7L, BAD_PERIOD),
                rejected("quality.source", split, 0L, NULL_FIELD),
                rejected("quality.source", split, 1L, NULL_FIELD),
                rejected("quality.source", split, 0L, BAD_PERIOD),
                rejected("quality.source", split, 0L, NULL_FIELD));
    }

    private static String typedSplitDigest() {
        CollectionSource input = new CollectionSource(
                INPUT_META, "quality-evidence", Collections.emptyList());
        return SourceRecordPosition.forSplit(
                "quality.source", input.batchSourceSplit()).at(0L)
                .splitIdentitySha256();
    }

    private static String objectSplitDigest() throws Exception {
        return SourceRecordPosition.forSplit(
                "object.source", objectInput().batchSourceSplit()).at(0L)
                .splitIdentitySha256();
    }

    private static DataRow rejected(
            String uid, String split, long ordinal, QualityReasonKey key) {
        DataRow row = new DataRow(7);
        row.setValues(new Object[]{uid, split, ordinal, key.errorCode(),
                key.ruleId(), key.fieldIndex(), key.detail()});
        return row;
    }

    private static Fixture fixture(
            Path domain, QualityGatePolicy policy, List<DataRow> summaryRows,
            List<DataRow> rejectedRows, boolean retry) throws Exception {
        return fixture(graph(domain, policy), domain, summaryRows, rejectedRows, retry);
    }

    private static Fixture fixture(
            PlanGraph graph, Path domain, List<DataRow> summaryRows,
            List<DataRow> rejectedRows, boolean retry) throws Exception {
        Map<String, ManifestFileSink> outputs = outputs(graph);
        Path data = domain.resolve("data").resolve(Long.toString(DATA_GENERATION));
        Files.createDirectories(data);
        Path rejectedPath = write(data.resolve("rejected.data"), REJECTED_META, rejectedRows);
        Path summaryPath = write(data.resolve("summary.data"),
                TypedQualityManifest.summarySchema(), summaryRows);
        Path validPath = data.resolve("valid.data");

        RunAttemptFence fence = new RunAttemptFence(
                "run", retry ? "attempt-2" : "attempt-1", retry ? 2L : 1L);
        List<StageAssignmentRecord> records = new ArrayList<>();
        List<String> roles = Arrays.asList(
                "typed-quality.valid", "typed-quality.rejected", "typed-quality.summary");
        for (int i = 0; i < roles.size(); i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0, StageAssignmentRole.SINK,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1, "inc-" + i));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact", 1L, records);
        List<StageAssignmentAcceptance> acceptances = new ArrayList<>();
        List<StageTerminalReceipt> terminals = new ArrayList<>();
        List<ManifestMemberReceipt> receipts = new ArrayList<>();
        for (int i = 0; i < roles.size(); i++) {
            String role = roles.get(i);
            ManifestFileSink output = outputs.get(role);
            Path path = "typed-quality.valid".equals(role) ? validPath
                    : "typed-quality.rejected".equals(role) ? rejectedPath : summaryPath;
            byte[] hash = "typed-quality.valid".equals(role)
                    ? sha256("unread-valid".getBytes(StandardCharsets.UTF_8))
                    : ManifestFileStore.sha256(path);
            long length = "typed-quality.valid".equals(role) ? 12L : Files.size(path);
            CommitManifest.Member member = new CommitManifest.Member(
                    output.memberName(), "data/" + DATA_GENERATION + "/"
                    + output.memberName() + ".data",
                    output.batchSinkDescriptor().schemaFingerprint(), length, hash);
            StageAssignmentToken token = StageAssignmentToken.forAssignment(
                    manifest, records.get(i), 1L);
            byte[] command = sha256(role.getBytes(StandardCharsets.UTF_8));
            acceptances.add(new StageAssignmentAcceptance(token, command, i));
            terminals.add(StageTerminalReceipt.succeeded(token, command, 1L, 1L, i));
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, records.get(i), role, output.batchSinkDescriptor());
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    output.domainId(), ManifestCommitWire.encodeMember(member),
                    length, hash, i);
            receipts.add(ManifestMemberReceipt.sealed(
                    token, command, output.domainId(), DATA_GENERATION,
                    "publication", member, i, locator));
        }
        StageAssignmentSnapshot assignments = new StageAssignmentSnapshot(
                manifest, 1L, acceptances, terminals);
        return new Fixture(graph, new ManifestCommitJournalSnapshot(
                assignments, receipts, null), validPath, rejectedPath, summaryPath);
    }

    private static Fixture reseal(Fixture fixture, String role, Path path)
            throws Exception {
        List<ManifestMemberReceipt> receipts = new ArrayList<>();
        int count = fixture.journal.assignments().manifest().assignmentCount();
        for (int i = 0; i < count; i++) {
            ManifestMemberReceipt current = fixture.journal.member(i);
            if (!role.equals(current.preparedLocator().attemptIdentity()
                    .sinkOperatorId())) {
                receipts.add(current);
                continue;
            }
            CommitManifest.Member old = current.member();
            CommitManifest.Member changed = new CommitManifest.Member(
                    old.name(), old.relativeDataPath(), old.schemaFingerprint(),
                    Files.size(path), ManifestFileStore.sha256(path));
            PreparedSinkLocator oldLocator = current.preparedLocator();
            PreparedSinkLocator changedLocator = new PreparedSinkLocator(
                    oldLocator.locatorVersion(), oldLocator.attemptIdentity(),
                    oldLocator.capabilityProfile(), oldLocator.commitDomainId(),
                    ManifestCommitWire.encodeMember(changed), changed.length(),
                    changed.sha256(), oldLocator.preparedAtMillis());
            receipts.add(ManifestMemberReceipt.sealed(
                    current.token(), current.commandDigest(), current.domainId(),
                    current.generation(), current.publicationId(), changed,
                    current.sealedAtMillis(), changedLocator));
        }
        return new Fixture(fixture.graph, new ManifestCommitJournalSnapshot(
                fixture.journal.assignments(), receipts, fixture.journal.intent()),
                fixture.validPath, fixture.rejectedPath, fixture.summaryPath);
    }

    private static ManifestCommitJournalSnapshot journal(
            PlanGraph graph, CommitManifest manifest, boolean retry) {
        Map<String, ManifestFileSink> outputs = outputs(graph);
        Map<String, CommitManifest.Member> members = new LinkedHashMap<>();
        for (CommitManifest.Member member : manifest.members()) {
            members.put(member.name(), member);
        }
        RunAttemptFence fence = new RunAttemptFence(
                "run", retry ? "attempt-2" : "attempt-1", retry ? 2L : 1L);
        List<String> roles = Arrays.asList(
                "typed-quality.valid", "typed-quality.rejected",
                "typed-quality.summary");
        List<StageAssignmentRecord> records = new ArrayList<>();
        for (int i = 0; i < roles.size(); i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0, StageAssignmentRole.SINK,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1, "inc-" + i));
        }
        StageAssignmentManifest assignments = new StageAssignmentManifest(
                fence, "artifact", 1L, records);
        List<StageAssignmentAcceptance> acceptances = new ArrayList<>();
        List<StageTerminalReceipt> terminals = new ArrayList<>();
        List<ManifestMemberReceipt> receipts = new ArrayList<>();
        for (int i = 0; i < roles.size(); i++) {
            String role = roles.get(i);
            ManifestFileSink output = outputs.get(role);
            CommitManifest.Member member = members.get(output.memberName());
            StageAssignmentToken token = StageAssignmentToken.forAssignment(
                    assignments, records.get(i), 1L);
            byte[] command;
            try {
                command = sha256(role.getBytes(StandardCharsets.UTF_8));
            } catch (Exception impossible) {
                throw new AssertionError(impossible);
            }
            acceptances.add(new StageAssignmentAcceptance(token, command, i));
            terminals.add(StageTerminalReceipt.succeeded(token, command, 1L, 1L, i));
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    assignments, records.get(i), role, output.batchSinkDescriptor());
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    output.domainId(), ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), i);
            receipts.add(ManifestMemberReceipt.sealed(
                    token, command, output.domainId(), manifest.generation(),
                    manifest.attemptId(), member, i, locator));
        }
        return new ManifestCommitJournalSnapshot(new StageAssignmentSnapshot(
                assignments, 1L, acceptances, terminals), receipts, null);
    }

    private static PlanGraph graph(Path domain, QualityGatePolicy policy) {
        ExecutionEnvironment session = new ExecutionEnvironment("evidence", "quality evidence");
        RowValidation validation = RowValidation.builder(INPUT_META)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12).build();
        DataRowClassification classification = session.fromSource(new CollectionSource(
                        INPUT_META, "quality-evidence", Collections.emptyList()))
                .uid("quality.source").classifyRowsWithSourcePosition(validation);
        classification.writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary")
                .withQualityGate(policy));
        return session.toPlanGraph();
    }

    private static PlanGraph objectGraph(Path domain, QualityGatePolicy policy)
            throws Exception {
        ObjectStorageSnapshotSource input = objectInput();
        ExecutionEnvironment session = new ExecutionEnvironment("object-evidence", "object evidence");
        session.classifyObjectStorageRows("object.source", input)
                .writeToManifest(TypedQualityManifest.of(
                                domain.toString(), "valid", "rejected", "summary")
                        .withQualityGate(policy));
        return session.toPlanGraph();
    }

    private static ObjectStorageSnapshotSource objectInput() throws Exception {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.LongType),
                new Field("amount", DataType.BigDecimalType));
        byte[] empty = new byte[0];
        return new ObjectStorageSnapshotSource(
                meta, new EvidenceObjectProvider(), "endpoint", "credential",
                "domain", "ledger.tsv", "version-1", 0L, hex(sha256(empty)),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);
    }

    private static Map<String, ManifestFileSink> outputs(PlanGraph graph) {
        Map<String, ManifestFileSink> result = new LinkedHashMap<>();
        for (PlanNode node : graph.nodes()) {
            if (node.kind() == PlanNode.Kind.SINK) {
                result.put(node.param(PlanNode.OPERATOR_UID_PARAM),
                        (ManifestFileSink) node.output());
            }
        }
        return result;
    }

    private static Path write(Path path, DataSchema meta, List<DataRow> rows) {
        FileSink output = new FileSink(meta, path.toString());
        for (DataRow row : rows) output.collect(row);
        output.close();
        return path;
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static String hex(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            result[i * 2] = alphabet[(value[i] >>> 4) & 0xf];
            result[i * 2 + 1] = alphabet[value[i] & 0xf];
        }
        return new String(result);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static final class Fixture {
        final PlanGraph graph;
        final ManifestCommitJournalSnapshot journal;
        final Path validPath;
        final Path rejectedPath;
        final Path summaryPath;

        Fixture(PlanGraph graph, ManifestCommitJournalSnapshot journal,
                Path validPath, Path rejectedPath, Path summaryPath) {
            this.graph = graph;
            this.journal = journal;
            this.validPath = validPath;
            this.rejectedPath = rejectedPath;
            this.summaryPath = summaryPath;
        }
    }

    private static final class ProfileShape {
        final PlanGraph graph;
        final String firstSplitDigest;
        final String lastSplitDigest;
        final QualityReasonKey firstReason;
        final QualityReasonKey lastReason;

        ProfileShape(PlanGraph graph, String firstSplitDigest,
                     String lastSplitDigest, QualityReasonKey firstReason,
                     QualityReasonKey lastReason) {
            this.graph = graph;
            this.firstSplitDigest = firstSplitDigest;
            this.lastSplitDigest = lastSplitDigest;
            this.firstReason = firstReason;
            this.lastReason = lastReason;
        }
    }

    public static final class EvidenceObjectProvider
            implements ObjectStorageSourceProvider {
        @Override public String providerType() { return "quality-evidence"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            throw new AssertionError("prepared evidence must not open the Object source");
        }
    }
}
