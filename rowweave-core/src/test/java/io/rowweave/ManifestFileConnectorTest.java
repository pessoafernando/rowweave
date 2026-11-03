package io.rowweave;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A manifest-aware connector with a single commit point for the whole group locally. */
class ManifestFileConnectorTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("label", DataType.StringType),
            new Field("amount", DataType.BigDecimalType),
            new Field("time", DataType.TimestampType));

    @Test
    void singleMemberAlsoPublishesThroughManifestCoordinator(@TempDir Path tmp) throws Exception {
        Path domain = tmp.resolve("single-domain");
        DataRow expected = row(1);
        ExecutionEnvironment session = RowWeave.createEnvironment("manifest-single");
        session.fromSource(new CollectionSource(META, Collections.singletonList(expected)))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "ledger"));

        session.execute();

        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertNotNull(manifest);
        assertEquals(1, manifest.members().size());
        assertEquals("ledger", manifest.members().get(0).name());
        assertRowEquals(expected, readAll(new ManifestFileSource(META, domain.toString(), "ledger")).get(0));
    }

    @Test
    void twoToFiveMembersShareOneGenerationAndMatchFileOutputBytes(@TempDir Path tmp) throws Exception {
        for (int memberCount = 2; memberCount <= 5; memberCount++) {
            Path domain = tmp.resolve("domain-" + memberCount);
            ExecutionEnvironment session = RowWeave.createEnvironment("manifest-" + memberCount);
            List<DataRow> expected = new ArrayList<>();
            for (int i = 0; i < memberCount; i++) {
                DataRow row = row(100L * memberCount + i);
                expected.add(row);
                session.fromSource(new CollectionSource(META, Collections.singletonList(row)))
                        .sinkTo(new ManifestFileSink(META, domain.toString(), "member-" + i));
            }

            session.execute();

            ManifestFileStore store = new ManifestFileStore(domain);
            CommitManifest manifest = store.latest();
            assertNotNull(manifest);
            assertEquals(memberCount, manifest.members().size());
            try (Stream<Path> manifests = Files.list(domain.resolve("manifests"))) {
                assertEquals(1L, manifests.count(), "the whole group has exactly one visibility point");
            }
            for (int i = 0; i < memberCount; i++) {
                CommitManifest.Member member = member(manifest, "member-" + i);
                Path immutableData = domain.resolve(member.relativeDataPath());
                Path oracle = tmp.resolve("oracle-" + memberCount + "-" + i + ".tsv");
                FileSink file = new FileSink(META, oracle.toString());
                file.collect(expected.get(i));
                file.close();
                assertArrayEquals(Files.readAllBytes(oracle), Files.readAllBytes(immutableData),
                        "the manifest connector must be bit-identical to the FileSink row encoding");
                List<DataRow> actual = readAll(
                        new ManifestFileSource(META, domain.toString(), "member-" + i));
                assertEquals(1, actual.size());
                assertRowEquals(expected.get(i), actual.get(0));
            }
        }
    }

    @Test
    void readerPinsLatestOrExplicitGenerationSnapshot(@TempDir Path tmp) {
        Path domain = tmp.resolve("snapshots");
        commitOne(domain, row(11));
        long first = new ManifestFileStore(domain).latest().generation();
        commitOne(domain, row(22));
        long second = new ManifestFileStore(domain).latest().generation();

        assertTrue(second > first);
        assertEquals(22L, readAll(new ManifestFileSource(
                META, domain.toString(), "ledger")).get(0).get(0));
        assertEquals(11L, readAll(new ManifestFileSource(
                META, domain.toString(), "ledger", first)).get(0).get(0));
    }

    @Test
    void missingMemberAndSchemaMismatchFailLoud(@TempDir Path tmp) {
        Path domain = tmp.resolve("validation");
        commitOne(domain, row(31));

        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                () -> new ManifestFileSource(META, domain.toString(), "missing").openReader());
        assertTrue(missing.getMessage().contains("missing"), missing.getMessage());

        DataSchema wrong = new DataSchema(new Field("id", DataType.LongType));
        IllegalArgumentException schema = assertThrows(IllegalArgumentException.class,
                () -> new ManifestFileSource(wrong, domain.toString(), "ledger").openReader());
        assertTrue(schema.getMessage().contains("schema"), schema.getMessage());
    }

    @Test
    void mixedManifestDomainsAndLegacyOutputFailBeforeInputOpens(@TempDir Path tmp) {
        AtomicInteger opens = new AtomicInteger();
        Source source = countingInput(opens);
        ExecutionEnvironment domains = RowWeave.createEnvironment("mixed-manifest-domains");
        DataSet shared = domains.fromSource(source);
        shared.sinkTo(new ManifestFileSink(META, tmp.resolve("a").toString(), "left"));
        shared.sinkTo(new ManifestFileSink(META, tmp.resolve("b").toString(), "right"));

        IllegalArgumentException mixedDomain = assertThrows(
                IllegalArgumentException.class, domains::execute);
        assertTrue(mixedDomain.getMessage().contains("same manifest commit domain"),
                mixedDomain.getMessage());
        assertEquals(0, opens.get());
        assertFalse(Files.exists(tmp.resolve("a")));
        assertFalse(Files.exists(tmp.resolve("b")));

        ExecutionEnvironment legacy = RowWeave.createEnvironment("mixed-manifest-legacy");
        DataSet again = legacy.fromSource(source);
        again.sinkTo(new ManifestFileSink(META, tmp.resolve("c").toString(), "manifest"));
        again.sinkTo(new FileSink(META, tmp.resolve("legacy.tsv").toString()));

        IllegalArgumentException mixedType = assertThrows(IllegalArgumentException.class, legacy::execute);
        assertTrue(mixedType.getMessage().contains("cannot mix"), mixedType.getMessage());
        assertEquals(0, opens.get());
        assertFalse(Files.exists(tmp.resolve("c")));
    }

    @Test
    void dataInstallFailureRollsBackOnlyOwnedNewGenerationAndKeepsOldVisible(
            @TempDir Path tmp) throws Exception {
        Path domain = tmp.resolve("install-failure");
        commitPair(domain, row(41), row(42));
        CommitManifest old = new ManifestFileStore(domain).latest();
        assertEquals(1L, old.generation());

        Path foreignConflict = domain.resolve("data/2/b.data");
        Files.createDirectories(foreignConflict);
        ExecutionEnvironment failing = RowWeave.createEnvironment("manifest-install-failure");
        failing.fromSource(new CollectionSource(META, Collections.singletonList(row(51))))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "a"));
        failing.fromSource(new CollectionSource(META, Collections.singletonList(row(52))))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "b"));

        RuntimeException failure = assertThrows(RuntimeException.class, failing::execute);

        assertTrue(allMessages(failure).contains("not an owned regular file"), allMessages(failure));
        assertEquals(1L, new ManifestFileStore(domain).latest().generation());
        assertEquals(41L, readAll(new ManifestFileSource(
                META, domain.toString(), "a")).get(0).get(0));
        assertEquals(42L, readAll(new ManifestFileSource(
                META, domain.toString(), "b")).get(0).get(0));
        assertFalse(Files.exists(domain.resolve("data/2/a.data")),
                "members installed by this attempt must be rolled back");
        assertTrue(Files.isDirectory(foreignConflict),
                "a conflicting foreign target with different content or type must never be deleted by rollback");
        assertFalse(Files.exists(domain.resolve("manifests/2.manifest")));
    }

    @Test
    void connectorDescriptionsRemainStableAcrossSerialization(@TempDir Path tmp) throws Exception {
        Path domain = tmp.resolve("serialization");
        ManifestFileSink output = new ManifestFileSink(META, domain.toString(), "ledger");
        ManifestFileSink outputCopy = roundTrip(output);
        assertEquals(output.domainId(), outputCopy.domainId());
        assertEquals(output.exclusiveDestinationId(), outputCopy.exclusiveDestinationId());
        assertTrue(outputCopy.isWorkerWritable(), "the connector is Worker-writable now that the group commit protocol is in place");

        commitOne(domain, row(61));
        ManifestFileSource inputCopy = roundTrip(
                new ManifestFileSource(META, domain.toString(), "ledger"));
        assertEquals(61L, readAll(inputCopy).get(0).get(0));
    }

    @Test
    void manifestInputRunsAsAStreamingJobSource(@TempDir Path tmp) throws Exception {
        Path domain = tmp.resolve("source-domain");
        commitOne(domain, row(71));
        Path copied = tmp.resolve("copied.tsv");
        ExecutionEnvironment session = RowWeave.createEnvironment("manifest-input-source");
        session.fromSource(new ManifestFileSource(META, domain.toString(), "ledger"))
                .sinkTo(new FileSink(META, copied.toString()));

        session.execute();

        List<DataRow> rows = FileSink.read(META, copied.toString());
        assertEquals(1, rows.size());
        assertEquals(71L, rows.get(0).get(0));
    }

    @Test
    void existingDomainIdentityCannotBeRetargetedAfterDescriptionCreation(@TempDir Path tmp)
            throws Exception {
        Path first = Files.createDirectory(tmp.resolve("first"));
        Path second = Files.createDirectory(tmp.resolve("second"));
        Path alias = tmp.resolve("active");
        Files.createSymbolicLink(alias, first);
        ManifestFileSink output = new ManifestFileSink(META, alias.toString(), "ledger");
        Files.delete(alias);
        Files.createSymbolicLink(alias, second);
        ExecutionEnvironment session = RowWeave.createEnvironment("manifest-retarget");
        session.fromSource(new CollectionSource(META, Collections.singletonList(row(81))))
                .sinkTo(output);

        IllegalStateException failure = assertThrows(IllegalStateException.class, session::execute);
        assertTrue(allMessages(failure).contains("identity changed"), allMessages(failure));
        assertFalse(Files.exists(second.resolve("manifests")));
    }

    private static void commitOne(Path domain, DataRow row) {
        ExecutionEnvironment session = RowWeave.createEnvironment("manifest-generation");
        session.fromSource(new CollectionSource(META, Collections.singletonList(row)))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "ledger"));
        session.execute();
    }

    private static void commitPair(Path domain, DataRow a, DataRow b) {
        ExecutionEnvironment session = RowWeave.createEnvironment("manifest-pair");
        session.fromSource(new CollectionSource(META, Collections.singletonList(a)))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "a"));
        session.fromSource(new CollectionSource(META, Collections.singletonList(b)))
                .sinkTo(new ManifestFileSink(META, domain.toString(), "b"));
        session.execute();
    }

    private static Source countingInput(AtomicInteger opens) {
        return new Source() {
            @Override
            public DataSchema getSchema() {
                return META;
            }

            @Override
            public Iterator<DataRow> iterator() {
                opens.incrementAndGet();
                return Collections.singletonList(row(9)).iterator();
            }
        };
    }

    private static List<DataRow> readAll(ManifestFileSource input) {
        List<DataRow> rows = new ArrayList<>();
        try (FileSource.Reader reader = input.openReader()) {
            while (reader.hasNext()) {
                rows.add(reader.next());
            }
        }
        return rows;
    }

    private static CommitManifest.Member member(CommitManifest manifest, String name) {
        for (CommitManifest.Member member : manifest.members()) {
            if (name.equals(member.name())) {
                return member;
            }
        }
        throw new AssertionError("missing member " + name);
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(4);
        row.set(0, id);
        row.set(1, id % 2 == 0 ? "tab\tline\n" : null);
        row.set(2, new BigDecimal(id + ".5000000000"));
        Timestamp timestamp = new Timestamp(1_700_000_000_000L + id);
        timestamp.setNanos(123_456_789);
        row.set(3, timestamp);
        return row;
    }

    private static void assertRowEquals(DataRow expected, DataRow actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), actual.get(i), "column " + i);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) in.readObject();
        }
    }

    private static String allMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        appendMessages(failure, messages);
        return messages.toString();
    }

    private static void appendMessages(Throwable failure, StringBuilder messages) {
        if (failure == null) {
            return;
        }
        messages.append(failure.getMessage()).append('\n');
        appendMessages(failure.getCause(), messages);
        for (Throwable suppressed : failure.getSuppressed()) {
            appendMessages(suppressed, messages);
        }
    }
}
