package io.rowweave.commit;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/** Separate JVM for ManifestProcessCrashRecoveryTest; Runtime.halt only, so the process dies with no finally or shutdown hook. */
public final class ManifestCrashProbe {

    private static final int HALT_CODE = 41;
    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    private ManifestCrashProbe() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: <domain> <crash-point>");
        }
        Path domain = Paths.get(args[0]);
        String point = args[1];
        String attemptId = "crash-" + point;
        try (FileCommitDomainLease lease = requireLease(domain)) {
            FileCommitGenerationAuthority authority = new FileCommitGenerationAuthority();
            long generation = authority.reserve(domain, attemptId);
            System.out.println("GENERATION=" + generation);
            System.out.flush();
            ManifestFileStore plain = new ManifestFileStore(domain);
            Path a = writeStaged(plain, attemptId, "a", 101L);
            Path b = writeStaged(plain, attemptId, "b", 102L);
            haltAt(point, "DATA_WRITTEN");
            plain.forceStaged(a);
            plain.forceStaged(b);
            haltAt(point, "DATA_FORCED");
            String schema = SchemaFingerprint.of(META);
            CommitManifest.Member ma = plain.installMember(generation, "a", schema, a);
            haltAt(point, "PARTIAL_DATA_INSTALLED");
            CommitManifest.Member mb = plain.installMember(generation, "b", schema, b);
            haltAt(point, "ALL_DATA_INSTALLED");
            CommitManifest manifest = new CommitManifest(
                    plain.domainId(), generation, attemptId, Arrays.asList(ma, mb));
            ManifestFileStore observed = new ManifestFileStore(domain, (step, target) -> {
                if ("MANIFEST_TEMP_FORCED".equals(point)
                        && step == DurableFileOps.Step.TEMP_FILE_FORCED) {
                    Runtime.getRuntime().halt(HALT_CODE);
                }
                if ("MANIFEST_NAME_CREATED".equals(point)
                        && step == DurableFileOps.Step.IMMUTABLE_NAME_CREATED) {
                    Runtime.getRuntime().halt(HALT_CODE);
                }
            });
            observed.publish(manifest);
            haltAt(point, "BEFORE_ACK");
        }
    }

    private static FileCommitDomainLease requireLease(Path domain) {
        FileCommitDomainLease lease = FileCommitDomainLease.tryAcquire(domain);
        if (lease == null) {
            throw new IllegalStateException("failed to acquire crash probe domain lease");
        }
        return lease;
    }

    private static Path writeStaged(
            ManifestFileStore store, String attemptId, String member, long id) {
        Path path = store.stagingPath(attemptId, member);
        FileSink output = new FileSink(META, path.toString());
        DataRow row = new DataRow(1);
        row.set(0, id);
        output.collect(row);
        output.close();
        return path;
    }

    private static void haltAt(String actual, String expected) {
        if (expected.equals(actual)) {
            Runtime.getRuntime().halt(HALT_CODE);
        }
    }
}
