package io.rowweave.incremental;

import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** object increments come only from immutable manifests. */
class ObjectManifestIncrementalSourceProfileTest {
    private static final String A = repeat('a');
    private static final String B = repeat('b');

    @Test
    void workerOpenRejectsObjectRequestSwapBeforeLocalAccess() throws Exception {
        ObjectManifestIncrementalSourceProfile profile = profile();
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        ObjectManifestIncrementalSourceProfile.Manifest manifest =
                new ObjectManifestIncrementalSourceProfile.Manifest(
                        2L, sha256("manifest"), "manifest-version-2");
        CapturedTargetReceipt receipt = request.bind(
                profile.sourceConfig().contractDigest(),
                profile.capture(request, manifest));
        IncrementalMaterializationResponse correct = profile.materialize(
                receipt, manifest, Collections.singletonList(entry(0)));
        IncrementalMaterializationResponse other = profile.materialize(
                receipt, manifest, Collections.singletonList(entry(1)));
        IncrementalMaterializedSplit swapped = new IncrementalMaterializedSplit(
                correct.splits().get(0).descriptor(),
                other.splits().get(0).openRequest());
        io.rowweave.IncrementalSourceSplit input =
                new io.rowweave.IncrementalSourceSplit(
                        configuration(true, true, true).dataSchema(),
                        profile.sourceConfig(), receipt,
                        correct.partitionSetDigest(), swapped);
        final boolean[] opened = {false};
        ObjectManifestIncrementalSourceAccess access = objectAccess(opened);

        assertThrows(IllegalArgumentException.class,
                () -> new ObjectManifestIncrementalSourceProvider(access)
                        .openSplit(input));
        assertFalse(opened[0]);
    }

    @Test
    void coreProviderUsesWorkerLocalManifestAccess() throws Exception {
        ObjectManifestIncrementalSourceProfile profile = profile();
        ObjectManifestIncrementalSourceAccess access =
                new ObjectManifestIncrementalSourceAccess() {
                    @Override
                    public boolean supports(
                            ObjectManifestIncrementalSourceProfile candidate) {
                        return "manifest-prod".equals(candidate.manifestRef());
                    }

                    @Override
                    public ObjectManifestIncrementalSourceProfile.Manifest capture(
                            ObjectManifestIncrementalSourceProfile candidate,
                            IncrementalCaptureRequest request) {
                        return new ObjectManifestIncrementalSourceProfile.Manifest(
                                3L, digest("manifest-3"), "manifest-version-3");
                    }

                    @Override
                    public List<ObjectManifestIncrementalSourceProfile.Entry>
                    materialize(ObjectManifestIncrementalSourceProfile candidate,
                                CapturedTargetReceipt receipt,
                                ObjectManifestIncrementalSourceProfile.Manifest manifest) {
                        return Collections.singletonList(entry(0));
                    }

                    @Override
                    public io.rowweave.SourceExecutor<io.rowweave
                            .IncrementalSourceSplit> open(
                            ObjectManifestIncrementalSourceProfile candidate,
                            io.rowweave.IncrementalSourceSplit input,
                            ObjectStorageReadRequest request) {
                        throw new AssertionError("metadata test must not open rows");
                    }
                };
        ObjectManifestIncrementalSourceProvider provider =
                new ObjectManifestIncrementalSourceProvider(access);
        IncrementalSourceActionExecutor actions =
                new IncrementalSourceActionExecutor(
                        provider.contract(), profile.sourceConfig(), provider,
                        io.rowweave.internal.ProviderActionSupervisor
                                .processWide(2));
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        CapturedTargetReceipt receipt = actions.capture(
                request, System.nanoTime() + 5_000_000_000L);

        assertEquals(3L, receipt.target().generation());
        assertEquals(1, actions.materialize(receipt,
                System.nanoTime() + 5_000_000_000L,
                ignored -> { }).splits().size());
    }

    private static ObjectManifestIncrementalSourceAccess objectAccess(
            boolean[] opened) {
        return new ObjectManifestIncrementalSourceAccess() {
            @Override public boolean supports(
                    ObjectManifestIncrementalSourceProfile p) { return true; }
            @Override public ObjectManifestIncrementalSourceProfile.Manifest
            capture(ObjectManifestIncrementalSourceProfile p,
                    IncrementalCaptureRequest r) { throw new AssertionError(); }
            @Override public List<ObjectManifestIncrementalSourceProfile.Entry>
            materialize(ObjectManifestIncrementalSourceProfile p,
                        CapturedTargetReceipt r,
                        ObjectManifestIncrementalSourceProfile.Manifest m) {
                throw new AssertionError();
            }
            @Override public io.rowweave.SourceExecutor<io.rowweave
                    .IncrementalSourceSplit> open(
                    ObjectManifestIncrementalSourceProfile p,
                    io.rowweave.IncrementalSourceSplit i,
                    ObjectStorageReadRequest r) {
                opened[0] = true;
                return null;
            }
        };
    }

    @Test
    void captureFreezesManifestAndMaterializesOnlyExactObjectVersions()
            throws Exception {
        ObjectManifestIncrementalSourceProfile profile = profile();
        IncrementalSourceConfig inert = profile.sourceConfig();
        assertArrayEquals(inert.configurationBytes(),
                profile.canonicalizeConfiguration(inert));
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        ObjectManifestIncrementalSourceProfile.Manifest manifest =
                new ObjectManifestIncrementalSourceProfile.Manifest(
                        12L, sha256("manifest-12"), "manifest-version-12");
        CapturedTargetReceipt receipt = request.bind(
                inert.contractDigest(), profile.capture(request, manifest));
        IncrementalMaterializationResponse materialized = profile.materialize(
                receipt, manifest, java.util.Arrays.asList(
                        new ObjectManifestIncrementalSourceProfile.Entry(
                                "tenant-a", "delta/12/part-0", "version-1",
                                10L, A,
                                ObjectManifestIncrementalSourceProfile.EntryKind.DATA),
                        new ObjectManifestIncrementalSourceProfile.Entry(
                                "tenant-a", "delta/12/tombstone-0", "version-2",
                                11L, B,
                                ObjectManifestIncrementalSourceProfile.EntryKind.TOMBSTONE)));

        assertEquals(2, materialized.splits().size());
        assertTrue(profile.contract().tombstoneCapable());
        ObjectStorageReadRequest first =
                ObjectManifestIncrementalSourceProfile.decodeOpenRequest(
                        materialized.splits().get(0).openRequest());
        assertEquals("tenant-a", first.namespace());
        assertEquals("delta/12/part-0", first.objectKey());
        assertEquals("version-1", first.versionId());
        assertEquals(10L, first.contentLength());
        assertEquals(A, first.contentSha256());
        assertEquals(IncrementalFrontier.manifestGeneration(
                        12L, sha256("manifest-12")),
                receipt.target());
    }

    @Test
    void latestListingMutableManifestAndSnapshotDriftFailClosed()
            throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> configuration(false, true, true));
        assertThrows(IllegalArgumentException.class,
                () -> configuration(true, false, true));
        assertThrows(IllegalArgumentException.class,
                () -> configuration(true, true, false));
        assertThrows(IllegalArgumentException.class,
                () -> new ObjectManifestIncrementalSourceProfile.Entry(
                        "tenant-a", "delta/current", "latest", 1L, A,
                        ObjectManifestIncrementalSourceProfile.EntryKind.DATA));

        ObjectManifestIncrementalSourceProfile profile = profile();
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        ObjectManifestIncrementalSourceProfile.Manifest first =
                new ObjectManifestIncrementalSourceProfile.Manifest(
                        7L, sha256("manifest-a"), "manifest-version-7");
        CapturedTargetReceipt receipt = request.bind(
                profile.sourceConfig().contractDigest(),
                profile.capture(request, first));
        ObjectManifestIncrementalSourceProfile.Manifest drift =
                new ObjectManifestIncrementalSourceProfile.Manifest(
                        7L, sha256("manifest-b"), "manifest-version-7");
        assertThrows(IllegalArgumentException.class,
                () -> profile.materialize(receipt, drift,
                        Collections.singletonList(entry(0))));
    }

    @Test
    void splitBacklogIsBoundedAt4096BeforeResponseConstruction()
            throws Exception {
        ObjectManifestIncrementalSourceProfile profile = profile();
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        ObjectManifestIncrementalSourceProfile.Manifest manifest =
                new ObjectManifestIncrementalSourceProfile.Manifest(
                        9L, sha256("manifest"), "manifest-version-9");
        CapturedTargetReceipt receipt = request.bind(
                profile.sourceConfig().contractDigest(),
                profile.capture(request, manifest));
        List<ObjectManifestIncrementalSourceProfile.Entry> maximum =
                new ArrayList<>();
        for (int index = 0; index < 4096; index++) maximum.add(entry(index));
        assertEquals(4096,
                profile.materialize(receipt, manifest, maximum).splits().size());

        maximum.add(entry(4096));
        assertThrows(IllegalArgumentException.class,
                () -> profile.materialize(receipt, manifest, maximum));
    }

    private static ObjectManifestIncrementalSourceProfile profile() {
        return new ObjectManifestIncrementalSourceProfile(
                configuration(true, true, true));
    }

    private static ObjectManifestIncrementalSourceProfile.Configuration
    configuration(boolean immutableManifest, boolean exactVersionReads,
                  boolean noOpenTimeListing) {
        return new ObjectManifestIncrementalSourceProfile.Configuration(
                new io.rowweave.DataSchema(
                        new io.rowweave.Field(
                                "id", io.rowweave.DataType.LongType)),
                "object-endpoint", "object-credential", "manifest-prod",
                immutableManifest, exactVersionReads, noOpenTimeListing,
                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES);
    }

    private static ObjectManifestIncrementalSourceProfile.Entry entry(int index) {
        return new ObjectManifestIncrementalSourceProfile.Entry(
                "tenant-a", "delta/part-" + index, "version-" + index,
                1L, A,
                ObjectManifestIncrementalSourceProfile.EntryKind.DATA);
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] digest(String value) {
        try {
            return sha256(value);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
