package io.rowweave.plan;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** durable replay must unwrap and trust the reserved quality source. */
class ObjectStorageQualityDurableAdmissionTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType));

    @Test
    void canonicalWrapperUsesUnderlyingProviderTrustNotWrapperTrust(
            @TempDir Path temp) {
        PlanGraph graph = qualityGraph(temp.resolve("quality"));

        assertDoesNotThrow(() -> BatchSourcePlanValidator.validateDurable(
                graph, ObjectTrustContext.localTrusted(getClass().getClassLoader())));
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> BatchSourcePlanValidator.validateDurable(
                        graph, ObjectTrustContext.mtlsBuiltIns()));
        assertTrue(rejected.getMessage().contains("UNTRUSTED_OBJECT_STORAGE_PROVIDER"),
                rejected::getMessage);
    }

    @Test
    void customInputCannotClaimReservedObjectQualityCapability() {
        PlanGraph graph = new PlanGraph(Collections.singletonList(
                PlanNode.sourceFromInputs(META,
                                Collections.singletonList(new SpoofedQualityInput()))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger.source")));

        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
                () -> BatchSourcePlanValidator.validateDurable(
                        graph, ObjectTrustContext.localTrusted(getClass().getClassLoader())));
        assertTrue(rejected.getMessage().contains("RESERVED_OBJECT_QUALITY_SOURCE"),
                rejected::getMessage);
    }

    private static PlanGraph qualityGraph(Path domain) {
        byte[] empty = new byte[0];
        ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                META, new AdmissionProvider(), "endpoint", "credential", "domain",
                "ledger.tsv", "version-1", 0L, sha256(empty),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);
        ExecutionEnvironment session = new ExecutionEnvironment("quality-durable-admission", "quality durable");
        session.classifyObjectStorageRows("ledger.source", input)
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        return session.toPlanGraph();
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte item : digest) {
                result.append(Character.forDigit((item >>> 4) & 0xf, 16));
                result.append(Character.forDigit(item & 0xf, 16));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static final class AdmissionProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        public AdmissionProvider() { }
        @Override public String providerType() { return "quality-admission"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            throw new AssertionError("admission must not open the provider");
        }
    }

    private static final class SpoofedQualityInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor = new BatchSourceSplitDescriptor(
                1, "rowweave.object-storage-row-quality/forged", 1, "forged-quality",
                SchemaFingerprint.of(META), "forged-snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }
}
