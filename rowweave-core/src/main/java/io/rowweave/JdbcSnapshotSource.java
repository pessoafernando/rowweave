package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** One application-provided partition bound to one reopenable database snapshot. */
public final class JdbcSnapshotSource implements BatchExecutableSource {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_FETCH_SIZE = 1000;
    public static final int MAX_FETCH_SIZE = 10_000;
    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 300;
    public static final int MAX_QUERY_TIMEOUT_SECONDS = 3600;
    public static final int MIN_CELL_BYTES = 1024;
    public static final int DEFAULT_MAX_CELL_BYTES = 1024 * 1024;
    public static final int MAX_CELL_BYTES = 16 * 1024 * 1024;

    private final DataSchema dataSchema;
    private final JdbcSnapshotSourceProvider provider;
    private final String providerType;
    private final int providerVersion;
    private final JdbcSnapshotReadRequest readRequest;
    private final BatchSourceSplitDescriptor batchSourceSplit;

    public JdbcSnapshotSource(
            DataSchema dataSchema, JdbcSnapshotSourceProvider provider,
            String endpointRef, String credentialRef, String datasetRef,
            String queryRef, String partitionRef, String snapshotRef,
            String queryDigest, String partitionDigest, String snapshotId,
            int fetchSize, int queryTimeoutSeconds, int maxCellBytes) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("jdbc snapshot dataSchema is required");
        }
        ProviderIdentity identity = validateProvider(provider);
        JdbcSnapshotReadRequest request = new JdbcSnapshotReadRequest(
                endpointRef, credentialRef, datasetRef, queryRef, partitionRef,
                snapshotRef, queryDigest, partitionDigest, snapshotId,
                fetchSize, queryTimeoutSeconds, maxCellBytes);
        this.dataSchema = dataSchema;
        this.provider = provider;
        this.providerType = identity.type;
        this.providerVersion = identity.version;
        this.readRequest = request;
        this.batchSourceSplit = descriptor(dataSchema, identity, request);
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    @Override
    public BatchSourceSplitDescriptor batchSourceSplit() {
        ProviderIdentity current = validateProvider(provider);
        if (!providerType.equals(current.type) || providerVersion != current.version) {
            throw new IllegalArgumentException(
                    "jdbc snapshot provider identity changed after plan binding");
        }
        return batchSourceSplit;
    }

    @Override
    public SourceExecutor<?> createSourceExecutor() {
        batchSourceSplit();
        return new JdbcSnapshotInputExecutor(this);
    }

    public JdbcSnapshotReadRequest readRequest() {
        return readRequest;
    }

    JdbcSnapshotSourceProvider provider() {
        return provider;
    }

    /** Stateless provider instance; exposed for durable trust verification only. */
    public JdbcSnapshotSourceProvider sourceProvider() {
        return provider;
    }

    String providerType() {
        return providerType;
    }

    int providerVersion() {
        return providerVersion;
    }

    private Object readResolve() throws ObjectStreamException {
        try {
            JdbcSnapshotSource validated = new JdbcSnapshotSource(
                    dataSchema, provider, readRequest.endpointRef(),
                    readRequest.credentialRef(), readRequest.datasetRef(),
                    readRequest.queryRef(), readRequest.partitionRef(),
                    readRequest.snapshotRef(), readRequest.queryDigest(),
                    readRequest.partitionDigest(), readRequest.snapshotId(),
                    readRequest.fetchSize(), readRequest.queryTimeoutSeconds(),
                    readRequest.maxCellBytes());
            if (!providerType.equals(validated.providerType)
                    || providerVersion != validated.providerVersion
                    || !batchSourceSplit.equals(validated.batchSourceSplit)) {
                throw new IllegalArgumentException(
                        "jdbc snapshot derived identity mismatch");
            }
            return validated;
        } catch (RuntimeException invalid) {
            InvalidObjectException rejected = new InvalidObjectException(
                    "invalid serialized JdbcSnapshotSource: " + invalid.getMessage());
            rejected.initCause(invalid);
            throw rejected;
        }
    }

    private static BatchSourceSplitDescriptor descriptor(
            DataSchema dataSchema, ProviderIdentity provider,
            JdbcSnapshotReadRequest request) {
        String splitId = digest("split", provider.type,
                Integer.toString(provider.version), request.endpointRef(),
                request.datasetRef(), request.queryDigest(),
                request.partitionDigest());
        String snapshotId = digest("snapshot", splitId, request.snapshotId());
        return new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                "rowweave.jdbc-snapshot/" + provider.type, provider.version,
                splitId, SchemaFingerprint.of(dataSchema), snapshotId,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private static ProviderIdentity validateProvider(
            JdbcSnapshotSourceProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException("jdbc snapshot provider is required");
        }
        Class<?> type = provider.getClass();
        if (!Modifier.isPublic(type.getModifiers())) {
            throw new IllegalArgumentException(
                    "jdbc snapshot provider class must be public");
        }
        try {
            Constructor<?> constructor = type.getConstructor();
            if (!Modifier.isPublic(constructor.getModifiers())) {
                throw new NoSuchMethodException();
            }
        } catch (NoSuchMethodException missing) {
            throw new IllegalArgumentException(
                    "jdbc snapshot provider requires a public no-arg constructor");
        }
        for (Class<?> current = type; current != null && current != Object.class;
             current = current.getSuperclass()) {
            for (java.lang.reflect.Field field : current.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    throw new IllegalArgumentException(
                            "jdbc snapshot provider must not declare instance fields");
                }
            }
        }
        String providerType = provider.providerType();
        if (providerType == null
                || !providerType.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
            throw new IllegalArgumentException(
                    "jdbc snapshot providerType is invalid or exceeds its hard limit");
        }
        int providerVersion = provider.providerVersion();
        if (providerVersion <= 0) {
            throw new IllegalArgumentException(
                    "jdbc snapshot providerVersion must be positive");
        }
        return new ProviderIdentity(providerType, providerVersion);
    }

    private static String digest(String... values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                digest.update((byte) (bytes.length >>> 24));
                digest.update((byte) (bytes.length >>> 16));
                digest.update((byte) (bytes.length >>> 8));
                digest.update((byte) bytes.length);
                digest.update(bytes);
            }
            byte[] bytes = digest.digest();
            char[] output = new char[bytes.length * 2];
            char[] digits = "0123456789abcdef".toCharArray();
            for (int i = 0; i < bytes.length; i++) {
                output[i * 2] = digits[(bytes[i] >>> 4) & 0xf];
                output[i * 2 + 1] = digits[bytes[i] & 0xf];
            }
            return new String(output);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    @Override
    public String toString() {
        return "JdbcSnapshotSource{" + providerType
                + ", split=" + batchSourceSplit.splitId().substring(0, 16)
                + ", snapshot=" + batchSourceSplit.snapshotId().substring(0, 16) + '}';
    }

    private static final class ProviderIdentity {
        private final String type;
        private final int version;

        private ProviderIdentity(String type, int version) {
            this.type = type;
            this.version = version;
        }
    }
}
