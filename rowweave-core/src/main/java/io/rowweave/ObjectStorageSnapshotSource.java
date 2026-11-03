package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** One application-provided split bound to one complete immutable object version. */
public final class ObjectStorageSnapshotSource implements BatchExecutableSource {

    private static final long serialVersionUID = 1L;

    public static final int MIN_ROW_BYTES = 1024;
    public static final int DEFAULT_MAX_ROW_BYTES = 1024 * 1024;
    public static final int MAX_ROW_BYTES = 16 * 1024 * 1024;

    private final DataSchema dataSchema;
    private final ObjectStorageSourceProvider provider;
    private final String providerType;
    private final int providerVersion;
    private final ObjectStorageReadRequest readRequest;
    private final int maxRowBytes;
    private final BatchSourceSplitDescriptor batchSourceSplit;

    public ObjectStorageSnapshotSource(
            DataSchema dataSchema, ObjectStorageSourceProvider provider,
            String endpointRef, String credentialRef, String namespace,
            String objectKey, String versionId, long contentLength,
            String contentSha256, int maxRowBytes) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("object storage dataSchema is required");
        }
        ProviderIdentity identity = validateProvider(provider);
        if (maxRowBytes < MIN_ROW_BYTES || maxRowBytes > MAX_ROW_BYTES) {
            throw new IllegalArgumentException(
                    "object storage maxRowBytes is outside its hard limits");
        }
        ObjectStorageReadRequest request = new ObjectStorageReadRequest(
                endpointRef, credentialRef, namespace, objectKey, versionId,
                contentLength, contentSha256);
        this.dataSchema = dataSchema;
        this.provider = provider;
        this.providerType = identity.type;
        this.providerVersion = identity.version;
        this.readRequest = request;
        this.maxRowBytes = maxRowBytes;
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
                    "object storage provider identity changed after plan binding");
        }
        return batchSourceSplit;
    }

    @Override
    public SourceExecutor<?> createSourceExecutor() {
        batchSourceSplit();
        return new ObjectStorageSnapshotInputExecutor(this);
    }

    public ObjectStorageReadRequest readRequest() {
        return readRequest;
    }

    public int maxRowBytes() {
        return maxRowBytes;
    }

    ObjectStorageSourceProvider provider() {
        return provider;
    }

    /** Stateless provider instance; exposed for durable trust verification only. */
    public ObjectStorageSourceProvider sourceProvider() {
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
            ObjectStorageSnapshotSource validated = new ObjectStorageSnapshotSource(
                    dataSchema, provider, readRequest.endpointRef(), readRequest.credentialRef(),
                    readRequest.namespace(), readRequest.objectKey(), readRequest.versionId(),
                    readRequest.contentLength(), readRequest.contentSha256(), maxRowBytes);
            if (!providerType.equals(validated.providerType)
                    || providerVersion != validated.providerVersion
                    || !batchSourceSplit.equals(validated.batchSourceSplit)) {
                throw new IllegalArgumentException(
                        "object storage derived snapshot identity mismatch");
            }
            return validated;
        } catch (RuntimeException invalid) {
            InvalidObjectException rejected = new InvalidObjectException(
                    "invalid serialized ObjectStorageSnapshotSource: " + invalid.getMessage());
            rejected.initCause(invalid);
            throw rejected;
        }
    }

    private static BatchSourceSplitDescriptor descriptor(
            DataSchema dataSchema, ProviderIdentity provider,
            ObjectStorageReadRequest request) {
        String splitId = digest("split", provider.type,
                Integer.toString(provider.version), request.endpointRef(),
                request.namespace(), request.objectKey());
        String snapshotId = digest("snapshot", splitId, request.versionId(),
                Long.toString(request.contentLength()), request.contentSha256(),
                "ROWWEAVE_ROW_TEXT_V1");
        return new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                "rowweave.object-storage/" + provider.type, provider.version,
                splitId, SchemaFingerprint.of(dataSchema), snapshotId,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private static ProviderIdentity validateProvider(
            ObjectStorageSourceProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException("object storage provider is required");
        }
        Class<?> type = provider.getClass();
        if (!Modifier.isPublic(type.getModifiers())) {
            throw new IllegalArgumentException(
                    "object storage provider class must be public");
        }
        try {
            Constructor<?> constructor = type.getConstructor();
            if (!Modifier.isPublic(constructor.getModifiers())) {
                throw new NoSuchMethodException();
            }
        } catch (NoSuchMethodException missing) {
            throw new IllegalArgumentException(
                    "object storage provider requires a public no-arg constructor");
        }
        for (Class<?> current = type; current != null && current != Object.class;
             current = current.getSuperclass()) {
            for (java.lang.reflect.Field field : current.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    throw new IllegalArgumentException(
                            "object storage provider must not declare instance fields");
                }
            }
        }
        String providerType = provider.providerType();
        if (providerType == null
                || !providerType.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")) {
            throw new IllegalArgumentException(
                    "object storage providerType is invalid or exceeds its hard limit");
        }
        int providerVersion = provider.providerVersion();
        if (providerVersion <= 0) {
            throw new IllegalArgumentException(
                    "object storage providerVersion must be positive");
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
        return "ObjectStorageSnapshotSource{" + providerType
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
