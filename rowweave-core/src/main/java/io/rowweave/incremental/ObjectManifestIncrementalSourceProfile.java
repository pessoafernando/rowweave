package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Core-enforced metadata profile for manifest-versioned immutable objects.
 * Listing results and symbolic latest versions are deliberately not modeled.
 */
public final class ObjectManifestIncrementalSourceProfile {
    private static final int CONFIG_MAGIC = 0x52574f43; // RWOC
    private static final int MANIFEST_MAGIC = 0x52574f4d; // RWOM
    private static final int OPEN_MAGIC = 0x52574f4f; // RWOO
    private static final int VERSION = 1;
    private static final int MAX_CONFIG_BYTES = 16 * 1024;

    public enum EntryKind {
        DATA(1), TOMBSTONE(2);
        private final int wireId;
        EntryKind(int wireId) { this.wireId = wireId; }
    }

    private final DecodedConfiguration configuration;
    private final IncrementalSourceProviderContract contract;
    private final IncrementalSourceConfig sourceConfig;

    public ObjectManifestIncrementalSourceProfile(Configuration configuration) {
        if (configuration == null) {
            throw failure("OBJECT_MANIFEST_CONFIG_INVALID");
        }
        this.configuration = decoded(configuration);
        this.contract = builtInContract();
        byte[] config = encodeConfiguration(this.configuration);
        this.sourceConfig = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract), config);
    }

    public IncrementalSourceProviderContract contract() { return contract; }

    public static IncrementalSourceProviderContract builtInContract() {
        return contractValue();
    }

    public IncrementalSourceConfig sourceConfig() { return sourceConfig; }

    public static ObjectManifestIncrementalSourceProfile bind(
            IncrementalSourceConfig sourceConfig) {
        IncrementalSourceProviderContract contract = builtInContract();
        if (sourceConfig == null
                || !contract.providerKey().equals(sourceConfig.providerKey())
                || !MessageDigest.isEqual(
                IncrementalSourceProviderContractCodec.digest(contract),
                sourceConfig.contractDigest())) {
            throw failure("OBJECT_MANIFEST_CONFIG_BINDING_INVALID");
        }
        DecodedConfiguration decoded = decodeConfiguration(
                sourceConfig.configurationBytes());
        if (!MessageDigest.isEqual(sourceConfig.configurationBytes(),
                encodeConfiguration(decoded))) {
            throw failure("OBJECT_MANIFEST_CONFIG_NONCANONICAL");
        }
        return new ObjectManifestIncrementalSourceProfile(decoded);
    }

    private ObjectManifestIncrementalSourceProfile(
            DecodedConfiguration configuration) {
        this.configuration = configuration;
        this.contract = builtInContract();
        byte[] config = encodeConfiguration(configuration);
        this.sourceConfig = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract), config);
    }

    public String endpointRef() { return configuration.endpointRef; }
    public String credentialRef() { return configuration.credentialRef; }
    public String manifestRef() { return configuration.manifestRef; }
    public int maxRowBytes() { return configuration.maxRowBytes; }

    public byte[] canonicalizeConfiguration(IncrementalSourceConfig candidate) {
        if (candidate == null
                || !contract.providerKey().equals(candidate.providerKey())
                || !MessageDigest.isEqual(sourceConfig.contractDigest(),
                candidate.contractDigest())) {
            throw failure("OBJECT_MANIFEST_CONFIG_BINDING_INVALID");
        }
        DecodedConfiguration decoded = decodeConfiguration(
                candidate.configurationBytes());
        byte[] canonical = encodeConfiguration(decoded);
        if (!MessageDigest.isEqual(canonical, candidate.configurationBytes())) {
            throw failure("OBJECT_MANIFEST_CONFIG_NONCANONICAL");
        }
        return canonical;
    }

    public IncrementalCaptureResponse capture(
            IncrementalCaptureRequest request, Manifest manifest) {
        if (request == null || manifest == null) {
            throw failure("OBJECT_MANIFEST_CAPTURE_INVALID");
        }
        IncrementalFrontier target = IncrementalFrontier.manifestGeneration(
                manifest.generation, manifest.manifestDigest);
        if (request.mode() != IncrementalRunMode.SNAPSHOT_V1) {
            if (request.from().kind()
                    != IncrementalFrontier.Kind.MANIFEST_GENERATION_V1
                    || IncrementalFrontierOrder.compareConcrete(
                    request.from(), target) > 0) {
                throw failure("OBJECT_MANIFEST_CAPTURE_ORDER_INVALID");
            }
        }
        return new IncrementalCaptureResponse(
                target, encodeManifest(manifest));
    }

    public IncrementalMaterializationResponse materialize(
            CapturedTargetReceipt receipt, Manifest reopenedManifest,
            List<Entry> entries) {
        if (receipt == null || reopenedManifest == null || entries == null
                || entries.isEmpty()
                || entries.size() > IncrementalMaterializationResponse.MAX_SPLITS
                || receipt.target().kind()
                != IncrementalFrontier.Kind.MANIFEST_GENERATION_V1
                || !MessageDigest.isEqual(receipt.sourceContractDigest(),
                sourceConfig.contractDigest())) {
            throw failure("OBJECT_MANIFEST_MATERIALIZATION_INVALID");
        }
        byte[] manifestIdentity = encodeManifest(reopenedManifest);
        if (!MessageDigest.isEqual(
                receipt.snapshotIdentity(), manifestIdentity)
                || receipt.target().generation()
                != reopenedManifest.generation
                || !MessageDigest.isEqual(receipt.target().manifestDigest(),
                reopenedManifest.manifestDigest)) {
            throw failure("OBJECT_MANIFEST_SNAPSHOT_DRIFT");
        }

        List<Entry> ordered = new ArrayList<>(entries);
        Collections.sort(ordered, Comparator
                .comparing(Entry::namespace)
                .thenComparing(Entry::objectKey)
                .thenComparing(Entry::versionId));
        Set<String> objectKeys = new HashSet<>();
        ByteArrayOutputStream partitionBytes = new ByteArrayOutputStream();
        List<IncrementalMaterializedSplit> splits =
                new ArrayList<>(ordered.size());
        String snapshotId = hex(sha256(manifestIdentity));
        for (Entry entry : ordered) {
            if (entry == null
                    || !objectKeys.add(entry.namespace + '\0' + entry.objectKey)) {
                throw failure("OBJECT_MANIFEST_ENTRY_INVALID");
            }
            writeEntryIdentity(partitionBytes, entry);
            ObjectStorageReadRequest open = new ObjectStorageReadRequest(
                    configuration.endpointRef, configuration.credentialRef,
                    entry.namespace, entry.objectKey, entry.versionId,
                    entry.contentLength, entry.contentSha256);
            byte[] openRequest = encodeOpenRequest(open);
            String splitId = splitId(open, entry.kind);
            BatchSourceSplitDescriptor descriptor =
                    new BatchSourceSplitDescriptor(
                            BatchSourceSplitDescriptor.CURRENT_VERSION,
                            contract.providerKey().connectorType(),
                            contract.providerKey().connectorVersion(), splitId,
                            configuration.schemaFingerprint,
                            snapshotId,
                            BatchSourceSplitDescriptor.ReplayGuarantee
                                    .EXACT_ORDERED_FROM_START);
            splits.add(new IncrementalMaterializedSplit(
                    descriptor, openRequest));
        }
        return new IncrementalMaterializationResponse(
                contract.providerKey(), receipt,
                sha256(partitionBytes.toByteArray()), splits);
    }

    public static ObjectStorageReadRequest decodeOpenRequest(byte[] frame) {
        try {
            DataInputStream input = input(frame, OPEN_MAGIC,
                    IncrementalMaterializedSplit.MAX_OPEN_REQUEST_BYTES,
                    "OBJECT_MANIFEST_OPEN_REQUEST_INVALID");
            ObjectStorageReadRequest decoded = new ObjectStorageReadRequest(
                    readText(input), readText(input), readText(input),
                    readText(input), readText(input), input.readLong(),
                    readText(input));
            requireEof(input, "OBJECT_MANIFEST_OPEN_REQUEST_INVALID");
            if (!MessageDigest.isEqual(frame, encodeOpenRequest(decoded))) {
                throw failure("OBJECT_MANIFEST_OPEN_REQUEST_NONCANONICAL");
            }
            return decoded;
        } catch (IOException invalid) {
            throw failure("OBJECT_MANIFEST_OPEN_REQUEST_INVALID", invalid);
        }
    }

    public void verifyOpenRequest(
            io.rowweave.IncrementalSourceSplit input,
            ObjectStorageReadRequest request) {
        if (input == null || request == null
                || !sourceConfig.equals(input.configuration())
                || !configuration.endpointRef.equals(request.endpointRef())
                || !configuration.credentialRef.equals(request.credentialRef())
                || isLatest(request.versionId())
                || !configuration.schemaFingerprint.equals(
                input.batchSourceSplit().schemaFingerprint())
                || !hex(sha256(input.capturedTarget().snapshotIdentity()))
                .equals(input.batchSourceSplit().snapshotId())) {
            throw failure("OBJECT_MANIFEST_OPEN_BINDING_DRIFT");
        }
        String expected = input.batchSourceSplit().splitId();
        if (!expected.equals(splitId(request, EntryKind.DATA))
                && !expected.equals(splitId(request, EntryKind.TOMBSTONE))) {
            throw failure("OBJECT_MANIFEST_OPEN_BINDING_DRIFT");
        }
    }

    private String splitId(
            ObjectStorageReadRequest request, EntryKind kind) {
        return hex(sha256(concat(
                sourceConfig.configurationBytes(),
                request.namespace().getBytes(StandardCharsets.UTF_8),
                request.objectKey().getBytes(StandardCharsets.UTF_8),
                request.versionId().getBytes(StandardCharsets.UTF_8),
                Long.toString(request.contentLength())
                        .getBytes(StandardCharsets.US_ASCII),
                request.contentSha256().getBytes(StandardCharsets.US_ASCII),
                new byte[]{(byte) kind.wireId})));
    }

    private static IncrementalSourceProviderContract contractValue() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey(
                        "rowweave.object-manifest-incremental", 1),
                new IncrementalSourceCodecDescriptor(
                        "rowweave.object-manifest-config", 1, MAX_CONFIG_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID,
                        1, IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                IncrementalFrontier.Kind.MANIFEST_GENERATION_V1,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                true,
                new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L,
                        1, 60_000L),
                IncrementalSourceProviderContract.DeploymentCapability
                        .CORE_BUILTIN_V1);
    }

    private static DecodedConfiguration decoded(Configuration configuration) {
        return new DecodedConfiguration(
                SchemaFingerprint.of(configuration.dataSchema),
                configuration.endpointRef, configuration.credentialRef,
                configuration.manifestRef, configuration.immutableManifest,
                configuration.exactVersionReads,
                configuration.noOpenTimeListing,
                configuration.maxRowBytes);
    }

    private static byte[] encodeConfiguration(DecodedConfiguration value) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(CONFIG_MAGIC);
            out.writeInt(VERSION);
            writeText(out, value.schemaFingerprint);
            writeText(out, value.endpointRef);
            writeText(out, value.credentialRef);
            writeText(out, value.manifestRef);
            out.writeBoolean(value.immutableManifest);
            out.writeBoolean(value.exactVersionReads);
            out.writeBoolean(value.noOpenTimeListing);
            out.writeInt(value.maxRowBytes);
            out.flush();
            byte[] result = bytes.toByteArray();
            if (result.length > MAX_CONFIG_BYTES) {
                throw failure("OBJECT_MANIFEST_CONFIG_INVALID");
            }
            return result;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static DecodedConfiguration decodeConfiguration(byte[] frame) {
        try {
            DataInputStream input = input(frame, CONFIG_MAGIC,
                    MAX_CONFIG_BYTES, "OBJECT_MANIFEST_CONFIG_INVALID");
            DecodedConfiguration decoded = new DecodedConfiguration(
                    readText(input), readText(input), readText(input),
                    readText(input), input.readBoolean(),
                    input.readBoolean(), input.readBoolean(), input.readInt());
            requireEof(input, "OBJECT_MANIFEST_CONFIG_INVALID");
            return decoded;
        } catch (IOException invalid) {
            throw failure("OBJECT_MANIFEST_CONFIG_INVALID", invalid);
        }
    }

    private static byte[] encodeManifest(Manifest manifest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MANIFEST_MAGIC);
            out.writeInt(VERSION);
            out.writeLong(manifest.generation);
            out.write(manifest.manifestDigest);
            writeText(out, manifest.versionId);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static Manifest decodeManifestIdentity(byte[] frame) {
        try {
            DataInputStream input = input(frame, MANIFEST_MAGIC,
                    CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES,
                    "OBJECT_MANIFEST_IDENTITY_INVALID");
            long generation = input.readLong();
            byte[] digest = new byte[32];
            input.readFully(digest);
            Manifest manifest = new Manifest(
                    generation, digest, readText(input));
            requireEof(input, "OBJECT_MANIFEST_IDENTITY_INVALID");
            if (!MessageDigest.isEqual(frame, encodeManifest(manifest))) {
                throw failure("OBJECT_MANIFEST_IDENTITY_NONCANONICAL");
            }
            return manifest;
        } catch (IOException invalid) {
            throw failure("OBJECT_MANIFEST_IDENTITY_INVALID", invalid);
        }
    }

    private static byte[] encodeOpenRequest(ObjectStorageReadRequest request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(OPEN_MAGIC);
            out.writeInt(VERSION);
            writeText(out, request.endpointRef());
            writeText(out, request.credentialRef());
            writeText(out, request.namespace());
            writeText(out, request.objectKey());
            writeText(out, request.versionId());
            out.writeLong(request.contentLength());
            writeText(out, request.contentSha256());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static DataInputStream input(
            byte[] frame, int magic, int max, String code) throws IOException {
        if (frame == null || frame.length < 8 || frame.length > max) {
            throw failure(code);
        }
        DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame));
        if (input.readInt() != magic || input.readInt() != VERSION) {
            throw failure(code);
        }
        return input;
    }

    private static void writeEntryIdentity(
            ByteArrayOutputStream target, Entry entry) {
        try {
            DataOutputStream out = new DataOutputStream(target);
            writeText(out, entry.namespace);
            writeText(out, entry.objectKey);
            writeText(out, entry.versionId);
            out.writeLong(entry.contentLength);
            writeText(out, entry.contentSha256);
            out.writeInt(entry.kind.wireId);
            out.flush();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void writeText(DataOutputStream out, String text)
            throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        if (input.available() < Integer.BYTES) throw new EOFException();
        int length = input.readInt();
        if (length <= 0 || length > IncrementalMaterializedSplit.MAX_OPEN_REQUEST_BYTES
                || length > input.available()) throw new EOFException();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(bytes,
                value.getBytes(StandardCharsets.UTF_8))) {
            throw failure("OBJECT_MANIFEST_TEXT_INVALID");
        }
        return value;
    }

    private static void requireEof(DataInputStream input, String code)
            throws IOException {
        if (input.read() != -1) throw failure(code);
    }

    private static byte[] concat(byte[]... values) {
        int length = 0;
        for (byte[] value : values) length = Math.addExact(length, value.length);
        byte[] result = new byte[length];
        int offset = 0;
        for (byte[] value : values) {
            System.arraycopy(value, 0, result, offset, value.length);
            offset += value.length;
        }
        return result;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte item : value) {
            result.append(Character.forDigit((item >>> 4) & 0xf, 16));
            result.append(Character.forDigit(item & 0xf, 16));
        }
        return result.toString();
    }

    private static IllegalArgumentException failure(String code) {
        return new IllegalArgumentException(code);
    }

    private static IllegalArgumentException failure(
            String code, Throwable cause) {
        return new IllegalArgumentException(code, cause);
    }

    public static final class Configuration {
        private final DataSchema dataSchema;
        private final String endpointRef;
        private final String credentialRef;
        private final String manifestRef;
        private final boolean immutableManifest;
        private final boolean exactVersionReads;
        private final boolean noOpenTimeListing;
        private final int maxRowBytes;

        public Configuration(
                DataSchema dataSchema, String endpointRef, String credentialRef,
                String manifestRef, boolean immutableManifest,
                boolean exactVersionReads, boolean noOpenTimeListing,
                int maxRowBytes) {
            validateConfiguration(dataSchema == null ? null
                            : SchemaFingerprint.of(dataSchema), endpointRef,
                    credentialRef, manifestRef, immutableManifest,
                    exactVersionReads, noOpenTimeListing, maxRowBytes);
            this.dataSchema = dataSchema;
            this.endpointRef = endpointRef;
            this.credentialRef = credentialRef;
            this.manifestRef = manifestRef;
            this.immutableManifest = immutableManifest;
            this.exactVersionReads = exactVersionReads;
            this.noOpenTimeListing = noOpenTimeListing;
            this.maxRowBytes = maxRowBytes;
        }

        public DataSchema dataSchema() { return dataSchema; }
    }

    public static final class Manifest {
        private final long generation;
        private final byte[] manifestDigest;
        private final String versionId;

        public Manifest(long generation, byte[] manifestDigest,
                        String versionId) {
            if (generation < 0L || manifestDigest == null
                    || manifestDigest.length != 32 || isLatest(versionId)) {
                throw failure("OBJECT_MANIFEST_IDENTITY_INVALID");
            }
            validateObjectRequest("namespace", "manifest", versionId,
                    0L, repeat('0', 64));
            this.generation = generation;
            this.manifestDigest = manifestDigest.clone();
            this.versionId = versionId;
        }

        public long generation() { return generation; }
        public byte[] manifestDigest() { return manifestDigest.clone(); }
        public String versionId() { return versionId; }
    }

    public static final class Entry {
        private final String namespace;
        private final String objectKey;
        private final String versionId;
        private final long contentLength;
        private final String contentSha256;
        private final EntryKind kind;

        public Entry(String namespace, String objectKey, String versionId,
                     long contentLength, String contentSha256,
                     EntryKind kind) {
            if (kind == null || isLatest(versionId)) {
                throw failure("OBJECT_MANIFEST_EXACT_VERSION_REQUIRED");
            }
            validateObjectRequest(namespace, objectKey, versionId,
                    contentLength, contentSha256);
            this.namespace = namespace;
            this.objectKey = objectKey;
            this.versionId = versionId;
            this.contentLength = contentLength;
            this.contentSha256 = contentSha256;
            this.kind = kind;
        }

        public String namespace() { return namespace; }
        public String objectKey() { return objectKey; }
        public String versionId() { return versionId; }
        public long contentLength() { return contentLength; }
        public String contentSha256() { return contentSha256; }
        public EntryKind kind() { return kind; }
    }

    private static final class DecodedConfiguration {
        private final String schemaFingerprint;
        private final String endpointRef;
        private final String credentialRef;
        private final String manifestRef;
        private final boolean immutableManifest;
        private final boolean exactVersionReads;
        private final boolean noOpenTimeListing;
        private final int maxRowBytes;

        private DecodedConfiguration(
                String schemaFingerprint, String endpointRef,
                String credentialRef, String manifestRef,
                boolean immutableManifest, boolean exactVersionReads,
                boolean noOpenTimeListing, int maxRowBytes) {
            validateConfiguration(schemaFingerprint, endpointRef,
                    credentialRef, manifestRef, immutableManifest,
                    exactVersionReads, noOpenTimeListing, maxRowBytes);
            this.schemaFingerprint = schemaFingerprint;
            this.endpointRef = endpointRef;
            this.credentialRef = credentialRef;
            this.manifestRef = manifestRef;
            this.immutableManifest = immutableManifest;
            this.exactVersionReads = exactVersionReads;
            this.noOpenTimeListing = noOpenTimeListing;
            this.maxRowBytes = maxRowBytes;
        }
    }

    private static void validateConfiguration(
            String schemaFingerprint, String endpointRef,
            String credentialRef, String manifestRef,
            boolean immutableManifest, boolean exactVersionReads,
            boolean noOpenTimeListing, int maxRowBytes) {
        if (schemaFingerprint == null
                || !schemaFingerprint.matches("[0-9a-f]{64}")
                || !immutableManifest || !exactVersionReads
                || !noOpenTimeListing
                || maxRowBytes < ObjectStorageSnapshotSource.MIN_ROW_BYTES
                || maxRowBytes > ObjectStorageSnapshotSource.MAX_ROW_BYTES
                || isLatest(manifestRef)) {
            throw failure("OBJECT_MANIFEST_SAFETY_PROOF_REQUIRED");
        }
        new ObjectStorageReadRequest(
                endpointRef, credentialRef, "profile", "manifest",
                manifestRef, 0L, repeat('0', 64));
    }

    private static void validateObjectRequest(
            String namespace, String objectKey, String versionId,
            long contentLength, String contentSha256) {
        new ObjectStorageReadRequest(
                "endpoint", "credential", namespace, objectKey, versionId,
                contentLength, contentSha256);
    }

    private static boolean isLatest(String value) {
        if (value == null) return true;
        String normalized = value.toLowerCase(Locale.ROOT);
        return "latest".equals(normalized) || "current".equals(normalized)
                || "*".equals(normalized);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
