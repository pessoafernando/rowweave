package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotReadRequest;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Core-enforced metadata profile for an immutable JDBC change table or snapshot.
 * It never carries a URL, SQL text, credential, connection, or result row.
 */
public final class JdbcIncrementalSourceProfile {
    private static final int CONFIG_MAGIC = 0x52574a43; // RWJC
    private static final int SNAPSHOT_MAGIC = 0x52574a53; // RWJS
    private static final int OPEN_MAGIC = 0x52574a4f; // RWJO
    private static final int VERSION = 1;
    private static final int MAX_FRAME_BYTES = 16 * 1024;

    public enum SourceKind {
        IMMUTABLE_CHANGE_TABLE(1), IMMUTABLE_SNAPSHOT(2);
        private final int wireId;
        SourceKind(int wireId) { this.wireId = wireId; }
        static SourceKind fromWire(int wireId) {
            for (SourceKind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("JDBC_PROFILE_SOURCE_KIND_INVALID");
        }
    }

    private final DecodedConfiguration configuration;
    private final IncrementalSourceProviderContract contract;
    private final IncrementalSourceConfig sourceConfig;

    private JdbcIncrementalSourceProfile(
            Configuration configuration, IncrementalFrontier.Kind frontierKind) {
        if (configuration == null) throw failure("JDBC_PROFILE_CONFIG_INVALID");
        this.configuration = decoded(configuration);
        this.contract = contract(frontierKind);
        byte[] bytes = encodeConfiguration(this.configuration);
        this.sourceConfig = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract), bytes);
    }

    private JdbcIncrementalSourceProfile(
            DecodedConfiguration configuration,
            IncrementalFrontier.Kind frontierKind) {
        this.configuration = configuration;
        this.contract = contract(frontierKind);
        byte[] bytes = encodeConfiguration(configuration);
        this.sourceConfig = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract), bytes);
    }

    public static JdbcIncrementalSourceProfile sequence(
            Configuration configuration) {
        return new JdbcIncrementalSourceProfile(
                configuration, IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    public static JdbcIncrementalSourceProfile timestampKey(
            Configuration configuration) {
        if (configuration == null || !configuration.stableKey
                || !configuration.noLateLeft) {
            throw failure("JDBC_PROFILE_TIMESTAMP_ORDER_UNSAFE");
        }
        return new JdbcIncrementalSourceProfile(
                configuration, IncrementalFrontier.Kind.TIMESTAMP_KEY_V1);
    }

    public IncrementalSourceProviderContract contract() { return contract; }

    public static IncrementalSourceProviderContract sequenceContract() {
        return contract(IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
    }

    public static IncrementalSourceProviderContract timestampKeyContract() {
        return contract(IncrementalFrontier.Kind.TIMESTAMP_KEY_V1);
    }

    public IncrementalSourceConfig sourceConfig() { return sourceConfig; }

    public static JdbcIncrementalSourceProfile bind(
            IncrementalSourceConfig sourceConfig,
            IncrementalFrontier.Kind frontierKind) {
        IncrementalSourceProviderContract contract = contract(frontierKind);
        if (sourceConfig == null
                || !contract.providerKey().equals(sourceConfig.providerKey())
                || !MessageDigest.isEqual(
                IncrementalSourceProviderContractCodec.digest(contract),
                sourceConfig.contractDigest())) {
            throw failure("JDBC_PROFILE_CONFIG_BINDING_INVALID");
        }
        DecodedConfiguration decoded = decodeConfiguration(
                sourceConfig.configurationBytes());
        if (!MessageDigest.isEqual(sourceConfig.configurationBytes(),
                encodeConfiguration(decoded))) {
            throw failure("JDBC_PROFILE_CONFIG_NONCANONICAL");
        }
        return new JdbcIncrementalSourceProfile(decoded, frontierKind);
    }

    public String endpointRef() { return configuration.endpointRef; }
    public String credentialRef() { return configuration.credentialRef; }
    public String datasetRef() { return configuration.datasetRef; }
    public String queryRef() { return configuration.queryRef; }

    public byte[] canonicalizeConfiguration(IncrementalSourceConfig candidate) {
        if (candidate == null
                || !contract.providerKey().equals(candidate.providerKey())
                || !MessageDigest.isEqual(sourceConfig.contractDigest(),
                candidate.contractDigest())) {
            throw failure("JDBC_PROFILE_CONFIG_BINDING_INVALID");
        }
        DecodedConfiguration decoded = decodeConfiguration(
                candidate.configurationBytes());
        byte[] canonical = encodeConfiguration(decoded);
        if (!MessageDigest.isEqual(canonical, candidate.configurationBytes())) {
            throw failure("JDBC_PROFILE_CONFIG_NONCANONICAL");
        }
        return canonical;
    }

    public IncrementalCaptureResponse capture(
            IncrementalCaptureRequest request, IncrementalFrontier target,
            Snapshot snapshot) {
        if (request == null || target == null || snapshot == null
                || target.kind() != contract.frontierKind()) {
            throw failure("JDBC_PROFILE_CAPTURE_INVALID");
        }
        if (request.mode() != IncrementalRunMode.SNAPSHOT_V1) {
            if (request.from().kind() != target.kind()
                    || IncrementalFrontierOrder.compareConcrete(
                    request.from(), target) > 0) {
                throw failure("JDBC_PROFILE_CAPTURE_ORDER_INVALID");
            }
        }
        return new IncrementalCaptureResponse(
                target, encodeSnapshot(snapshot));
    }

    public IncrementalMaterializationResponse materialize(
            CapturedTargetReceipt receipt, Snapshot reopenedSnapshot,
            List<Partition> partitions) {
        if (receipt == null || reopenedSnapshot == null || partitions == null
                || partitions.isEmpty()
                || partitions.size() > IncrementalMaterializationResponse.MAX_SPLITS
                || receipt.target().kind() != contract.frontierKind()
                || !MessageDigest.isEqual(receipt.sourceContractDigest(),
                sourceConfig.contractDigest())) {
            throw failure("JDBC_PROFILE_MATERIALIZATION_INVALID");
        }
        byte[] snapshotIdentity = encodeSnapshot(reopenedSnapshot);
        if (!MessageDigest.isEqual(
                receipt.snapshotIdentity(), snapshotIdentity)) {
            throw failure("JDBC_PROFILE_SNAPSHOT_DRIFT");
        }

        List<Partition> ordered = new ArrayList<>(partitions);
        Collections.sort(ordered, Comparator.comparing(Partition::partitionRef));
        Set<String> identities = new HashSet<>();
        ByteArrayOutputStream partitionBytes = new ByteArrayOutputStream();
        List<IncrementalMaterializedSplit> splits =
                new ArrayList<>(ordered.size());
        String frozenSnapshot = hex(sha256(snapshotIdentity));
        for (Partition partition : ordered) {
            if (partition == null || !identities.add(partition.partitionRef)) {
                throw failure("JDBC_PROFILE_PARTITION_INVALID");
            }
            writePartitionIdentity(partitionBytes, partition);
            JdbcSnapshotReadRequest open = new JdbcSnapshotReadRequest(
                    configuration.endpointRef, configuration.credentialRef,
                    configuration.datasetRef, configuration.queryRef,
                    partition.partitionRef, reopenedSnapshot.snapshotRef,
                    configuration.queryDigest, partition.partitionDigest,
                    reopenedSnapshot.snapshotId, configuration.fetchSize,
                    configuration.queryTimeoutSeconds,
                    configuration.maxCellBytes);
            byte[] openRequest = encodeOpenRequest(open);
            String splitId = hex(sha256(concat(
                    sourceConfig.configurationBytes(),
                    partition.partitionRef.getBytes(StandardCharsets.UTF_8),
                    partition.partitionDigest.getBytes(StandardCharsets.US_ASCII))));
            BatchSourceSplitDescriptor descriptor =
                    new BatchSourceSplitDescriptor(
                            BatchSourceSplitDescriptor.CURRENT_VERSION,
                            contract.providerKey().connectorType(),
                            contract.providerKey().connectorVersion(), splitId,
                            configuration.schemaFingerprint,
                            frozenSnapshot,
                            BatchSourceSplitDescriptor.ReplayGuarantee
                                    .EXACT_ORDERED_FROM_START);
            splits.add(new IncrementalMaterializedSplit(
                    descriptor, openRequest));
        }
        return new IncrementalMaterializationResponse(
                contract.providerKey(), receipt,
                sha256(partitionBytes.toByteArray()), splits);
    }

    public static JdbcSnapshotReadRequest decodeOpenRequest(byte[] frame) {
        try {
            DataInputStream input = input(frame, OPEN_MAGIC,
                    IncrementalMaterializedSplit.MAX_OPEN_REQUEST_BYTES,
                    "JDBC_PROFILE_OPEN_REQUEST_INVALID");
            JdbcSnapshotReadRequest decoded = new JdbcSnapshotReadRequest(
                    readText(input), readText(input), readText(input),
                    readText(input), readText(input), readText(input),
                    readText(input), readText(input), readText(input),
                    input.readInt(), input.readInt(), input.readInt());
            requireEof(input, "JDBC_PROFILE_OPEN_REQUEST_INVALID");
            if (!MessageDigest.isEqual(frame, encodeOpenRequest(decoded))) {
                throw failure("JDBC_PROFILE_OPEN_REQUEST_NONCANONICAL");
            }
            return decoded;
        } catch (IOException invalid) {
            throw failure("JDBC_PROFILE_OPEN_REQUEST_INVALID", invalid);
        }
    }

    public void verifyOpenRequest(
            io.rowweave.IncrementalSourceSplit input,
            JdbcSnapshotReadRequest request) {
        if (input == null || request == null
                || !sourceConfig.equals(input.configuration())
                || !configuration.endpointRef.equals(request.endpointRef())
                || !configuration.credentialRef.equals(request.credentialRef())
                || !configuration.datasetRef.equals(request.datasetRef())
                || !configuration.queryRef.equals(request.queryRef())
                || !configuration.queryDigest.equals(request.queryDigest())
                || configuration.fetchSize != request.fetchSize()
                || configuration.queryTimeoutSeconds
                != request.queryTimeoutSeconds()
                || configuration.maxCellBytes != request.maxCellBytes()) {
            throw failure("JDBC_PROFILE_OPEN_BINDING_DRIFT");
        }
        Snapshot snapshot = decodeSnapshotIdentity(
                input.capturedTarget().snapshotIdentity());
        String splitId = hex(sha256(concat(
                sourceConfig.configurationBytes(),
                request.partitionRef().getBytes(StandardCharsets.UTF_8),
                request.partitionDigest().getBytes(StandardCharsets.US_ASCII))));
        if (!snapshot.snapshotRef.equals(request.snapshotRef())
                || !snapshot.snapshotId.equals(request.snapshotId())
                || !splitId.equals(input.batchSourceSplit().splitId())
                || !configuration.schemaFingerprint.equals(
                input.batchSourceSplit().schemaFingerprint())
                || !hex(sha256(input.capturedTarget().snapshotIdentity()))
                .equals(input.batchSourceSplit().snapshotId())) {
            throw failure("JDBC_PROFILE_OPEN_BINDING_DRIFT");
        }
    }

    private static IncrementalSourceProviderContract contract(
            IncrementalFrontier.Kind frontierKind) {
        if (frontierKind != IncrementalFrontier.Kind.INT64_SEQUENCE_V1
                && frontierKind != IncrementalFrontier.Kind.TIMESTAMP_KEY_V1) {
            throw failure("JDBC_PROFILE_FRONTIER_INVALID");
        }
        String suffix = frontierKind == IncrementalFrontier.Kind.INT64_SEQUENCE_V1
                ? "sequence" : "timestamp-key";
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey(
                        "rowweave.jdbc-incremental-" + suffix, 1),
                new IncrementalSourceCodecDescriptor(
                        "rowweave.jdbc-incremental-config", 1, MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID,
                        1, IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES, frontierKind,
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

    private static DecodedConfiguration decoded(Configuration value) {
        return new DecodedConfiguration(
                SchemaFingerprint.of(value.dataSchema), value.endpointRef,
                value.credentialRef, value.datasetRef, value.queryRef,
                value.queryDigest, value.sourceKind, value.immutable,
                value.reopenableSnapshot, value.stableKey, value.noLateLeft,
                value.fetchSize, value.queryTimeoutSeconds,
                value.maxCellBytes);
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
            writeText(out, value.datasetRef);
            writeText(out, value.queryRef);
            writeText(out, value.queryDigest);
            out.writeInt(value.sourceKind.wireId);
            out.writeBoolean(value.immutable);
            out.writeBoolean(value.reopenableSnapshot);
            out.writeBoolean(value.stableKey);
            out.writeBoolean(value.noLateLeft);
            out.writeInt(value.fetchSize);
            out.writeInt(value.queryTimeoutSeconds);
            out.writeInt(value.maxCellBytes);
            out.flush();
            byte[] result = bytes.toByteArray();
            if (result.length > MAX_FRAME_BYTES) {
                throw failure("JDBC_PROFILE_CONFIG_INVALID");
            }
            return result;
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static DecodedConfiguration decodeConfiguration(byte[] frame) {
        try {
            DataInputStream input = input(frame, CONFIG_MAGIC, MAX_FRAME_BYTES,
                    "JDBC_PROFILE_CONFIG_INVALID");
            DecodedConfiguration decoded = new DecodedConfiguration(
                    readText(input), readText(input), readText(input),
                    readText(input), readText(input), readText(input),
                    SourceKind.fromWire(input.readInt()),
                    input.readBoolean(), input.readBoolean(),
                    input.readBoolean(), input.readBoolean(),
                    input.readInt(), input.readInt(), input.readInt());
            requireEof(input, "JDBC_PROFILE_CONFIG_INVALID");
            return decoded;
        } catch (IOException invalid) {
            throw failure("JDBC_PROFILE_CONFIG_INVALID", invalid);
        }
    }

    private static byte[] encodeSnapshot(Snapshot snapshot) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(SNAPSHOT_MAGIC);
            out.writeInt(VERSION);
            writeText(out, snapshot.snapshotRef);
            writeText(out, snapshot.snapshotId);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static Snapshot decodeSnapshotIdentity(byte[] frame) {
        try {
            DataInputStream input = input(frame, SNAPSHOT_MAGIC,
                    CapturedTargetReceipt.MAX_SNAPSHOT_IDENTITY_BYTES,
                    "JDBC_PROFILE_SNAPSHOT_INVALID");
            Snapshot snapshot = new Snapshot(readText(input), readText(input));
            requireEof(input, "JDBC_PROFILE_SNAPSHOT_INVALID");
            if (!MessageDigest.isEqual(frame, encodeSnapshot(snapshot))) {
                throw failure("JDBC_PROFILE_SNAPSHOT_NONCANONICAL");
            }
            return snapshot;
        } catch (IOException invalid) {
            throw failure("JDBC_PROFILE_SNAPSHOT_INVALID", invalid);
        }
    }

    private static byte[] encodeOpenRequest(JdbcSnapshotReadRequest request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(OPEN_MAGIC);
            out.writeInt(VERSION);
            writeText(out, request.endpointRef());
            writeText(out, request.credentialRef());
            writeText(out, request.datasetRef());
            writeText(out, request.queryRef());
            writeText(out, request.partitionRef());
            writeText(out, request.snapshotRef());
            writeText(out, request.queryDigest());
            writeText(out, request.partitionDigest());
            writeText(out, request.snapshotId());
            out.writeInt(request.fetchSize());
            out.writeInt(request.queryTimeoutSeconds());
            out.writeInt(request.maxCellBytes());
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

    private static void writeText(DataOutputStream out, String text)
            throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        if (input.available() < Integer.BYTES) throw new EOFException();
        int length = input.readInt();
        if (length <= 0 || length > MAX_FRAME_BYTES
                || length > input.available()) throw new EOFException();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        String value = new String(bytes, StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(bytes,
                value.getBytes(StandardCharsets.UTF_8))) {
            throw failure("JDBC_PROFILE_TEXT_INVALID");
        }
        return value;
    }

    private static void requireEof(DataInputStream input, String code)
            throws IOException {
        if (input.read() != -1) throw failure(code);
    }

    private static void writePartitionIdentity(
            ByteArrayOutputStream target, Partition partition) {
        try {
            DataOutputStream out = new DataOutputStream(target);
            writeText(out, partition.partitionRef);
            writeText(out, partition.partitionDigest);
            out.flush();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
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
        private final String datasetRef;
        private final String queryRef;
        private final String queryDigest;
        private final SourceKind sourceKind;
        private final boolean immutable;
        private final boolean reopenableSnapshot;
        private final boolean stableKey;
        private final boolean noLateLeft;
        private final int fetchSize;
        private final int queryTimeoutSeconds;
        private final int maxCellBytes;

        public Configuration(
                DataSchema dataSchema, String endpointRef, String credentialRef,
                String datasetRef, String queryRef, String queryDigest,
                SourceKind sourceKind, boolean immutable,
                boolean reopenableSnapshot, boolean stableKey,
                boolean noLateLeft, int fetchSize,
                int queryTimeoutSeconds, int maxCellBytes) {
            if (dataSchema == null || sourceKind == null || !immutable
                    || !reopenableSnapshot || !stableKey || !noLateLeft) {
                throw failure("JDBC_PROFILE_SAFETY_PROOF_REQUIRED");
            }
            new JdbcSnapshotReadRequest(
                    endpointRef, credentialRef, datasetRef, queryRef,
                    "profile-validation", repeat('0', 64), queryDigest,
                    repeat('0', 64), repeat('0', 64), fetchSize,
                    queryTimeoutSeconds, maxCellBytes);
            this.dataSchema = dataSchema;
            this.endpointRef = endpointRef;
            this.credentialRef = credentialRef;
            this.datasetRef = datasetRef;
            this.queryRef = queryRef;
            this.queryDigest = queryDigest;
            this.sourceKind = sourceKind;
            this.immutable = immutable;
            this.reopenableSnapshot = reopenableSnapshot;
            this.stableKey = stableKey;
            this.noLateLeft = noLateLeft;
            this.fetchSize = fetchSize;
            this.queryTimeoutSeconds = queryTimeoutSeconds;
            this.maxCellBytes = maxCellBytes;
        }

        public DataSchema dataSchema() { return dataSchema; }
    }

    public static final class Snapshot {
        private final String snapshotRef;
        private final String snapshotId;

        public Snapshot(String snapshotRef, String snapshotId) {
            new JdbcSnapshotReadRequest(
                    "endpoint", "credential", "dataset", "query",
                    "partition", snapshotRef, repeat('0', 64),
                    repeat('0', 64), snapshotId, 1, 1,
                    JdbcSnapshotSource.MIN_CELL_BYTES);
            this.snapshotRef = snapshotRef;
            this.snapshotId = snapshotId;
        }

        public String snapshotRef() { return snapshotRef; }
        public String snapshotId() { return snapshotId; }
    }

    public static final class Partition {
        private final String partitionRef;
        private final String partitionDigest;

        public Partition(String partitionRef, String partitionDigest) {
            new JdbcSnapshotReadRequest(
                    "endpoint", "credential", "dataset", "query",
                    partitionRef, "snapshot", repeat('0', 64),
                    partitionDigest, repeat('0', 64), 1, 1,
                    JdbcSnapshotSource.MIN_CELL_BYTES);
            this.partitionRef = partitionRef;
            this.partitionDigest = partitionDigest;
        }

        public String partitionRef() { return partitionRef; }
        public String partitionDigest() { return partitionDigest; }
    }

    private static final class DecodedConfiguration {
        private final String schemaFingerprint;
        private final String endpointRef;
        private final String credentialRef;
        private final String datasetRef;
        private final String queryRef;
        private final String queryDigest;
        private final SourceKind sourceKind;
        private final boolean immutable;
        private final boolean reopenableSnapshot;
        private final boolean stableKey;
        private final boolean noLateLeft;
        private final int fetchSize;
        private final int queryTimeoutSeconds;
        private final int maxCellBytes;

        private DecodedConfiguration(
                String schemaFingerprint, String endpointRef,
                String credentialRef, String datasetRef, String queryRef,
                String queryDigest, SourceKind sourceKind, boolean immutable,
                boolean reopenableSnapshot, boolean stableKey,
                boolean noLateLeft, int fetchSize,
                int queryTimeoutSeconds, int maxCellBytes) {
            if (schemaFingerprint == null
                    || !schemaFingerprint.matches("[0-9a-f]{64}")
                    || sourceKind == null || !immutable
                    || !reopenableSnapshot || !stableKey || !noLateLeft) {
                throw failure("JDBC_PROFILE_SAFETY_PROOF_REQUIRED");
            }
            new JdbcSnapshotReadRequest(
                    endpointRef, credentialRef, datasetRef, queryRef,
                    "profile-validation", repeat('0', 64), queryDigest,
                    repeat('0', 64), repeat('0', 64), fetchSize,
                    queryTimeoutSeconds, maxCellBytes);
            this.schemaFingerprint = schemaFingerprint;
            this.endpointRef = endpointRef;
            this.credentialRef = credentialRef;
            this.datasetRef = datasetRef;
            this.queryRef = queryRef;
            this.queryDigest = queryDigest;
            this.sourceKind = sourceKind;
            this.immutable = immutable;
            this.reopenableSnapshot = reopenableSnapshot;
            this.stableKey = stableKey;
            this.noLateLeft = noLateLeft;
            this.fetchSize = fetchSize;
            this.queryTimeoutSeconds = queryTimeoutSeconds;
            this.maxCellBytes = maxCellBytes;
        }
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
