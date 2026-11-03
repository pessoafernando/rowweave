package io.rowweave.run;

import io.rowweave.plan.StageExecutionTelemetry;
import io.rowweave.plan.StageExecutionTelemetryCodec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** bounded canonical root/shard/token codec with SHA-256 envelopes. */
public final class StageAssignmentManifestCodec {
    public static final int MAX_ROOT_FRAME_BYTES = 8 * 1024;
    public static final int MAX_SHARD_FRAME_BYTES = 128 * 1024;
    /** Includes up to 64 maximum 4 KiB prepared locators and stays below ZK 1 MiB. */
    public static final int MAX_TERMINAL_SHARD_FRAME_BYTES = 768 * 1024;
    public static final int MAX_TOKEN_FRAME_BYTES = 2 * 1024;
    public static final int MAX_TERMINAL_RECEIPT_FRAME_BYTES = 4 * 1024;

    private static final int MAGIC = 0x5257414d; // RWAM
    private static final int WRITE_VERSION = 3;
    // Version 4 remains exclusive to terminal shards. Version 5 is exclusive
    // to roots and adds the trusted quality binding without changing shards.
    private static final int TERMINAL_SHARD_WRITE_VERSION = 5;
    private static final int ROOT_WRITE_VERSION = 5;
    private static final int TERMINAL_TELEMETRY_VERSION = 4;
    private static final int TERMINAL_PREPARED_VERSION = 5;
    private static final int MIN_READ_VERSION = 1;
    private static final int ROOT = 1;
    private static final int SHARD = 2;
    private static final int TOKEN = 3;
    private static final int ACCEPTANCE_SHARD = 4;
    private static final int TERMINAL_SHARD = 5;
    private static final int TERMINAL_RECEIPT = 6;
    private static final int HEADER_BYTES = 12;
    private static final int CHECKSUM_BYTES = 32;

    private StageAssignmentManifestCodec() {
    }

    public static byte[] encodeRoot(StageAssignmentManifest manifest) {
        return encodeRoot(manifest, 1L);
    }

    public static byte[] encodeRoot(StageAssignmentManifest manifest,
                                    long acceptanceGeneration) {
        requireManifest(manifest);
        if (acceptanceGeneration <= 0L) {
            throw new IllegalArgumentException("acceptanceGeneration must be positive");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeIdentity(out, manifest.fence(), manifest.artifactId(), manifest.generation());
            out.writeByte(manifest.outputProfile().wireId());
            if (manifest.hasQualityGateBinding()) {
                out.writeBoolean(true);
                writeQualityGateBinding(out, manifest.qualityGateBinding());
            }
            out.writeLong(acceptanceGeneration);
            out.writeInt(manifest.assignmentCount());
            out.writeInt(manifest.shardCount());
            for (int i = 0; i < manifest.shardCount(); i++) {
                out.write(sha256(shardIdentityBody(manifest, i)));
            }
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(manifest.hasQualityGateBinding()
                        ? ROOT_WRITE_VERSION : WRITE_VERSION,
                ROOT, bytes.toByteArray(),
                MAX_ROOT_FRAME_BYTES);
    }

    public static byte[] encodeShard(StageAssignmentManifest manifest, int shardIndex) {
        requireManifest(manifest);
        return frame(SHARD, shardIdentityBody(manifest, shardIndex), MAX_SHARD_FRAME_BYTES);
    }

    /** Decode the bounded root metadata needed by quality-specific authority reads. */
    public static QualityRootSnapshot decodeQualityRoot(byte[] rootBytes) {
        DecodedRoot root = decodeRoot(rootBytes);
        if (root.qualityGateBinding == null
                || root.outputProfile
                != AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1) {
            throw new IllegalArgumentException(
                    "assignment root is not a quality manifest root");
        }
        for (QualityGateAssignmentBinding.MemberBinding member
                : root.qualityGateBinding.members()) {
            if (member.physicalOrdinal() >= root.assignmentCount) {
                throw new IllegalArgumentException(
                        "quality binding ordinal is outside assignment root");
            }
        }
        return new QualityRootSnapshot(
                root.fence, root.artifactId, root.generation,
                root.assignmentCount, root.shardCount,
                root.qualityGateBinding);
    }

    /** Reads only the bounded root frame to select the quality-specific authority path. */
    public static boolean hasQualityGateBinding(byte[] rootBytes) {
        return decodeRoot(rootBytes).qualityGateBinding != null;
    }

    private static byte[] shardIdentityBody(StageAssignmentManifest manifest, int shardIndex) {
        List<StageAssignmentRecord> records = manifest.shardAssignments(shardIndex);
        return shardIdentityBody(manifest.fence(), manifest.artifactId(), manifest.generation(),
                manifest.assignmentCount(), manifest.shardCount(), shardIndex, records,
                WRITE_VERSION);
    }

    private static byte[] shardIdentityBody(RunAttemptFence fence, String artifactId,
                                            long generation, int assignmentCount,
                                            int shardCount, int shardIndex,
                                            List<StageAssignmentRecord> records,
                                            int version) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(16 * 1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeIdentity(out, fence, artifactId, generation);
            out.writeInt(assignmentCount);
            out.writeInt(shardCount);
            out.writeInt(shardIndex);
            out.writeInt(records.size());
            for (StageAssignmentRecord record : records) writeRecord(out, record, version);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    public static StageAssignmentManifest decode(byte[] rootBytes, List<byte[]> shardBytes) {
        DecodedRoot root = decodeRoot(rootBytes);
        if (shardBytes == null || shardBytes.size() != root.shardCount) {
            throw new IllegalArgumentException("manifest shard count mismatch");
        }
        List<StageAssignmentRecord> assignments = new ArrayList<>(root.assignmentCount);
        for (int i = 0; i < root.shardCount; i++) {
            byte[] encoded = shardBytes.get(i);
            DecodedShard shard = decodeShard(encoded);
            if (shard.shardIndex != i
                    || shard.version != expectedShardVersion(root.version)
                    || shard.assignmentCount != root.assignmentCount
                    || shard.shardCount != root.shardCount
                    || shard.generation != root.generation
                    || !shard.fence.equals(root.fence)
                    || !shard.artifactId.equals(root.artifactId)) {
                throw new IllegalArgumentException("manifest shard identity mismatch at " + i);
            }
            byte[] immutableDigest = sha256(shardIdentityBody(
                    shard.fence, shard.artifactId, shard.generation, shard.assignmentCount,
                    shard.shardCount, shard.shardIndex, shard.records,
                    shard.version));
            if (!MessageDigest.isEqual(root.shardDigests[i], immutableDigest)) {
                throw new IllegalArgumentException("manifest shard digest mismatch at " + i);
            }
            assignments.addAll(shard.records);
        }
        if (assignments.size() != root.assignmentCount) {
            throw new IllegalArgumentException("manifest assignment count mismatch");
        }
        return new StageAssignmentManifest(
                root.fence, root.artifactId, root.generation,
                root.outputProfile, assignments, root.qualityGateBinding);
    }

    public static byte[] encodeAcceptanceShard(StageAssignmentSnapshot snapshot,
                                               int shardIndex) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        StageAssignmentManifest manifest = snapshot.manifest();
        List<StageAssignmentRecord> records = manifest.shardAssignments(shardIndex);
        int firstOrdinal = shardIndex * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        List<StageAssignmentAcceptance> acceptances = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            acceptances.add(snapshot.acceptance(firstOrdinal + i));
        }
        return encodeAcceptanceShard(manifest.fence(), manifest.artifactId(),
                manifest.generation(), manifest.assignmentCount(), manifest.shardCount(),
                shardIndex, acceptances);
    }

    /** Decode and authenticate one immutable/acceptance shard against the published root. */
    public static ShardSnapshot decodeShardSnapshot(
            byte[] rootBytes, byte[] assignmentShardBytes,
            byte[] acceptanceShardBytes, int expectedShardIndex) {
        return decodeShardSnapshot(rootBytes, assignmentShardBytes,
                acceptanceShardBytes, null, expectedShardIndex);
    }

    /** Decode and authenticate immutable, acceptance and optional terminal evidence. */
    public static ShardSnapshot decodeShardSnapshot(
            byte[] rootBytes, byte[] assignmentShardBytes,
            byte[] acceptanceShardBytes, byte[] terminalShardBytes,
            int expectedShardIndex) {
        DecodedRoot root = decodeRoot(rootBytes);
        if (expectedShardIndex < 0 || expectedShardIndex >= root.shardCount) {
            throw new IllegalArgumentException("expected shard index out of bounds");
        }
        DecodedShard assignment = decodeShard(assignmentShardBytes);
        DecodedAcceptanceShard acceptance =
                decodeAcceptanceShard(acceptanceShardBytes);
        DecodedTerminalShard terminal = terminalShardBytes == null
                ? null : decodeTerminalShard(terminalShardBytes);
        requireShardIdentity(root, assignment, expectedShardIndex, "assignment");
        requireShardIdentity(root, acceptance, expectedShardIndex, "acceptance");
        if (terminal != null) {
            requireShardIdentity(root, terminal, expectedShardIndex, "terminal");
        }
        byte[] immutableDigest = sha256(shardIdentityBody(
                assignment.fence, assignment.artifactId, assignment.generation,
                assignment.assignmentCount, assignment.shardCount,
                assignment.shardIndex, assignment.records, assignment.version));
        if (!MessageDigest.isEqual(root.shardDigests[expectedShardIndex], immutableDigest)) {
            throw new IllegalArgumentException(
                    "manifest shard digest mismatch at " + expectedShardIndex);
        }
        if (assignment.records.size() != acceptance.acceptances.size()) {
            throw new IllegalArgumentException("assignment/acceptance record count mismatch");
        }
        if (terminal != null
                && assignment.records.size() != terminal.terminals.size()) {
            throw new IllegalArgumentException(
                    "assignment/terminal record count mismatch");
        }
        List<StageTerminalReceipt> terminals = terminal == null
                ? Collections.nCopies(assignment.records.size(),
                (StageTerminalReceipt) null)
                : terminal.terminals;
        List<PreparedSinkReceipt> preparedSinks = terminal == null
                ? Collections.nCopies(assignment.records.size(),
                (PreparedSinkReceipt) null)
                : terminal.preparedSinks;
        int firstOrdinal = expectedShardIndex
                * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        for (int i = 0; i < assignment.records.size(); i++) {
            StageAssignmentRecord record = assignment.records.get(i);
            if (record.physicalOrdinal() != firstOrdinal + i) {
                throw new IllegalArgumentException(
                        "non-canonical physical ordinal in shard");
            }
            StageAssignmentAcceptance receipt = acceptance.acceptances.get(i);
            if (receipt != null) requireAcceptanceIdentity(receipt, record, root);
            StageTerminalReceipt terminalReceipt = terminals.get(i);
            if (terminalReceipt != null) {
                requireTerminalIdentity(terminalReceipt, receipt, record, root);
            }
            PreparedSinkReceipt prepared = preparedSinks.get(i);
            if (prepared != null) {
                requirePreparedIdentity(prepared, terminalReceipt, receipt, record, root);
            }
        }
        return new ShardSnapshot(root.fence, root.artifactId, root.generation,
                root.acceptanceGeneration, root.assignmentCount, root.shardCount,
                expectedShardIndex, assignment.records, acceptance.acceptances,
                terminals, preparedSinks);
    }

    /** Canonically install one previously absent receipt without materializing other shards. */
    public static byte[] encodeAcceptanceShard(
            ShardSnapshot snapshot, StageAssignmentAcceptance acceptance) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        if (acceptance == null) throw new IllegalArgumentException("acceptance is required");
        int ordinal = acceptance.token().physicalOrdinal();
        int offset = snapshot.offset(ordinal);
        if (snapshot.acceptances.get(offset) != null) {
            throw new IllegalArgumentException("assignment is already accepted");
        }
        DecodedRoot root = new DecodedRoot(WRITE_VERSION, snapshot.fence, snapshot.artifactId,
                snapshot.generation, AttemptOutputProfile.UNKNOWN_LEGACY, null,
                snapshot.acceptanceGeneration,
                snapshot.assignmentCount, snapshot.shardCount,
                new byte[snapshot.shardCount][]);
        requireAcceptanceIdentity(acceptance, snapshot.records.get(offset), root);
        List<StageAssignmentAcceptance> updated =
                new ArrayList<>(snapshot.acceptances);
        updated.set(offset, acceptance);
        return encodeAcceptanceShard(snapshot.fence, snapshot.artifactId,
                snapshot.generation, snapshot.assignmentCount, snapshot.shardCount,
                snapshot.shardIndex, updated);
    }

    public static byte[] encodeTerminalShard(StageAssignmentSnapshot snapshot,
                                             int shardIndex) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        StageAssignmentManifest manifest = snapshot.manifest();
        List<StageAssignmentRecord> records = manifest.shardAssignments(shardIndex);
        int firstOrdinal = shardIndex * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        List<StageTerminalReceipt> terminals = new ArrayList<>(records.size());
        List<PreparedSinkReceipt> preparedSinks = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            terminals.add(snapshot.terminal(firstOrdinal + i));
            preparedSinks.add(snapshot.preparedSink(firstOrdinal + i));
        }
        return encodeTerminalShard(manifest.fence(), manifest.artifactId(),
                manifest.generation(), manifest.assignmentCount(), manifest.shardCount(),
                shardIndex, terminals, preparedSinks);
    }

    /** Canonically install one previously absent terminal in a decoded bounded shard. */
    public static byte[] encodeTerminalShard(
            ShardSnapshot snapshot, StageTerminalReceipt terminal) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        if (terminal == null) throw new IllegalArgumentException("terminal is required");
        int ordinal = terminal.token().physicalOrdinal();
        int offset = snapshot.offset(ordinal);
        if (snapshot.terminals.get(offset) != null) {
            throw new IllegalArgumentException("assignment is already terminal");
        }
        DecodedRoot root = new DecodedRoot(WRITE_VERSION, snapshot.fence, snapshot.artifactId,
                snapshot.generation, AttemptOutputProfile.UNKNOWN_LEGACY, null,
                snapshot.acceptanceGeneration,
                snapshot.assignmentCount, snapshot.shardCount,
                new byte[snapshot.shardCount][]);
        requireTerminalIdentity(terminal, snapshot.acceptances.get(offset),
                snapshot.records.get(offset), root);
        List<StageTerminalReceipt> updated = new ArrayList<>(snapshot.terminals);
        updated.set(offset, terminal);
        return encodeTerminalShard(snapshot.fence, snapshot.artifactId,
                snapshot.generation, snapshot.assignmentCount, snapshot.shardCount,
                snapshot.shardIndex, updated, snapshot.preparedSinks);
    }

    /** Canonically install one prepared locator into its bounded terminal-evidence shard. */
    public static byte[] encodePreparedSinkShard(
            ShardSnapshot snapshot, PreparedSinkReceipt prepared) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        if (prepared == null) throw new IllegalArgumentException("prepared is required");
        int ordinal = prepared.token().physicalOrdinal();
        int offset = snapshot.offset(ordinal);
        if (snapshot.preparedSinks.get(offset) != null) {
            throw new IllegalArgumentException("prepared Sink is already recorded");
        }
        DecodedRoot root = new DecodedRoot(WRITE_VERSION, snapshot.fence,
                snapshot.artifactId, snapshot.generation,
                AttemptOutputProfile.UNKNOWN_LEGACY, null,
                snapshot.acceptanceGeneration, snapshot.assignmentCount,
                snapshot.shardCount, new byte[snapshot.shardCount][]);
        requirePreparedIdentity(prepared, snapshot.terminals.get(offset),
                snapshot.acceptances.get(offset), snapshot.records.get(offset), root);
        List<PreparedSinkReceipt> updated =
                new ArrayList<>(snapshot.preparedSinks);
        updated.set(offset, prepared);
        return encodeTerminalShard(snapshot.fence, snapshot.artifactId,
                snapshot.generation, snapshot.assignmentCount, snapshot.shardCount,
                snapshot.shardIndex, snapshot.terminals, updated);
    }

    private static byte[] encodeAcceptanceShard(
            RunAttemptFence fence, String artifactId, long generation,
            int assignmentCount, int shardCount, int shardIndex,
            List<StageAssignmentAcceptance> acceptances) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(16 * 1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeIdentity(out, fence, artifactId, generation);
            out.writeInt(assignmentCount);
            out.writeInt(shardCount);
            out.writeInt(shardIndex);
            out.writeInt(acceptances.size());
            for (StageAssignmentAcceptance acceptance : acceptances) {
                out.writeBoolean(acceptance != null);
                if (acceptance != null) {
                    byte[] token = encodeToken(acceptance.token());
                    out.writeInt(token.length);
                    out.write(token);
                    out.write(acceptance.commandDigest());
                    out.writeLong(acceptance.acceptedAtMillis());
                }
            }
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(ACCEPTANCE_SHARD, bytes.toByteArray(), MAX_SHARD_FRAME_BYTES);
    }

    private static byte[] encodeTerminalShard(
            RunAttemptFence fence, String artifactId, long generation,
            int assignmentCount, int shardCount, int shardIndex,
            List<StageTerminalReceipt> terminals,
            List<PreparedSinkReceipt> preparedSinks) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(16 * 1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeIdentity(out, fence, artifactId, generation);
            out.writeInt(assignmentCount);
            out.writeInt(shardCount);
            out.writeInt(shardIndex);
            out.writeInt(terminals.size());
            for (StageTerminalReceipt terminal : terminals) {
                out.writeBoolean(terminal != null);
                if (terminal != null) {
                    writeTerminalReceipt(out, terminal, true);
                }
            }
            for (PreparedSinkReceipt prepared : preparedSinks) {
                out.writeBoolean(prepared != null);
                if (prepared != null) {
                    byte[] frame = PreparedSinkReceiptCodec.encode(prepared);
                    out.writeInt(frame.length);
                    out.write(frame);
                }
            }
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(TERMINAL_SHARD_WRITE_VERSION, TERMINAL_SHARD,
                bytes.toByteArray(), MAX_TERMINAL_SHARD_FRAME_BYTES);
    }

    private static void writeTerminalReceipt(DataOutputStream out,
                                             StageTerminalReceipt terminal,
                                             boolean includeTelemetry) throws IOException {
        byte[] token = encodeToken(terminal.token());
        out.writeInt(token.length);
        out.write(token);
        out.write(terminal.commandDigest());
        out.writeByte(terminalOutcomeId(terminal.outcome()));
        out.writeLong(terminal.inputRows());
        out.writeLong(terminal.outputRows());
        RunWire.writeNullableText(out, terminal.failureCategory());
        byte[] failureDigest = terminal.failureDigest();
        out.writeBoolean(failureDigest != null);
        if (failureDigest != null) out.write(failureDigest);
        out.writeLong(terminal.completedAtMillis());
        if (includeTelemetry) {
            byte[] telemetry = StageExecutionTelemetryCodec.encode(terminal.telemetry());
            out.writeInt(telemetry.length);
            out.write(telemetry);
        }
    }

    private static StageTerminalReceipt readTerminalReceipt(RunWire.Reader body,
                                                             String path,
                                                             boolean hasTelemetry) {
        int tokenLength = body.readInt(path + ".tokenLength");
        if (tokenLength < 1 || tokenLength > MAX_TOKEN_FRAME_BYTES) {
            throw new IllegalArgumentException("terminal token length out of bounds");
        }
        StageAssignmentToken token = decodeToken(body.readFixed(
                tokenLength, path + ".token"));
        byte[] commandDigest = body.readFixed(CHECKSUM_BYTES, path + ".commandDigest");
        StageTerminalOutcome outcome = terminalOutcome(
                body.readUnsignedByte(path + ".outcome"));
        long inputRows = body.readLong(path + ".inputRows");
        long outputRows = body.readLong(path + ".outputRows");
        String failureCategory = body.readNullableText(
                path + ".failureCategory", RunText.MAX_REASON_CODE_BYTES);
        byte[] failureDigest = body.readBoolean(path + ".hasFailureDigest")
                ? body.readFixed(CHECKSUM_BYTES, path + ".failureDigest") : null;
        long completedAt = body.readLong(path + ".completedAt");
        StageExecutionTelemetry telemetry = StageExecutionTelemetry.unavailable();
        if (hasTelemetry) {
            int telemetryLength = body.readInt(path + ".telemetryLength");
            if (telemetryLength < 1
                    || telemetryLength > StageExecutionTelemetryCodec.MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("terminal telemetry length out of bounds");
            }
            telemetry = StageExecutionTelemetryCodec.decode(body.readFixed(
                    telemetryLength, path + ".telemetry"));
        }
        return StageTerminalReceipt.decoded(token, commandDigest, outcome,
                inputRows, outputRows, failureCategory, failureDigest, completedAt,
                telemetry);
    }

    private static void requireShardIdentity(DecodedRoot root, Identity shard,
                                             int expectedShardIndex, String kind) {
        int assignmentCount;
        int shardCount;
        int shardIndex;
        if (shard instanceof DecodedShard) {
            DecodedShard decoded = (DecodedShard) shard;
            if (decoded.version != expectedShardVersion(root.version)) {
                throw new IllegalArgumentException(kind + " shard version mismatch at "
                        + expectedShardIndex);
            }
            assignmentCount = decoded.assignmentCount;
            shardCount = decoded.shardCount;
            shardIndex = decoded.shardIndex;
        } else if (shard instanceof DecodedAcceptanceShard) {
            DecodedAcceptanceShard decoded = (DecodedAcceptanceShard) shard;
            assignmentCount = decoded.assignmentCount;
            shardCount = decoded.shardCount;
            shardIndex = decoded.shardIndex;
        } else {
            DecodedTerminalShard decoded = (DecodedTerminalShard) shard;
            assignmentCount = decoded.assignmentCount;
            shardCount = decoded.shardCount;
            shardIndex = decoded.shardIndex;
        }
        if (shardIndex != expectedShardIndex
                || assignmentCount != root.assignmentCount
                || shardCount != root.shardCount
                || shard.generation != root.generation
                || !shard.fence.equals(root.fence)
                || !shard.artifactId.equals(root.artifactId)) {
            throw new IllegalArgumentException(kind + " shard identity mismatch at "
                    + expectedShardIndex);
        }
    }

    private static void requireAcceptanceIdentity(
            StageAssignmentAcceptance acceptance, StageAssignmentRecord record,
            DecodedRoot root) {
        StageAssignmentToken token = acceptance.token();
        if (!token.fence().equals(root.fence)
                || token.manifestGeneration() != root.generation
                || token.physicalOrdinal() != record.physicalOrdinal()
                || !token.stageExecutionId().equals(record.stageExecutionId())
                || token.runtimeStageId() != record.runtimeStageId()
                || !token.workerId().equals(record.workerId())
                || !token.workerIncarnationId().equals(record.workerIncarnationId())) {
            throw new IllegalArgumentException("acceptance identity mismatch at "
                    + record.physicalOrdinal());
        }
        if (root.version >= 2) {
            if (!token.hasAssignmentRecordDigest()
                    || !MessageDigest.isEqual(
                    token.assignmentRecordDigest(), record.canonicalDigest())) {
                throw new IllegalArgumentException("acceptance record digest mismatch at "
                        + record.physicalOrdinal());
            }
        } else if (token.hasAssignmentRecordDigest()) {
            throw new IllegalArgumentException("v1 acceptance carries a v2 record digest");
        }
    }

    private static void requireTerminalIdentity(
            StageTerminalReceipt terminal, StageAssignmentAcceptance acceptance,
            StageAssignmentRecord record, DecodedRoot root) {
        if (acceptance == null) {
            throw new IllegalArgumentException("terminal receipt requires acceptance at "
                    + record.physicalOrdinal());
        }
        requireAcceptanceIdentity(acceptance, record, root);
        if (!terminal.token().equals(acceptance.token())
                || !MessageDigest.isEqual(
                terminal.commandDigest(), acceptance.commandDigest())) {
            throw new IllegalArgumentException("terminal identity mismatch at "
                    + record.physicalOrdinal());
        }
    }

    private static void requirePreparedIdentity(
            PreparedSinkReceipt prepared, StageTerminalReceipt terminal,
            StageAssignmentAcceptance acceptance, StageAssignmentRecord record,
            DecodedRoot root) {
        if (terminal == null || !terminal.succeeded()) {
            throw new IllegalArgumentException("prepared Sink requires successful terminal at "
                    + record.physicalOrdinal());
        }
        requireTerminalIdentity(terminal, acceptance, record, root);
        if (!prepared.token().equals(terminal.token())
                || !MessageDigest.isEqual(
                prepared.commandDigest(), terminal.commandDigest())) {
            throw new IllegalArgumentException("prepared Sink identity mismatch at "
                    + record.physicalOrdinal());
        }
    }

    public static StageAssignmentSnapshot decodeSnapshot(byte[] rootBytes,
                                                         List<byte[]> assignmentShardBytes,
                                                         List<byte[]> acceptanceShardBytes) {
        DecodedRoot root = decodeRoot(rootBytes);
        StageAssignmentManifest manifest = decode(rootBytes, assignmentShardBytes);
        List<StageAssignmentAcceptance> acceptances = decodeAcceptances(
                root, acceptanceShardBytes);
        return new StageAssignmentSnapshot(
                manifest, root.acceptanceGeneration, acceptances);
    }

    public static StageAssignmentSnapshot decodeSnapshot(
            byte[] rootBytes, List<byte[]> assignmentShardBytes,
            List<byte[]> acceptanceShardBytes, List<byte[]> terminalShardBytes) {
        DecodedRoot root = decodeRoot(rootBytes);
        StageAssignmentManifest manifest = decode(rootBytes, assignmentShardBytes);
        List<StageAssignmentAcceptance> acceptances = decodeAcceptances(
                root, acceptanceShardBytes);
        if (terminalShardBytes == null || terminalShardBytes.size() != root.shardCount) {
            throw new IllegalArgumentException("terminal shard count mismatch");
        }
        List<StageTerminalReceipt> terminals = new ArrayList<>(root.assignmentCount);
        List<PreparedSinkReceipt> preparedSinks = new ArrayList<>(root.assignmentCount);
        for (int i = 0; i < root.shardCount; i++) {
            DecodedTerminalShard shard = decodeTerminalShard(terminalShardBytes.get(i));
            if (shard.shardIndex != i
                    || shard.assignmentCount != root.assignmentCount
                    || shard.shardCount != root.shardCount
                    || shard.generation != root.generation
                    || !shard.fence.equals(root.fence)
                    || !shard.artifactId.equals(root.artifactId)) {
                throw new IllegalArgumentException("terminal shard identity mismatch at " + i);
            }
            terminals.addAll(shard.terminals);
            preparedSinks.addAll(shard.preparedSinks);
        }
        if (terminals.size() != root.assignmentCount) {
            throw new IllegalArgumentException("terminal count mismatch");
        }
        return new StageAssignmentSnapshot(manifest, root.acceptanceGeneration,
                acceptances, terminals, preparedSinks);
    }

    private static List<StageAssignmentAcceptance> decodeAcceptances(
            DecodedRoot root, List<byte[]> acceptanceShardBytes) {
        if (acceptanceShardBytes == null
                || acceptanceShardBytes.size() != root.shardCount) {
            throw new IllegalArgumentException("acceptance shard count mismatch");
        }
        List<StageAssignmentAcceptance> acceptances =
                new ArrayList<>(root.assignmentCount);
        for (int i = 0; i < root.shardCount; i++) {
            DecodedAcceptanceShard shard =
                    decodeAcceptanceShard(acceptanceShardBytes.get(i));
            if (shard.shardIndex != i
                    || shard.assignmentCount != root.assignmentCount
                    || shard.shardCount != root.shardCount
                    || shard.generation != root.generation
                    || !shard.fence.equals(root.fence)
                    || !shard.artifactId.equals(root.artifactId)) {
                throw new IllegalArgumentException(
                        "acceptance shard identity mismatch at " + i);
            }
            acceptances.addAll(shard.acceptances);
        }
        if (acceptances.size() != root.assignmentCount) {
            throw new IllegalArgumentException("acceptance count mismatch");
        }
        return acceptances;
    }

    public static byte[] encodeToken(StageAssignmentToken token) {
        if (token == null) throw new IllegalArgumentException("token is required");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            if (token.protocolVersion() == 1) {
                writeTokenFieldsV1(out, token.fence(), token.manifestGeneration(),
                        token.acceptanceGeneration(), token.physicalOrdinal(),
                        token.stageExecutionId(), token.runtimeStageId(), token.workerId(),
                        token.workerIncarnationId());
            } else {
                writeTokenFieldsV2(out, token);
            }
            out.write(token.tokenDigest());
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(token.protocolVersion(), TOKEN, bytes.toByteArray(),
                MAX_TOKEN_FRAME_BYTES);
    }

    public static StageAssignmentToken decodeToken(byte[] encoded) {
        DecodedBody decodedBody = bodyWithVersion(encoded, TOKEN, MAX_TOKEN_FRAME_BYTES);
        RunWire.Reader body = decodedBody.body;
        RunAttemptFence fence = readFence(body, "token.fence");
        long manifestGeneration = body.readLong("token.manifestGeneration");
        long acceptanceGeneration = body.readLong("token.acceptanceGeneration");
        int ordinal = body.readInt("token.ordinal");
        String executionId = body.readText("token.stageExecutionId", 64);
        long runtimeStageId = body.readLong("token.runtimeStageId");
        String workerId = body.readText("token.workerId", RunText.MAX_ID_BYTES);
        String incarnation = body.readText("token.workerIncarnationId", RunText.MAX_ID_BYTES);
        if (decodedBody.version == 1) {
            byte[] digest = body.readFixed(CHECKSUM_BYTES, "token.digest");
            body.finish("stage assignment token body");
            return StageAssignmentToken.decodedV1(
                    fence, manifestGeneration, acceptanceGeneration, ordinal, executionId,
                    runtimeStageId, workerId, incarnation, digest);
        }
        int logicalStage = body.readInt("token.logicalStage");
        int instanceIndex = body.readInt("token.instanceIndex");
        StageAssignmentRole role = role(body.readUnsignedByte("token.role"));
        String host = body.readText("token.host", RunText.MAX_ID_BYTES);
        int rpcPort = body.readInt("token.rpcPort");
        int dataPort = body.readInt("token.dataPort");
        int sourceSplitCount = body.readInt("token.sourceSplitCount");
        byte[] sourceOwnershipDigest = body.readBoolean("token.hasSourceOwnershipDigest")
                ? body.readFixed(CHECKSUM_BYTES, "token.sourceOwnershipDigest") : null;
        byte[] recordDigest = body.readFixed(CHECKSUM_BYTES, "token.assignmentRecordDigest");
        byte[] digest = body.readFixed(CHECKSUM_BYTES, "token.digest");
        body.finish("stage assignment token body");
        return StageAssignmentToken.decodedV2(
                fence, manifestGeneration, acceptanceGeneration, ordinal, executionId,
                runtimeStageId, logicalStage, instanceIndex, role, workerId, host,
                rpcPort, dataPort, incarnation, sourceSplitCount, sourceOwnershipDigest,
                recordDigest, digest);
    }

    /** Canonical bounded wire form used by durable STAGE_REPORT retries. */
    public static byte[] encodeTerminalReceipt(StageTerminalReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("terminal receipt is required");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeTerminalReceipt(out, receipt, false);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return frame(TERMINAL_RECEIPT, bytes.toByteArray(),
                MAX_TERMINAL_RECEIPT_FRAME_BYTES);
    }

    public static StageTerminalReceipt decodeTerminalReceipt(byte[] encoded) {
        RunWire.Reader body = body(encoded, TERMINAL_RECEIPT,
                MAX_TERMINAL_RECEIPT_FRAME_BYTES);
        StageTerminalReceipt receipt = readTerminalReceipt(
                body, "terminalReceipt", false);
        body.finish("terminal receipt body");
        return receipt;
    }

    static byte[] digestTokenFieldsV1(RunAttemptFence fence, long manifestGeneration,
                                      long acceptanceGeneration,
                                      int physicalOrdinal, String stageExecutionId,
                                      long runtimeStageId, String workerId,
                                      String workerIncarnationId) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeTokenFieldsV1(out, fence, manifestGeneration, acceptanceGeneration,
                    physicalOrdinal, stageExecutionId,
                    runtimeStageId, workerId, workerIncarnationId);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return sha256(bytes.toByteArray());
    }

    static byte[] digestTokenFieldsV2(
            RunAttemptFence fence, long manifestGeneration, long acceptanceGeneration,
            StageAssignmentRecord assignment, byte[] assignmentRecordDigest) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeTokenFieldsV2(out, fence, manifestGeneration, acceptanceGeneration,
                    assignment.physicalOrdinal(), assignment.stageExecutionId(),
                    assignment.runtimeStageId(), assignment.workerId(),
                    assignment.workerIncarnationId(), assignment.logicalStage(),
                    assignment.instanceIndex(), assignment.role(), assignment.host(),
                    assignment.rpcPort(), assignment.dataPort(),
                    assignment.sourceSplitCount(), assignment.sourceOwnershipDigest(),
                    assignmentRecordDigest);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return sha256(bytes.toByteArray());
    }

    static byte[] digestTokenFieldsV2(StageAssignmentToken token) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeTokenFieldsV2(out, token);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return sha256(bytes.toByteArray());
    }

    private static DecodedRoot decodeRoot(byte[] encoded) {
        DecodedBody decodedBody = bodyWithVersion(encoded, ROOT, MAX_ROOT_FRAME_BYTES);
        RunWire.Reader body = decodedBody.body;
        Identity identity = readIdentity(body, "root");
        AttemptOutputProfile outputProfile = decodedBody.version >= 3
                ? AttemptOutputProfile.fromWireId(
                body.readUnsignedByte("root.outputProfile"))
                : AttemptOutputProfile.UNKNOWN_LEGACY;
        QualityGateAssignmentBinding qualityGateBinding = null;
        if (decodedBody.version >= ROOT_WRITE_VERSION) {
            if (!body.readBoolean("root.hasQualityGateBinding")) {
                throw new IllegalArgumentException(
                        "v5 assignment root requires a quality binding");
            }
            qualityGateBinding = readQualityGateBinding(body);
        }
        long acceptanceGeneration = body.readLong("root.acceptanceGeneration");
        if (acceptanceGeneration <= 0L) {
            throw new IllegalArgumentException("root acceptance generation must be positive");
        }
        int assignmentCount = body.readInt("root.assignmentCount");
        int shardCount = body.readInt("root.shardCount");
        if (assignmentCount < 1 || assignmentCount > StageAssignmentManifest.MAX_ASSIGNMENTS) {
            throw new IllegalArgumentException("root assignment count out of bounds");
        }
        int expectedShardCount = (assignmentCount + StageAssignmentManifest.MAX_RECORDS_PER_SHARD - 1)
                / StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        if (shardCount != expectedShardCount || shardCount > StageAssignmentManifest.MAX_SHARDS) {
            throw new IllegalArgumentException("root shard count mismatch");
        }
        byte[][] digests = new byte[shardCount][];
        for (int i = 0; i < shardCount; i++) {
            digests[i] = body.readFixed(CHECKSUM_BYTES, "root.shardDigest[" + i + "]");
        }
        body.finish("stage assignment root body");
        return new DecodedRoot(decodedBody.version, identity.fence, identity.artifactId,
                identity.generation, outputProfile, qualityGateBinding,
                acceptanceGeneration,
                assignmentCount, shardCount,
                digests);
    }

    private static void writeQualityGateBinding(
            DataOutputStream out, QualityGateAssignmentBinding binding)
            throws IOException {
        out.write(binding.policyDigest());
        out.writeInt(binding.members().size());
        for (QualityGateAssignmentBinding.MemberBinding member : binding.members()) {
            out.writeInt(member.physicalOrdinal());
            RunWire.writeText(out, member.role());
            out.write(member.descriptorDigest());
        }
    }

    private static QualityGateAssignmentBinding readQualityGateBinding(
            RunWire.Reader body) {
        byte[] policyDigest = body.readFixed(
                CHECKSUM_BYTES, "root.quality.policyDigest");
        int memberCount = body.readInt("root.quality.memberCount");
        if (memberCount != QualityGateAssignmentBinding.MEMBER_COUNT) {
            throw new IllegalArgumentException(
                    "root quality member count must be three");
        }
        List<QualityGateAssignmentBinding.MemberBinding> members =
                new ArrayList<>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            members.add(new QualityGateAssignmentBinding.MemberBinding(
                    body.readInt("root.quality.member[" + i + "].ordinal"),
                    body.readText("root.quality.member[" + i + "].role",
                            SinkAttemptIdentity.MAX_OPERATOR_ID_BYTES),
                    body.readFixed(CHECKSUM_BYTES,
                            "root.quality.member[" + i + "].descriptorDigest")));
        }
        return new QualityGateAssignmentBinding(policyDigest, members);
    }

    private static DecodedAcceptanceShard decodeAcceptanceShard(byte[] encoded) {
        RunWire.Reader body = body(encoded, ACCEPTANCE_SHARD, MAX_SHARD_FRAME_BYTES);
        Identity identity = readIdentity(body, "acceptanceShard");
        int assignmentCount = body.readInt("acceptanceShard.assignmentCount");
        int shardCount = body.readInt("acceptanceShard.shardCount");
        int shardIndex = body.readInt("acceptanceShard.shardIndex");
        int recordCount = body.readInt("acceptanceShard.recordCount");
        if (assignmentCount < 1 || assignmentCount > StageAssignmentManifest.MAX_ASSIGNMENTS
                || shardCount < 1 || shardCount > StageAssignmentManifest.MAX_SHARDS
                || shardIndex < 0 || shardIndex >= shardCount
                || recordCount < 1
                || recordCount > StageAssignmentManifest.MAX_RECORDS_PER_SHARD) {
            throw new IllegalArgumentException("acceptance shard scalar fields out of bounds");
        }
        int firstOrdinal = shardIndex * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        int expectedRecords = Math.min(StageAssignmentManifest.MAX_RECORDS_PER_SHARD,
                assignmentCount - firstOrdinal);
        if (recordCount != expectedRecords) {
            throw new IllegalArgumentException("acceptance shard record count mismatch");
        }
        List<StageAssignmentAcceptance> acceptances = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            if (!body.readBoolean("acceptanceShard.accepted[" + i + "]")) {
                acceptances.add(null);
                continue;
            }
            int tokenLength = body.readInt("acceptanceShard.tokenLength[" + i + "]");
            if (tokenLength < 1 || tokenLength > MAX_TOKEN_FRAME_BYTES) {
                throw new IllegalArgumentException("acceptance token length out of bounds");
            }
            StageAssignmentToken token = decodeToken(body.readFixed(
                    tokenLength, "acceptanceShard.token[" + i + "]"));
            byte[] commandDigest = body.readFixed(
                    CHECKSUM_BYTES, "acceptanceShard.commandDigest[" + i + "]");
            long acceptedAt = body.readLong("acceptanceShard.acceptedAt[" + i + "]");
            acceptances.add(new StageAssignmentAcceptance(token, commandDigest, acceptedAt));
        }
        body.finish("stage assignment acceptance shard body");
        return new DecodedAcceptanceShard(identity.fence, identity.artifactId,
                identity.generation, assignmentCount,
                shardCount, shardIndex, acceptances);
    }

    private static DecodedTerminalShard decodeTerminalShard(byte[] encoded) {
        DecodedBody decodedBody = bodyWithVersion(
                encoded, TERMINAL_SHARD, MAX_TERMINAL_SHARD_FRAME_BYTES);
        if (decodedBody.version < TERMINAL_PREPARED_VERSION
                && encoded.length > MAX_SHARD_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "legacy terminal shard frame length out of bounds");
        }
        RunWire.Reader body = decodedBody.body;
        Identity identity = readIdentity(body, "terminalShard");
        int assignmentCount = body.readInt("terminalShard.assignmentCount");
        int shardCount = body.readInt("terminalShard.shardCount");
        int shardIndex = body.readInt("terminalShard.shardIndex");
        int recordCount = body.readInt("terminalShard.recordCount");
        if (assignmentCount < 1
                || assignmentCount > StageAssignmentManifest.MAX_ASSIGNMENTS
                || shardCount < 1 || shardCount > StageAssignmentManifest.MAX_SHARDS
                || shardIndex < 0 || shardIndex >= shardCount
                || recordCount < 1
                || recordCount > StageAssignmentManifest.MAX_RECORDS_PER_SHARD) {
            throw new IllegalArgumentException(
                    "terminal shard scalar fields out of bounds");
        }
        int firstOrdinal = shardIndex * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        int expectedRecords = Math.min(StageAssignmentManifest.MAX_RECORDS_PER_SHARD,
                assignmentCount - firstOrdinal);
        if (recordCount != expectedRecords) {
            throw new IllegalArgumentException("terminal shard record count mismatch");
        }
        List<StageTerminalReceipt> terminals = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            if (!body.readBoolean("terminalShard.present[" + i + "]")) {
                terminals.add(null);
                continue;
            }
            terminals.add(readTerminalReceipt(body, "terminalShard[" + i + "]",
                    decodedBody.version >= TERMINAL_TELEMETRY_VERSION));
        }
        List<PreparedSinkReceipt> preparedSinks = new ArrayList<>(recordCount);
        if (decodedBody.version >= TERMINAL_PREPARED_VERSION) {
            for (int i = 0; i < recordCount; i++) {
                if (!body.readBoolean("terminalShard.prepared[" + i + "]")) {
                    preparedSinks.add(null);
                    continue;
                }
                int length = body.readInt(
                        "terminalShard.preparedLength[" + i + "]");
                if (length < 1 || length > PreparedSinkReceiptCodec.MAX_FRAME_BYTES) {
                    throw new IllegalArgumentException(
                            "prepared Sink receipt length out of bounds");
                }
                preparedSinks.add(PreparedSinkReceiptCodec.decode(body.readFixed(
                        length, "terminalShard.preparedReceipt[" + i + "]")));
            }
        } else {
            preparedSinks.addAll(Collections.nCopies(
                    recordCount, (PreparedSinkReceipt) null));
        }
        body.finish("stage assignment terminal shard body");
        return new DecodedTerminalShard(identity.fence, identity.artifactId,
                identity.generation, assignmentCount,
                shardCount, shardIndex, terminals, preparedSinks);
    }

    private static DecodedShard decodeShard(byte[] encoded) {
        DecodedBody decodedBody = bodyWithVersion(encoded, SHARD, MAX_SHARD_FRAME_BYTES);
        RunWire.Reader body = decodedBody.body;
        Identity identity = readIdentity(body, "shard");
        int assignmentCount = body.readInt("shard.assignmentCount");
        int shardCount = body.readInt("shard.shardCount");
        int shardIndex = body.readInt("shard.shardIndex");
        int recordCount = body.readInt("shard.recordCount");
        if (assignmentCount < 1 || assignmentCount > StageAssignmentManifest.MAX_ASSIGNMENTS
                || shardCount < 1 || shardCount > StageAssignmentManifest.MAX_SHARDS
                || shardIndex < 0 || shardIndex >= shardCount
                || recordCount < 1
                || recordCount > StageAssignmentManifest.MAX_RECORDS_PER_SHARD) {
            throw new IllegalArgumentException("shard scalar fields out of bounds");
        }
        int firstOrdinal = shardIndex * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        int expectedRecords = Math.min(StageAssignmentManifest.MAX_RECORDS_PER_SHARD,
                assignmentCount - firstOrdinal);
        if (expectedRecords != recordCount) {
            throw new IllegalArgumentException("shard record count mismatch");
        }
        List<StageAssignmentRecord> records = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            records.add(readRecord(body, identity.fence, identity.generation,
                    "shard.record[" + i + "]", decodedBody.version));
        }
        body.finish("stage assignment shard body");
        return new DecodedShard(decodedBody.version, identity.fence, identity.artifactId,
                identity.generation, assignmentCount, shardCount, shardIndex, records);
    }

    private static void requireManifest(StageAssignmentManifest manifest) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
    }

    private static void writeIdentity(DataOutputStream out, RunAttemptFence fence,
                                      String artifactId, long generation) throws IOException {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        RunText.required(artifactId, "artifactId", RunText.MAX_ID_BYTES);
        if (generation <= 0L) throw new IllegalArgumentException("generation must be positive");
        writeFence(out, fence);
        RunWire.writeText(out, artifactId);
        out.writeLong(generation);
    }

    private static Identity readIdentity(RunWire.Reader body, String path) {
        RunAttemptFence fence = readFence(body, path + ".fence");
        String artifactId = body.readText(path + ".artifactId", RunText.MAX_ID_BYTES);
        long generation = body.readLong(path + ".generation");
        if (generation <= 0L) throw new IllegalArgumentException(path + ".generation must be positive");
        return new Identity(fence, artifactId, generation);
    }

    private static void writeFence(DataOutputStream out, RunAttemptFence fence) throws IOException {
        RunWire.writeText(out, fence.runId());
        RunWire.writeText(out, fence.attemptId());
        out.writeLong(fence.fenceToken());
    }

    private static RunAttemptFence readFence(RunWire.Reader body, String path) {
        return new RunAttemptFence(
                body.readText(path + ".runId", RunText.MAX_ID_BYTES),
                body.readText(path + ".attemptId", RunText.MAX_ID_BYTES),
                body.readLong(path + ".fenceToken"));
    }

    private static void writeRecord(DataOutputStream out, StageAssignmentRecord record, int version)
            throws IOException {
        RunWire.writeText(out, record.stageExecutionId());
        out.writeInt(record.physicalOrdinal());
        out.writeLong(record.runtimeStageId());
        out.writeInt(record.logicalStage());
        out.writeInt(record.instanceIndex());
        out.writeByte(roleId(record.role()));
        RunWire.writeText(out, record.workerId());
        RunWire.writeText(out, record.host());
        out.writeInt(record.rpcPort());
        out.writeInt(record.dataPort());
        RunWire.writeText(out, record.workerIncarnationId());
        if (version >= 2) {
            out.writeInt(record.sourceSplitCount());
            byte[] ownership = record.sourceOwnershipDigest();
            out.writeBoolean(ownership != null);
            if (ownership != null) out.write(ownership);
        }
    }

    private static StageAssignmentRecord readRecord(RunWire.Reader body, RunAttemptFence fence,
                                                    long generation, String path, int version) {
        String executionId = body.readText(path + ".stageExecutionId", 64);
        int ordinal = body.readInt(path + ".physicalOrdinal");
        long runtimeId = body.readLong(path + ".runtimeStageId");
        int logicalStage = body.readInt(path + ".logicalStage");
        int instanceIndex = body.readInt(path + ".instanceIndex");
        StageAssignmentRole role = role(body.readUnsignedByte(path + ".role"));
        String workerId = body.readText(path + ".workerId", RunText.MAX_ID_BYTES);
        String host = body.readText(path + ".host", RunText.MAX_ID_BYTES);
        int rpcPort = body.readInt(path + ".rpcPort");
        int dataPort = body.readInt(path + ".dataPort");
        String incarnation = body.readText(path + ".workerIncarnationId", RunText.MAX_ID_BYTES);
        int sourceSplitCount = 0;
        byte[] sourceOwnershipDigest = null;
        if (version >= 2) {
            sourceSplitCount = body.readInt(path + ".sourceSplitCount");
            sourceOwnershipDigest = body.readBoolean(path + ".hasSourceOwnershipDigest")
                    ? body.readFixed(CHECKSUM_BYTES, path + ".sourceOwnershipDigest") : null;
        }
        return StageAssignmentRecord.decoded(fence, generation, executionId, ordinal,
                runtimeId, logicalStage, instanceIndex, role, workerId, host,
                rpcPort, dataPort, incarnation, sourceSplitCount, sourceOwnershipDigest);
    }

    static byte[] digestRecordFields(StageAssignmentRecord record) {
        if (record == null) throw new IllegalArgumentException("record is required");
        return digestRecordFields(record.stageExecutionId(), record.physicalOrdinal(),
                record.runtimeStageId(), record.logicalStage(), record.instanceIndex(),
                record.role(), record.workerId(), record.host(), record.rpcPort(),
                record.dataPort(), record.workerIncarnationId(), record.sourceSplitCount(),
                record.sourceOwnershipDigest());
    }

    static byte[] digestRecordFields(
            String stageExecutionId, int physicalOrdinal, long runtimeStageId,
            int logicalStage, int instanceIndex, StageAssignmentRole role,
            String workerId, String host, int rpcPort, int dataPort,
            String workerIncarnationId, int sourceSplitCount,
            byte[] sourceOwnershipDigest) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            RunWire.writeText(out, "rowweave-stage-assignment-record-v2");
            RunWire.writeText(out, stageExecutionId);
            out.writeInt(physicalOrdinal);
            out.writeLong(runtimeStageId);
            out.writeInt(logicalStage);
            out.writeInt(instanceIndex);
            out.writeByte(roleId(role));
            RunWire.writeText(out, workerId);
            RunWire.writeText(out, host);
            out.writeInt(rpcPort);
            out.writeInt(dataPort);
            RunWire.writeText(out, workerIncarnationId);
            out.writeInt(sourceSplitCount);
            out.writeBoolean(sourceOwnershipDigest != null);
            if (sourceOwnershipDigest != null) out.write(sourceOwnershipDigest);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return sha256(bytes.toByteArray());
    }

    private static void writeTokenFieldsV1(DataOutputStream out, RunAttemptFence fence,
                                           long manifestGeneration,
                                           long acceptanceGeneration, int physicalOrdinal,
                                           String stageExecutionId, long runtimeStageId,
                                           String workerId, String workerIncarnationId)
            throws IOException {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (manifestGeneration <= 0L || acceptanceGeneration <= 0L
                || physicalOrdinal < 0 || runtimeStageId <= 0L) {
            throw new IllegalArgumentException("invalid token scalar identity");
        }
        String expectedId = StageAssignmentRecord.deriveExecutionId(
                fence, manifestGeneration, physicalOrdinal);
        if (!expectedId.equals(stageExecutionId)) {
            throw new IllegalArgumentException("stageExecutionId does not match token identity");
        }
        RunText.required(workerId, "workerId", RunText.MAX_ID_BYTES);
        RunText.required(workerIncarnationId, "workerIncarnationId", RunText.MAX_ID_BYTES);
        writeFence(out, fence);
        out.writeLong(manifestGeneration);
        out.writeLong(acceptanceGeneration);
        out.writeInt(physicalOrdinal);
        RunWire.writeText(out, stageExecutionId);
        out.writeLong(runtimeStageId);
        RunWire.writeText(out, workerId);
        RunWire.writeText(out, workerIncarnationId);
    }

    private static void writeTokenFieldsV2(DataOutputStream out, StageAssignmentToken token)
            throws IOException {
        writeTokenFieldsV2(out, token.fence(), token.manifestGeneration(),
                token.acceptanceGeneration(), token.physicalOrdinal(),
                token.stageExecutionId(), token.runtimeStageId(), token.workerId(),
                token.workerIncarnationId(), token.logicalStage(), token.instanceIndex(),
                token.role(), token.host(), token.rpcPort(), token.dataPort(),
                token.sourceSplitCount(), token.sourceOwnershipDigest(),
                token.assignmentRecordDigest());
    }

    private static void writeTokenFieldsV2(
            DataOutputStream out, RunAttemptFence fence, long manifestGeneration,
            long acceptanceGeneration, int physicalOrdinal, String stageExecutionId,
            long runtimeStageId, String workerId, String workerIncarnationId,
            int logicalStage, int instanceIndex, StageAssignmentRole role, String host,
            int rpcPort, int dataPort, int sourceSplitCount,
            byte[] sourceOwnershipDigest, byte[] assignmentRecordDigest) throws IOException {
        writeTokenFieldsV1(out, fence, manifestGeneration, acceptanceGeneration,
                physicalOrdinal, stageExecutionId, runtimeStageId, workerId,
                workerIncarnationId);
        if (logicalStage < 0 || instanceIndex < 0 || role == null) {
            throw new IllegalArgumentException("invalid v2 token stage identity");
        }
        RunText.required(host, "host", RunText.MAX_ID_BYTES);
        if (rpcPort < 1 || rpcPort > 65535
                || dataPort != -1 && (dataPort < 1 || dataPort > 65535)) {
            throw new IllegalArgumentException("invalid v2 token endpoint");
        }
        if ((sourceOwnershipDigest == null) != (sourceSplitCount == 0)
                || sourceOwnershipDigest != null && sourceOwnershipDigest.length != CHECKSUM_BYTES
                || assignmentRecordDigest == null
                || assignmentRecordDigest.length != CHECKSUM_BYTES) {
            throw new IllegalArgumentException("invalid v2 token digests");
        }
        out.writeInt(logicalStage);
        out.writeInt(instanceIndex);
        out.writeByte(roleId(role));
        RunWire.writeText(out, host);
        out.writeInt(rpcPort);
        out.writeInt(dataPort);
        out.writeInt(sourceSplitCount);
        out.writeBoolean(sourceOwnershipDigest != null);
        if (sourceOwnershipDigest != null) out.write(sourceOwnershipDigest);
        out.write(assignmentRecordDigest);
    }

    private static byte[] frame(int kind, byte[] body, int maxFrameBytes) {
        return frame(WRITE_VERSION, kind, body, maxFrameBytes);
    }

    private static byte[] frame(int version, int kind, byte[] body, int maxFrameBytes) {
        if (body == null || HEADER_BYTES + body.length + CHECKSUM_BYTES > maxFrameBytes) {
            throw new IllegalArgumentException("stage assignment frame body exceeds limit");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                HEADER_BYTES + body.length + CHECKSUM_BYTES);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeShort(version);
            out.writeShort(kind);
            out.writeInt(body.length);
            out.write(body);
            out.flush();
            out.write(sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static RunWire.Reader body(byte[] encoded, int expectedKind, int maxFrameBytes) {
        return bodyWithVersion(encoded, expectedKind, maxFrameBytes).body;
    }

    private static DecodedBody bodyWithVersion(
            byte[] encoded, int expectedKind, int maxFrameBytes) {
        if (encoded == null || encoded.length < HEADER_BYTES + CHECKSUM_BYTES
                || encoded.length > maxFrameBytes) {
            throw new IllegalArgumentException("stage assignment frame length out of bounds");
        }
        RunWire.Reader frame = new RunWire.Reader(encoded);
        if (frame.readInt("frame.magic") != MAGIC) {
            throw new IllegalArgumentException("invalid stage assignment frame magic");
        }
        int version = (frame.readUnsignedByte("frame.version.high") << 8)
                | frame.readUnsignedByte("frame.version.low");
        boolean supportedVersion = version >= MIN_READ_VERSION
                && (expectedKind == ROOT
                ? version <= WRITE_VERSION || version == ROOT_WRITE_VERSION
                : expectedKind == TERMINAL_SHARD
                ? version <= TERMINAL_SHARD_WRITE_VERSION
                : version <= WRITE_VERSION);
        if (!supportedVersion) {
            throw new IllegalArgumentException("unsupported stage assignment version: " + version);
        }
        int kind = (frame.readUnsignedByte("frame.kind.high") << 8)
                | frame.readUnsignedByte("frame.kind.low");
        if (kind != expectedKind) {
            throw new IllegalArgumentException("unexpected stage assignment kind: " + kind);
        }
        int bodyLength = frame.readInt("frame.bodyLength");
        if (bodyLength < 0
                || encoded.length != HEADER_BYTES + bodyLength + CHECKSUM_BYTES) {
            throw new IllegalArgumentException("stage assignment body length mismatch");
        }
        byte[] body = frame.readFixed(bodyLength, "frame.body");
        byte[] checksum = frame.readFixed(CHECKSUM_BYTES, "frame.checksum");
        frame.finish("stage assignment frame");
        byte[] expected = sha256(Arrays.copyOf(encoded, encoded.length - CHECKSUM_BYTES));
        if (!MessageDigest.isEqual(expected, checksum)) {
            throw new IllegalArgumentException("stage assignment frame checksum mismatch");
        }
        return new DecodedBody(version, new RunWire.Reader(body));
    }

    private static int expectedShardVersion(int rootVersion) {
        return rootVersion >= ROOT_WRITE_VERSION ? WRITE_VERSION : rootVersion;
    }

    private static int roleId(StageAssignmentRole role) {
        switch (role) {
            case PRODUCER: return 1;
            case CONSUMER: return 2;
            case ROUTER: return 3;
            case KEYED_OP: return 4;
            case SINK: return 5;
            case WHOLE_GRAPH: return 6;
            case JOIN_CONSUMER: return 7;
            case SOURCE_STAGE: return 8;
            case SYSTEM_CONTROL: return 9;
            default: throw new IllegalArgumentException("unknown assignment role");
        }
    }

    private static StageAssignmentRole role(int id) {
        switch (id) {
            case 1: return StageAssignmentRole.PRODUCER;
            case 2: return StageAssignmentRole.CONSUMER;
            case 3: return StageAssignmentRole.ROUTER;
            case 4: return StageAssignmentRole.KEYED_OP;
            case 5: return StageAssignmentRole.SINK;
            case 6: return StageAssignmentRole.WHOLE_GRAPH;
            case 7: return StageAssignmentRole.JOIN_CONSUMER;
            case 8: return StageAssignmentRole.SOURCE_STAGE;
            case 9: return StageAssignmentRole.SYSTEM_CONTROL;
            default: throw new IllegalArgumentException("unknown assignment role: " + id);
        }
    }

    private static int terminalOutcomeId(StageTerminalOutcome outcome) {
        switch (outcome) {
            case SUCCEEDED: return 1;
            case FAILED: return 2;
            case CANCELED: return 3;
            case LOST: return 4;
            default: throw new IllegalArgumentException("unknown terminal outcome");
        }
    }

    private static StageTerminalOutcome terminalOutcome(int id) {
        switch (id) {
            case 1: return StageTerminalOutcome.SUCCEEDED;
            case 2: return StageTerminalOutcome.FAILED;
            case 3: return StageTerminalOutcome.CANCELED;
            case 4: return StageTerminalOutcome.LOST;
            default: throw new IllegalArgumentException(
                    "unknown terminal outcome: " + id);
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static class Identity {
        final RunAttemptFence fence;
        final String artifactId;
        final long generation;

        private Identity(RunAttemptFence fence, String artifactId, long generation) {
            this.fence = fence;
            this.artifactId = artifactId;
            this.generation = generation;
        }
    }

    /** Authenticated quality identity carried entirely by the bounded root frame. */
    public static final class QualityRootSnapshot {
        private final RunAttemptFence fence;
        private final String artifactId;
        private final long generation;
        private final int assignmentCount;
        private final int shardCount;
        private final QualityGateAssignmentBinding binding;

        private QualityRootSnapshot(
                RunAttemptFence fence, String artifactId, long generation,
                int assignmentCount, int shardCount,
                QualityGateAssignmentBinding binding) {
            this.fence = fence;
            this.artifactId = artifactId;
            this.generation = generation;
            this.assignmentCount = assignmentCount;
            this.shardCount = shardCount;
            this.binding = binding;
        }

        public RunAttemptFence fence() { return fence; }
        public String artifactId() { return artifactId; }
        public long generation() { return generation; }
        public int assignmentCount() { return assignmentCount; }
        public int shardCount() { return shardCount; }
        public QualityGateAssignmentBinding qualityGateBinding() {
            return binding;
        }
    }

    /** Authenticated bounded view of one assignment/acceptance/terminal shard ordinal range. */
    public static final class ShardSnapshot {
        private final RunAttemptFence fence;
        private final String artifactId;
        private final long generation;
        private final long acceptanceGeneration;
        private final int assignmentCount;
        private final int shardCount;
        private final int shardIndex;
        private final List<StageAssignmentRecord> records;
        private final List<StageAssignmentAcceptance> acceptances;
        private final List<StageTerminalReceipt> terminals;
        private final List<PreparedSinkReceipt> preparedSinks;

        private ShardSnapshot(RunAttemptFence fence, String artifactId,
                              long generation, long acceptanceGeneration,
                              int assignmentCount, int shardCount, int shardIndex,
                              List<StageAssignmentRecord> records,
                              List<StageAssignmentAcceptance> acceptances,
                              List<StageTerminalReceipt> terminals,
                              List<PreparedSinkReceipt> preparedSinks) {
            this.fence = fence;
            this.artifactId = artifactId;
            this.generation = generation;
            this.acceptanceGeneration = acceptanceGeneration;
            this.assignmentCount = assignmentCount;
            this.shardCount = shardCount;
            this.shardIndex = shardIndex;
            this.records = Collections.unmodifiableList(new ArrayList<>(records));
            this.acceptances = Collections.unmodifiableList(new ArrayList<>(acceptances));
            this.terminals = Collections.unmodifiableList(new ArrayList<>(terminals));
            this.preparedSinks = Collections.unmodifiableList(
                    new ArrayList<>(preparedSinks));
        }

        public RunAttemptFence fence() { return fence; }
        public String artifactId() { return artifactId; }
        public long generation() { return generation; }
        public long acceptanceGeneration() { return acceptanceGeneration; }
        public int assignmentCount() { return assignmentCount; }
        public int shardCount() { return shardCount; }
        public int shardIndex() { return shardIndex; }
        public int firstOrdinal() {
            return shardIndex * StageAssignmentManifest.MAX_RECORDS_PER_SHARD;
        }
        public StageAssignmentRecord record(int physicalOrdinal) {
            return records.get(offset(physicalOrdinal));
        }
        public StageAssignmentAcceptance acceptance(int physicalOrdinal) {
            return acceptances.get(offset(physicalOrdinal));
        }
        public StageTerminalReceipt terminal(int physicalOrdinal) {
            return terminals.get(offset(physicalOrdinal));
        }
        public PreparedSinkReceipt preparedSink(int physicalOrdinal) {
            return preparedSinks.get(offset(physicalOrdinal));
        }

        public ShardSnapshot withTerminal(StageTerminalReceipt terminal) {
            if (terminal == null) {
                throw new IllegalArgumentException("terminal is required");
            }
            int offset = offset(terminal.token().physicalOrdinal());
            if (terminals.get(offset) != null) {
                throw new IllegalArgumentException("assignment is already terminal");
            }
            DecodedRoot root = new DecodedRoot(WRITE_VERSION, fence, artifactId, generation,
                    AttemptOutputProfile.UNKNOWN_LEGACY, null,
                    acceptanceGeneration, assignmentCount, shardCount,
                    new byte[shardCount][]);
            requireTerminalIdentity(terminal, acceptances.get(offset),
                    records.get(offset), root);
            List<StageTerminalReceipt> updated = new ArrayList<>(terminals);
            updated.set(offset, terminal);
            return new ShardSnapshot(fence, artifactId, generation,
                    acceptanceGeneration, assignmentCount, shardCount, shardIndex,
                    records, acceptances, updated, preparedSinks);
        }

        private int offset(int physicalOrdinal) {
            int offset = physicalOrdinal - firstOrdinal();
            if (offset < 0 || offset >= records.size()) {
                throw new IllegalArgumentException(
                        "physical ordinal is outside decoded shard");
            }
            return offset;
        }
    }

    private static final class DecodedRoot extends Identity {
        private final int version;
        private final AttemptOutputProfile outputProfile;
        private final QualityGateAssignmentBinding qualityGateBinding;
        private final long acceptanceGeneration;
        private final int assignmentCount;
        private final int shardCount;
        private final byte[][] shardDigests;

        private DecodedRoot(int version, RunAttemptFence fence, String artifactId, long generation,
                            AttemptOutputProfile outputProfile,
                            QualityGateAssignmentBinding qualityGateBinding,
                            long acceptanceGeneration,
                            int assignmentCount, int shardCount, byte[][] shardDigests) {
            super(fence, artifactId, generation);
            this.version = version;
            this.outputProfile = outputProfile;
            this.qualityGateBinding = qualityGateBinding;
            this.acceptanceGeneration = acceptanceGeneration;
            this.assignmentCount = assignmentCount;
            this.shardCount = shardCount;
            this.shardDigests = shardDigests;
        }
    }

    private static final class DecodedAcceptanceShard extends Identity {
        private final int assignmentCount;
        private final int shardCount;
        private final int shardIndex;
        private final List<StageAssignmentAcceptance> acceptances;

        private DecodedAcceptanceShard(RunAttemptFence fence, String artifactId,
                                       long generation,
                                       int assignmentCount, int shardCount, int shardIndex,
                                       List<StageAssignmentAcceptance> acceptances) {
            super(fence, artifactId, generation);
            this.assignmentCount = assignmentCount;
            this.shardCount = shardCount;
            this.shardIndex = shardIndex;
            this.acceptances = acceptances;
        }
    }

    private static final class DecodedTerminalShard extends Identity {
        private final int assignmentCount;
        private final int shardCount;
        private final int shardIndex;
        private final List<StageTerminalReceipt> terminals;
        private final List<PreparedSinkReceipt> preparedSinks;

        private DecodedTerminalShard(RunAttemptFence fence, String artifactId,
                                     long generation,
                                     int assignmentCount, int shardCount, int shardIndex,
                                     List<StageTerminalReceipt> terminals,
                                     List<PreparedSinkReceipt> preparedSinks) {
            super(fence, artifactId, generation);
            this.assignmentCount = assignmentCount;
            this.shardCount = shardCount;
            this.shardIndex = shardIndex;
            this.terminals = terminals;
            this.preparedSinks = preparedSinks;
        }
    }

    private static final class DecodedShard extends Identity {
        private final int version;
        private final int assignmentCount;
        private final int shardCount;
        private final int shardIndex;
        private final List<StageAssignmentRecord> records;

        private DecodedShard(int version, RunAttemptFence fence, String artifactId, long generation,
                             int assignmentCount, int shardCount, int shardIndex,
                             List<StageAssignmentRecord> records) {
            super(fence, artifactId, generation);
            this.version = version;
            this.assignmentCount = assignmentCount;
            this.shardCount = shardCount;
            this.shardIndex = shardIndex;
            this.records = records;
        }
    }

    private static final class DecodedBody {
        private final int version;
        private final RunWire.Reader body;

        private DecodedBody(int version, RunWire.Reader body) {
            this.version = version;
            this.body = body;
        }
    }
}
