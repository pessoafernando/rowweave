package io.rowweave;

import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.SchemaFingerprint;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.ObjectStreamException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A member output of the manifest commit domain. A member writes only staging and immutable data, and the whole group's manifest
 * must be published by the commit coordinator of the complete PlanGraph.
 */
public final class ManifestFileSink implements BatchSink {

    private static final long serialVersionUID = 1L;
    private static final int PROVIDER_CONFIG_MAGIC = 0x52574d43; // RWMC
    private static final int PROVIDER_CONFIG_VERSION = 1;
    private static final int MAX_CONFIG_TEXT_BYTES = 64 * 1024;
    public static final DurableSinkCodecDescriptor PROVIDER_CONFIGURATION_CODEC =
            new DurableSinkCodecDescriptor("rowweave.manifest-config", 1,
                    DurableBatchSinkProviderContract.MAX_CONFIGURATION_BYTES);

    private enum State {
        NEW,
        BOUND,
        PREPARED,
        DATA_INSTALLED,
        DATA_SEALED,
        MANIFEST_PUBLISHED,
        FINALIZED,
        ABORTED
    }

    private final DataSchema dataSchema;
    private final String domainPath;
    private final String memberName;
    private final String domainId;
    private final String destinationId;
    private final String schemaFingerprint;
    private final byte[] qualityGatePolicyBytes;
    private final String qualityGateRole;

    private transient ManifestFileStore store;
    private transient String attemptId;
    private transient long generation;
    private transient Path stagedPath;
    private transient FileSink stagedOutput;
    private transient CommitManifest.Member installedMember;
    private transient State state;

    public ManifestFileSink(DataSchema dataSchema, String domainPath, String memberName) {
        this(dataSchema, domainPath, memberName, null, null);
    }

    static ManifestFileSink qualityGate(
            DataSchema dataSchema, String domainPath, String memberName,
            String qualityGateRole, QualityGatePolicy policy) {
        if (qualityGateRole == null || policy == null) {
            throw new IllegalArgumentException("quality gate role and policy are required");
        }
        return new ManifestFileSink(dataSchema, domainPath, memberName,
                qualityGateRole, QualityGatePolicyCodec.encode(policy));
    }

    private ManifestFileSink(DataSchema dataSchema, String domainPath, String memberName,
                               String qualityGateRole, byte[] qualityGatePolicyBytes) {
        if (dataSchema == null) {
            throw new IllegalArgumentException("dataSchema must not be null");
        }
        if (domainPath == null || domainPath.isEmpty()) {
            throw new IllegalArgumentException("domainPath must not be empty");
        }
        // The single CommitManifest validation rule rejects path fragments and Unicode homoglyphs.
        new CommitManifest.Member(memberName, "data/1/probe.data", "probe", 0L,
                new byte[CommitManifest.SHA256_BYTES]);
        if ((qualityGateRole == null) != (qualityGatePolicyBytes == null)) {
            throw new IllegalArgumentException(
                    "quality gate role and policy binding must both be present or absent");
        }
        if (qualityGateRole != null
                && !TypedQualityManifest.VALID_SINK_UID.equals(qualityGateRole)
                && !TypedQualityManifest.REJECTED_SINK_UID.equals(qualityGateRole)
                && !TypedQualityManifest.SUMMARY_SINK_UID.equals(qualityGateRole)) {
            throw new IllegalArgumentException("quality gate role is not reserved");
        }
        if (qualityGatePolicyBytes != null) {
            QualityGatePolicyCodec.decode(qualityGatePolicyBytes);
        }
        this.dataSchema = dataSchema;
        this.domainPath = Paths.get(domainPath).toAbsolutePath().normalize().toString();
        this.memberName = memberName;
        this.store = new ManifestFileStore(Paths.get(this.domainPath));
        this.domainId = store.domainId();
        this.destinationId = domainId + "#member:" + memberName;
        this.schemaFingerprint = SchemaFingerprint.of(dataSchema);
        this.qualityGateRole = qualityGateRole;
        this.qualityGatePolicyBytes = qualityGatePolicyBytes == null ? null
                : qualityGatePolicyBytes.clone();
    }

    @Override
    public DataSchema getSchema() {
        return dataSchema;
    }

    public String domainPath() {
        return domainPath;
    }

    public String domainId() {
        return domainId;
    }

    public String memberName() {
        return memberName;
    }

    /** Engine-owned immutable policy binding used by admission and recovery. */
    public QualityGatePolicy qualityGatePolicy() {
        return qualityGatePolicyBytes == null ? null
                : QualityGatePolicyCodec.decode(qualityGatePolicyBytes);
    }

    /** SHA-256 of the exact canonical policy frame. */
    public byte[] qualityGatePolicyDigest() {
        return qualityGatePolicyBytes == null ? null
                : QualityGatePolicyCodec.digestEncoded(qualityGatePolicyBytes);
    }

    /** Reserved three-member role frozen with the policy. */
    public String qualityGateRole() {
        return qualityGateRole;
    }

    /** Canonical policy bytes for exact three-member admission comparison. */
    public byte[] qualityGatePolicyBytes() {
        return qualityGatePolicyBytes == null ? null : qualityGatePolicyBytes.clone();
    }

    public boolean hasQualityGateBinding() {
        return qualityGateRole != null || qualityGatePolicyBytes != null;
    }

    public String qualityGateBindingIdentity() {
        if (!hasQualityGateBinding()) return BatchSinkDescriptor.NO_BINDING_IDENTITY;
        return "quality-gate-v1:" + qualityGateRole + ":"
                + hex(qualityGatePolicyDigest());
    }

    @Override
    public BatchSinkDescriptor batchSinkDescriptor() {
        return new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "rowweave.manifest-file", 1,
                schemaFingerprint, destinationId, domainId,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                qualityGateBindingIdentity());
    }

    /** Exact-core durable compiler bridge; emits canonical native provider config. */
    public ProviderBatchSink toProviderSink() {
        return new ProviderBatchSink(dataSchema, batchSinkDescriptor(),
                PROVIDER_CONFIGURATION_CODEC, encodeProviderConfiguration());
    }

    /** Exact-core static decode used by Master admission and the Worker provider. */
    public static ManifestFileSink fromProviderSink(
            ProviderBatchSink provider) {
        if (provider == null
                || !PROVIDER_CONFIGURATION_CODEC.equals(
                provider.configurationCodec())
                || !"rowweave.manifest-file".equals(
                provider.batchSinkDescriptor().connectorType())
                || provider.batchSinkDescriptor().connectorVersion() != 1
                || provider.batchSinkDescriptor().capabilityProfile()
                != BatchSinkDescriptor.CapabilityProfile
                .MANIFEST_ATOMIC_GROUP_V1) {
            throw new IllegalArgumentException(
                    "PROVIDER_CODEC_UNSUPPORTED");
        }
        ManifestFileSink decoded = decodeProviderConfiguration(
                provider.getSchema(), provider.configurationBytes());
        if (!decoded.batchSinkDescriptor().equals(
                provider.batchSinkDescriptor())) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL");
        }
        if (!Arrays.equals(decoded.encodeProviderConfiguration(),
                provider.configurationBytes())) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL");
        }
        return decoded;
    }

    private byte[] encodeProviderConfiguration() {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(PROVIDER_CONFIG_MAGIC);
            out.writeInt(PROVIDER_CONFIG_VERSION);
            writeConfigText(out, domainPath, false);
            writeConfigText(out, memberName, false);
            writeConfigText(out, qualityGateRole == null ? "" : qualityGateRole,
                    true);
            byte[] policy = qualityGatePolicyBytes == null
                    ? new byte[0] : qualityGatePolicyBytes;
            out.writeInt(policy.length);
            out.write(policy);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] encoded = bytes.toByteArray();
        if (encoded.length > PROVIDER_CONFIGURATION_CODEC.maxPayloadBytes()) {
            throw new IllegalArgumentException(
                    "PROVIDER_BUDGET_INVALID");
        }
        return encoded;
    }

    private static ManifestFileSink decodeProviderConfiguration(
            DataSchema dataSchema, byte[] encoded) {
        if (encoded == null || encoded.length < 24
                || encoded.length > PROVIDER_CONFIGURATION_CODEC
                .maxPayloadBytes()) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL");
        }
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(encoded))) {
            if (in.readInt() != PROVIDER_CONFIG_MAGIC
                    || in.readInt() != PROVIDER_CONFIG_VERSION) {
                throw new IllegalArgumentException(
                        "PROVIDER_CONFIG_NONCANONICAL");
            }
            String path = readConfigText(in);
            String member = readConfigText(in);
            String role = readConfigText(in);
            int policyLength = in.readInt();
            if (policyLength < 0 || policyLength > in.available()
                    || policyLength > QualityGatePolicyCodec.MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "PROVIDER_CONFIG_NONCANONICAL");
            }
            byte[] policy = new byte[policyLength];
            in.readFully(policy);
            if (in.available() != 0 || role.isEmpty() != (policyLength == 0)) {
                throw new IllegalArgumentException(
                        "PROVIDER_CONFIG_NONCANONICAL");
            }
            return role.isEmpty()
                    ? new ManifestFileSink(dataSchema, path, member)
                    : new ManifestFileSink(dataSchema, path, member, role, policy);
        } catch (IOException truncated) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL", truncated);
        }
    }

    private static void writeConfigText(
            DataOutputStream out, String value, boolean allowEmpty)
            throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if ((!allowEmpty && encoded.length < 1)
                || encoded.length > MAX_CONFIG_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "PROVIDER_BUDGET_INVALID");
        }
        out.writeInt(encoded.length);
        out.write(encoded);
    }

    private static String readConfigText(DataInputStream in)
            throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_CONFIG_TEXT_BYTES
                || length > in.available()) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL");
        }
        byte[] encoded = new byte[length];
        in.readFully(encoded);
        String value = new String(encoded, StandardCharsets.UTF_8);
        if (!Arrays.equals(encoded, value.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(
                    "PROVIDER_CONFIG_NONCANONICAL");
        }
        return value;
    }

    private Object readResolve() throws ObjectStreamException {
        return new ManifestFileSink(dataSchema, domainPath, memberName,
                qualityGateRole, qualityGatePolicyBytes);
    }

    /** The engine-internal commit-context binding; it must happen before any collect. */
    public void bindManifestAttempt(String boundAttemptId, long boundGeneration) {
        State current = state();
        if (current != State.NEW) {
            if (boundGeneration == generation && boundAttemptId != null
                    && boundAttemptId.equals(attemptId)) {
                return;
            }
            throw new IllegalStateException("manifest output is already bound or started: " + current);
        }
        ManifestFileStore currentStore = store();
        if (!domainId.equals(currentStore.domainId())) {
            throw new IllegalStateException("manifest commit domain identity changed after plan creation: expected="
                    + domainId + " actual=" + currentStore.domainId());
        }
        Path stage = currentStore.stagingPath(boundAttemptId, memberName);
        attemptId = boundAttemptId;
        generation = boundGeneration;
        stagedPath = stage;
        stagedOutput = new FileSink(dataSchema, stage.toString());
        state = State.BOUND;
    }

    @Override
    public void collect(DataRow row) {
        if (state() != State.BOUND) {
            throw new IllegalStateException(
                    "ManifestFileSink requires whole-graph commit context before collect");
        }
        stagedOutput.collect(row);
    }

    /** A member may never publish on its own; for a normal output stage, the coordinator view rewrites close as prepare. */
    @Override
    public void close() {
        throw new IllegalStateException(
                "ManifestFileSink must be committed by the whole-graph manifest coordinator");
    }

    @Override
    public boolean supportsCoordinatedCommit() {
        return true;
    }

    @Override
    public boolean isWorkerWritable() {
        return true;
    }

    @Override
    public String exclusiveDestinationId() {
        return destinationId;
    }

    @Override
    public void prepareCommit() {
        State current = state();
        if (current == State.PREPARED || current == State.DATA_INSTALLED || current == State.FINALIZED) {
            return;
        }
        if (current != State.BOUND) {
            throw new IllegalStateException("manifest output is not bound: " + current);
        }
        stagedOutput.close();
        store().forceStaged(stagedPath);
        state = State.PREPARED;
    }

    @Override
    public void commitPrepared() {
        State current = state();
        if (current == State.DATA_INSTALLED || current == State.FINALIZED) {
            return;
        }
        if (current != State.PREPARED) {
            throw new IllegalStateException("manifest output is not prepared: " + current);
        }
        installedMember = store().installMember(
                generation, memberName, schemaFingerprint, stagedPath);
        state = State.DATA_INSTALLED;
    }

    public CommitManifest.Member installedMember() {
        if (state() != State.DATA_INSTALLED && state() != State.DATA_SEALED
                && state() != State.MANIFEST_PUBLISHED
                && state() != State.FINALIZED) {
            throw new IllegalStateException("manifest member data is not installed: " + state());
        }
        return installedMember;
    }

    /**
     * Transfers the immutable member out of Worker rollback ownership. No manifest is published.
     */
    public void sealInstalledMember() {
        State current = state();
        if (current == State.DATA_SEALED || current == State.FINALIZED) return;
        if (current != State.DATA_INSTALLED) {
            throw new IllegalStateException("manifest member is not installed: " + current);
        }
        store().deleteStaging(stagedPath);
        stagedOutput = null;
        stagedPath = null;
        state = State.DATA_SEALED;
    }

    /** The one visibility action of the Worker group coordinator. */
    public void publishManifestGroup(CommitManifest manifest) {
        requireManifestContext(manifest.attemptId(), manifest.generation());
        if (!domainId.equals(manifest.domainId()) || !manifest.members().contains(installedMember)) {
            throw new IllegalArgumentException("manifest group does not contain coordinator member/domain");
        }
        store().publish(manifest);
        state = State.MANIFEST_PUBLISHED;
    }

    public void requireManifestContext(String expectedAttemptId, long expectedGeneration) {
        if (expectedAttemptId == null || !expectedAttemptId.equals(attemptId)
                || expectedGeneration != generation) {
            throw new IllegalArgumentException("manifest output attempt/generation mismatch");
        }
    }

    @Override
    public void rollbackCommit() {
        State current = state();
        if (current == State.ABORTED) {
            return;
        }
        if (current == State.FINALIZED) {
            throw new IllegalStateException("finalized manifest output cannot be rolled back");
        }
        if (current == State.DATA_SEALED) {
            return; // immutable member ownership has moved to the durable journal/GC domain.
        }
        if (generation > 0L) {
            CommitManifest visible = store().exact(generation);
            if (visible != null) {
                if (attemptId.equals(visible.attemptId()) && domainId.equals(visible.domainId())) {
                    if (installedMember != null && visible.members().contains(installedMember)) {
                        state = State.MANIFEST_PUBLISHED;
                        return; // the manifest is a durable outcome; a late rollback may only be absorbed, and must not delete data.
                    }
                    // A conflicting replay of the same attempt may already have failed during the immutable install.
                    // Once the generation is visible, only this attempt's staging may be cleaned; published data must not be touched.
                    store().deleteStaging(stagedPath);
                    state = State.ABORTED;
                    installedMember = null;
                    return;
                }
                throw new IllegalStateException(
                        "manifest generation conflicts with rollback context: " + generation);
            }
        }
        Throwable failure = null;
        FileSink output = stagedOutput;
        if (output != null) {
            try {
                output.abort();
            } catch (Throwable abortFailure) {
                failure = abortFailure;
            }
        }
        try {
            if (generation > 0L) {
                store().deleteUncommittedMember(generation, memberName, stagedPath);
            } else {
                store().deleteStaging(stagedPath);
            }
        } catch (Throwable cleanupFailure) {
            if (failure == null) {
                failure = cleanupFailure;
            } else {
                failure.addSuppressed(cleanupFailure);
            }
        }
        if (failure == null) {
            state = State.ABORTED;
            installedMember = null;
            return;
        }
        throwUnchecked(failure);
    }

    @Override
    public void finalizeCommit() {
        State current = state();
        if (current == State.FINALIZED) {
            return;
        }
        if (current == State.DATA_SEALED) {
            state = State.FINALIZED;
            return;
        }
        if (current == State.DATA_INSTALLED) {
            CommitManifest visible = store().exact(generation);
            if (visible == null || !attemptId.equals(visible.attemptId())
                    || !visible.members().contains(installedMember)) {
                throw new IllegalStateException(
                        "manifest output cannot finalize before group publication");
            }
        } else if (current != State.MANIFEST_PUBLISHED) {
            throw new IllegalStateException("manifest output data is not installed: " + current);
        }
        store().deleteStaging(stagedPath);
        state = State.FINALIZED;
    }

    @Override
    public void abort() {
        rollbackCommit();
    }

    private ManifestFileStore store() {
        if (store == null) {
            store = new ManifestFileStore(Paths.get(domainPath));
        }
        return store;
    }

    private State state() {
        return state == null ? State.NEW : state;
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    private static String hex(byte[] value) {
        char[] result = new char[value.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < value.length; i++) {
            int unsigned = value[i] & 0xff;
            result[i * 2] = digits[unsigned >>> 4];
            result[i * 2 + 1] = digits[unsigned & 0xf];
        }
        return new String(result);
    }
}
