package io.rowweave.stream.contract;

import java.util.Objects;
import java.util.regex.Pattern;

/** Exact signed adapter and state-contract mapping for one offline migration. */
public final class StreamStateMigrationBinding {
    private static final long MAX_OUTPUT_BYTES = 1L << 40;
    private static final Pattern DEPLOYMENT_ID =
            Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final Pattern CLASS_NAME = Pattern.compile(
            "[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");

    private final String operatorUid;
    private final StreamStateDescriptor sourceState;
    private final StreamStateDescriptor targetState;
    private final String deploymentSetId;
    private final String deploymentId;
    private final String deploymentJarDigest;
    private final String adapterClassName;
    private final String targetContentDigest;
    private final long maximumOutputBytes;

    public StreamStateMigrationBinding(
            String operatorUid, StreamStateDescriptor sourceState,
            StreamStateDescriptor targetState, String deploymentSetId,
            String deploymentId, String deploymentJarDigest,
            String adapterClassName, String targetContentDigest,
            long maximumOutputBytes) {
        this.operatorUid = StreamContractSupport.text(
                operatorUid, "stateMigration.operatorUid", 256);
        if (sourceState == null || targetState == null
                || !sourceState.stateName().equals(targetState.stateName())
                || sourceState.kind() != targetState.kind()
                || sourceState.ttlMode() != targetState.ttlMode()
                || sourceState.ttlMillis() != targetState.ttlMillis()
                || !contractChanged(sourceState, targetState)) {
            throw StreamContractSupport.failure("INVALID_STATE_MIGRATION",
                    "stateMigration.state",
                    "migration must preserve state identity and semantics");
        }
        this.sourceState = sourceState;
        this.targetState = targetState;
        this.deploymentSetId = StreamContractSupport.digest(
                deploymentSetId, "stateMigration.deploymentSetId");
        if (deploymentId == null || !DEPLOYMENT_ID.matcher(deploymentId).matches()) {
            throw StreamContractSupport.failure("INVALID_STATE_MIGRATION",
                    "stateMigration.deploymentId",
                    "canonical deployment identity is required");
        }
        this.deploymentId = deploymentId;
        this.deploymentJarDigest = StreamContractSupport.digest(
                deploymentJarDigest, "stateMigration.deploymentJarDigest");
        if (adapterClassName == null
                || !CLASS_NAME.matcher(adapterClassName).matches()) {
            throw StreamContractSupport.failure("INVALID_STATE_MIGRATION",
                    "stateMigration.adapterClassName",
                    "canonical adapter class name is required");
        }
        this.adapterClassName = adapterClassName;
        this.targetContentDigest = StreamContractSupport.digest(
                targetContentDigest, "stateMigration.targetContentDigest");
        if (maximumOutputBytes < 1L
                || maximumOutputBytes > MAX_OUTPUT_BYTES) {
            throw StreamContractSupport.failure("INVALID_STATE_MIGRATION",
                    "stateMigration.maximumOutputBytes",
                    "migration output bound is invalid");
        }
        this.maximumOutputBytes = maximumOutputBytes;
    }

    private static boolean contractChanged(
            StreamStateDescriptor source, StreamStateDescriptor target) {
        return source.stateSchemaVersion() != target.stateSchemaVersion()
                || !source.serializerContractId().equals(
                        target.serializerContractId());
    }

    public String operatorUid() { return operatorUid; }
    public StreamStateDescriptor sourceState() { return sourceState; }
    public StreamStateDescriptor targetState() { return targetState; }
    public String deploymentSetId() { return deploymentSetId; }
    public String deploymentId() { return deploymentId; }
    public String deploymentJarDigest() { return deploymentJarDigest; }
    public String adapterClassName() { return adapterClassName; }
    public String targetContentDigest() { return targetContentDigest; }
    public long maximumOutputBytes() { return maximumOutputBytes; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamStateMigrationBinding)) return false;
        StreamStateMigrationBinding that =
                (StreamStateMigrationBinding) other;
        return maximumOutputBytes == that.maximumOutputBytes
                && operatorUid.equals(that.operatorUid)
                && sourceState.equals(that.sourceState)
                && targetState.equals(that.targetState)
                && deploymentSetId.equals(that.deploymentSetId)
                && deploymentId.equals(that.deploymentId)
                && deploymentJarDigest.equals(that.deploymentJarDigest)
                && adapterClassName.equals(that.adapterClassName)
                && targetContentDigest.equals(that.targetContentDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorUid, sourceState, targetState,
                deploymentSetId, deploymentId, deploymentJarDigest,
                adapterClassName, targetContentDigest, maximumOutputBytes);
    }
}
