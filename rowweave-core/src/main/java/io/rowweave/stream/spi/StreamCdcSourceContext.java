package io.rowweave.stream.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** CDC cut and recovery view with optional Worker-local ephemeral secrets. */
public final class StreamCdcSourceContext {
    public enum Phase { SNAPSHOT, LOG }

    private final String operatorUid;
    private final byte[] configuration;
    private final String snapshotId;
    private final long snapshotCutSequence;
    private final List<String> snapshotSplitIds;
    private final List<String> completedSnapshotSplitIds;
    private final Phase phase;
    private final long lastLogSequence;
    private final byte[] externalPosition;
    private final StreamSecretAccess secrets;

    public StreamCdcSourceContext(String operatorUid, byte[] configuration,
            String snapshotId, long snapshotCutSequence,
            List<String> snapshotSplitIds,
            List<String> completedSnapshotSplitIds,
            Phase phase, long lastLogSequence) {
        this(operatorUid, configuration, snapshotId, snapshotCutSequence,
                snapshotSplitIds, completedSnapshotSplitIds, phase,
                lastLogSequence, null, StreamSecretAccess.unavailable());
    }

    public StreamCdcSourceContext(String operatorUid, byte[] configuration,
            String snapshotId, long snapshotCutSequence,
            List<String> snapshotSplitIds,
            List<String> completedSnapshotSplitIds,
            Phase phase, long lastLogSequence,
            StreamSecretAccess secrets) {
        this(operatorUid, configuration, snapshotId, snapshotCutSequence,
                snapshotSplitIds, completedSnapshotSplitIds, phase,
                lastLogSequence, null, secrets);
    }

    public StreamCdcSourceContext(String operatorUid, byte[] configuration,
            String snapshotId, long snapshotCutSequence,
            List<String> snapshotSplitIds,
            List<String> completedSnapshotSplitIds,
            Phase phase, long lastLogSequence,
            byte[] externalPosition, StreamSecretAccess secrets) {
        if (operatorUid == null || configuration == null
                || snapshotId == null || snapshotCutSequence < 0L
                || snapshotSplitIds == null || snapshotSplitIds.isEmpty()
                || completedSnapshotSplitIds == null || phase == null
                || lastLogSequence < snapshotCutSequence
                || (externalPosition != null
                && (externalPosition.length == 0
                || externalPosition.length
                > io.rowweave.stream.contract.StreamCdcEnumeratorState
                .MAX_EXTERNAL_POSITION_BYTES))
                || (phase == Phase.SNAPSHOT && externalPosition != null)
                || secrets == null) {
            throw new IllegalArgumentException(
                    "invalid resident CDC source context");
        }
        this.operatorUid = operatorUid;
        this.configuration = configuration.clone();
        this.snapshotId = snapshotId;
        this.snapshotCutSequence = snapshotCutSequence;
        this.snapshotSplitIds = immutable(snapshotSplitIds);
        this.completedSnapshotSplitIds = immutable(
                completedSnapshotSplitIds);
        this.phase = phase;
        this.lastLogSequence = lastLogSequence;
        this.externalPosition = externalPosition == null
                ? null : externalPosition.clone();
        this.secrets = secrets;
    }

    public String operatorUid() { return operatorUid; }
    public byte[] configurationFrame() { return configuration.clone(); }
    public String snapshotId() { return snapshotId; }
    public long snapshotCutSequence() { return snapshotCutSequence; }
    public List<String> snapshotSplitIds() { return snapshotSplitIds; }
    public List<String> completedSnapshotSplitIds() {
        return completedSnapshotSplitIds;
    }
    public Phase phase() { return phase; }
    public long lastLogSequence() { return lastLogSequence; }
    public boolean hasExternalPosition() {
        return externalPosition != null;
    }
    public byte[] externalPositionFrame() {
        if (externalPosition == null) {
            throw new IllegalStateException(
                    "resident CDC context has no external position");
        }
        return externalPosition.clone();
    }
    public StreamSecretAccess secrets() { return secrets; }

    public StreamCdcSourceContext withSecrets(StreamSecretAccess access) {
        return new StreamCdcSourceContext(operatorUid, configuration,
                snapshotId, snapshotCutSequence, snapshotSplitIds,
                completedSnapshotSplitIds, phase, lastLogSequence,
                externalPosition, access);
    }

    private static List<String> immutable(List<String> values) {
        List<String> copy = new ArrayList<>(values.size());
        for (String value : values) {
            if (value == null || value.isEmpty()) {
                throw new IllegalArgumentException(
                        "resident CDC split identity is required");
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }
}
