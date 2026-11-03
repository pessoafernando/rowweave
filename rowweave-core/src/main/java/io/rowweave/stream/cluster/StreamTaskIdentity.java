package io.rowweave.stream.cluster;

import java.util.Objects;

/** Stable identity of one physical operator subtask in a Stream attempt. */
public final class StreamTaskIdentity {
    private final String operatorUid;
    private final int subtaskIndex;

    public StreamTaskIdentity(String operatorUid, int subtaskIndex) {
        this.operatorUid = StreamControlMessage.text(
                operatorUid, "operator UID");
        if (subtaskIndex < 0 || subtaskIndex >= 4_096) {
            throw new IllegalArgumentException(
                    "Stream subtask index is invalid");
        }
        this.subtaskIndex = subtaskIndex;
    }

    public String operatorUid() { return operatorUid; }
    public int subtaskIndex() { return subtaskIndex; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamTaskIdentity)) return false;
        StreamTaskIdentity that = (StreamTaskIdentity) other;
        return subtaskIndex == that.subtaskIndex
                && operatorUid.equals(that.operatorUid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorUid, subtaskIndex);
    }

    @Override
    public String toString() {
        return operatorUid + "#" + subtaskIndex;
    }
}
