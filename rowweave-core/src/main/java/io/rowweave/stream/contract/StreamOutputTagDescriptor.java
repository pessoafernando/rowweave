package io.rowweave.stream.contract;

import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamEmissionLimit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Portable typed process output port and callback quota contract. */
public final class StreamOutputTagDescriptor {
    public enum ChangelogMode {
        APPEND(1), UPSERT(2), RETRACT(3);
        private final int wireId;
        ChangelogMode(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static ChangelogMode fromWireId(int wireId) {
            for (ChangelogMode value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_CHANGELOG_MODE",
                    "unknown output tag changelog mode");
        }
    }

    private final String tagId;
    private final PortableRowSchema schema;
    private final ChangelogMode changelogMode;
    private final List<String> stableKeyFields;
    private final int maximumLeftOutputs;
    private final long maximumLeftOutputBytes;
    private final int maximumRightOutputs;
    private final long maximumRightOutputBytes;
    private final int maximumTimerOutputs;
    private final long maximumTimerOutputBytes;

    public StreamOutputTagDescriptor(String tagId, PortableRowSchema schema,
            ChangelogMode changelogMode, List<String> stableKeyFields,
            int maximumLeftOutputs, long maximumLeftOutputBytes,
            int maximumRightOutputs, long maximumRightOutputBytes,
            int maximumTimerOutputs, long maximumTimerOutputBytes) {
        this.tagId = StreamContractSupport.text(tagId,
                "nodes.process.outputTags.id", 256);
        if (schema == null || changelogMode == null
                || stableKeyFields == null) invalid();
        this.schema = schema;
        this.changelogMode = changelogMode;
        this.stableKeyFields = keys(schema, stableKeyFields);
        if (changelogMode != ChangelogMode.APPEND
                && this.stableKeyFields.isEmpty()) invalid();
        emission(maximumLeftOutputs, maximumLeftOutputBytes);
        emission(maximumRightOutputs, maximumRightOutputBytes);
        emission(maximumTimerOutputs, maximumTimerOutputBytes);
        this.maximumLeftOutputs = maximumLeftOutputs;
        this.maximumLeftOutputBytes = maximumLeftOutputBytes;
        this.maximumRightOutputs = maximumRightOutputs;
        this.maximumRightOutputBytes = maximumRightOutputBytes;
        this.maximumTimerOutputs = maximumTimerOutputs;
        this.maximumTimerOutputBytes = maximumTimerOutputBytes;
    }

    private static List<String> keys(PortableRowSchema schema,
            List<String> values) {
        Map<String, PortableField> fields = new HashMap<>();
        for (PortableField field : schema.fields()) {
            fields.put(field.fieldId(), field);
        }
        Set<String> unique = new HashSet<>();
        List<String> copy = new ArrayList<>(values.size());
        for (String value : values) {
            PortableField field = fields.get(value);
            if (field == null || field.nullable() || !unique.add(value)) {
                invalid();
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static void emission(int rows, long bytes) {
        if (rows <= 0 || rows > StreamEmissionLimit.MAX_OUTPUTS_PER_INPUT
                || bytes <= 0L
                || bytes > StreamEmissionLimit.MAX_OUTPUT_BYTES_PER_INPUT) {
            invalid();
        }
    }

    private static void invalid() {
        throw failure("INVALID_OUTPUT_TAG",
                "output tag schema, key or quota is invalid");
    }

    private static StreamContractException failure(String code,
            String message) {
        return StreamContractSupport.failure(code,
                "nodes.process.outputTags", message);
    }

    public String tagId() { return tagId; }
    public PortableRowSchema schema() { return schema; }
    public ChangelogMode changelogMode() { return changelogMode; }
    public List<String> stableKeyFields() { return stableKeyFields; }
    public int maximumLeftOutputs() { return maximumLeftOutputs; }
    public long maximumLeftOutputBytes() { return maximumLeftOutputBytes; }
    public int maximumRightOutputs() { return maximumRightOutputs; }
    public long maximumRightOutputBytes() { return maximumRightOutputBytes; }
    public int maximumTimerOutputs() { return maximumTimerOutputs; }
    public long maximumTimerOutputBytes() { return maximumTimerOutputBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamOutputTagDescriptor)) return false;
        StreamOutputTagDescriptor that = (StreamOutputTagDescriptor) other;
        return maximumLeftOutputs == that.maximumLeftOutputs
                && maximumLeftOutputBytes == that.maximumLeftOutputBytes
                && maximumRightOutputs == that.maximumRightOutputs
                && maximumRightOutputBytes == that.maximumRightOutputBytes
                && maximumTimerOutputs == that.maximumTimerOutputs
                && maximumTimerOutputBytes == that.maximumTimerOutputBytes
                && tagId.equals(that.tagId) && schema.equals(that.schema)
                && changelogMode == that.changelogMode
                && stableKeyFields.equals(that.stableKeyFields);
    }

    @Override public int hashCode() {
        return Objects.hash(tagId, schema, changelogMode, stableKeyFields,
                maximumLeftOutputs, maximumLeftOutputBytes,
                maximumRightOutputs, maximumRightOutputBytes,
                maximumTimerOutputs, maximumTimerOutputBytes);
    }
}
