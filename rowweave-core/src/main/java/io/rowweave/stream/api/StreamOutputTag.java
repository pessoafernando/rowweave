package io.rowweave.stream.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Typed named process output port with callback-specific emission ceilings. */
public final class StreamOutputTag {
    private final String tagId;
    private final StreamSchema schema;
    private final StreamChangelogMode changelogMode;
    private final List<String> stableKeyFieldIds;
    private final StreamEmissionLimit leftEmissionLimit;
    private final StreamEmissionLimit rightEmissionLimit;
    private final StreamEmissionLimit timerEmissionLimit;

    private StreamOutputTag(String tagId, StreamSchema schema,
            StreamChangelogMode changelogMode,
            List<String> stableKeyFieldIds,
            StreamEmissionLimit leftEmissionLimit,
            StreamEmissionLimit rightEmissionLimit,
            StreamEmissionLimit timerEmissionLimit) {
        if (tagId == null || tagId.isEmpty() || schema == null
                || changelogMode == null || stableKeyFieldIds == null
                || leftEmissionLimit == null || rightEmissionLimit == null
                || timerEmissionLimit == null) {
            throw new IllegalArgumentException(
                    "tagged output declaration is incomplete");
        }
        this.stableKeyFieldIds = stableKeys(schema, stableKeyFieldIds);
        if (changelogMode != StreamChangelogMode.APPEND
                && this.stableKeyFieldIds.isEmpty()) {
            throw new IllegalArgumentException(
                    "UPSERT tagged output requires stable keys");
        }
        this.tagId = tagId;
        this.schema = schema;
        this.changelogMode = changelogMode;
        this.leftEmissionLimit = leftEmissionLimit;
        this.rightEmissionLimit = rightEmissionLimit;
        this.timerEmissionLimit = timerEmissionLimit;
    }

    public static StreamOutputTag of(String tagId, StreamSchema schema,
            StreamChangelogMode changelogMode,
            List<String> stableKeyFieldIds,
            StreamEmissionLimit leftEmissionLimit,
            StreamEmissionLimit rightEmissionLimit,
            StreamEmissionLimit timerEmissionLimit) {
        return new StreamOutputTag(tagId, schema, changelogMode,
                stableKeyFieldIds, leftEmissionLimit, rightEmissionLimit,
                timerEmissionLimit);
    }

    private static List<String> stableKeys(StreamSchema schema,
            List<String> values) {
        Set<String> unique = new HashSet<>();
        List<String> copy = new ArrayList<>(values.size());
        for (String value : values) {
            boolean found = false;
            for (StreamField field : schema.fields()) {
                if (field.fieldId().equals(value) && !field.nullable()) {
                    found = true;
                    break;
                }
            }
            if (!found || !unique.add(value)) {
                throw new IllegalArgumentException(
                        "tag keys must be unique non-null fields");
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    public String tagId() { return tagId; }
    public StreamSchema schema() { return schema; }
    public StreamChangelogMode changelogMode() { return changelogMode; }
    public List<String> stableKeyFieldIds() { return stableKeyFieldIds; }
    public StreamEmissionLimit leftEmissionLimit() {
        return leftEmissionLimit;
    }
    public StreamEmissionLimit rightEmissionLimit() {
        return rightEmissionLimit;
    }
    public StreamEmissionLimit timerEmissionLimit() {
        return timerEmissionLimit;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamOutputTag)) return false;
        StreamOutputTag that = (StreamOutputTag) other;
        return tagId.equals(that.tagId) && schema.equals(that.schema)
                && changelogMode == that.changelogMode
                && stableKeyFieldIds.equals(that.stableKeyFieldIds)
                && leftEmissionLimit.equals(that.leftEmissionLimit)
                && rightEmissionLimit.equals(that.rightEmissionLimit)
                && timerEmissionLimit.equals(that.timerEmissionLimit);
    }

    @Override public int hashCode() {
        return Objects.hash(tagId, schema, changelogMode, stableKeyFieldIds,
                leftEmissionLimit, rightEmissionLimit, timerEmissionLimit);
    }
}
