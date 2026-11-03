package io.rowweave.stream.api;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Public, inert and replayable source specification. */
public final class StreamSourceSpec {
    private final StreamSchema schema;
    private final String providerKey;
    private final String providerContractDigest;
    private final byte[] configurationFrame;
    private final List<StreamSplitSpec> splits;
    private final StreamSourceDiscoverySpec discoverySpec;
    private final StreamWatermarkPolicy watermarkPolicy;
    private final StreamChangelogMode changelogMode;
    private final boolean residentCdc;
    private final int residentCdcStateVersion;
    private final boolean boundedTermination;
    private final List<String> stableKeyFieldIds;
    private final String snapshotId;
    private final long snapshotCutSequence;
    private final List<String> snapshotSplitIds;

    private StreamSourceSpec(StreamSchema schema, String providerKey,
            String providerContractDigest, byte[] configurationFrame,
            List<StreamSplitSpec> splits,
            StreamSourceDiscoverySpec discoverySpec,
            StreamWatermarkPolicy watermarkPolicy,
            StreamChangelogMode changelogMode, boolean residentCdc,
            int residentCdcStateVersion,
            boolean boundedTermination,
            List<String> stableKeyFieldIds, String snapshotId,
            long snapshotCutSequence, List<String> snapshotSplitIds) {
        if (schema == null || providerKey == null || providerContractDigest == null
                || configurationFrame == null || splits == null
                || (splits.isEmpty() == (discoverySpec == null))
                || changelogMode == null || stableKeyFieldIds == null
                || snapshotSplitIds == null
                || (boundedTermination && (residentCdc || discoverySpec != null))
                || (!residentCdc && (snapshotId != null
                || snapshotCutSequence != -1L
                || !snapshotSplitIds.isEmpty()))
                || (residentCdc
                ? residentCdcStateVersion < 1
                || residentCdcStateVersion > 2
                : residentCdcStateVersion != 0)) {
            throw new IllegalArgumentException("replayable source fields are required");
        }
        this.schema = schema;
        this.providerKey = providerKey;
        this.providerContractDigest = providerContractDigest;
        this.configurationFrame = configurationFrame.clone();
        List<StreamSplitSpec> copied = new ArrayList<>(splits.size());
        for (StreamSplitSpec split : splits) {
            if (split == null) throw new IllegalArgumentException("source split is required");
            copied.add(split);
        }
        this.splits = Collections.unmodifiableList(copied);
        this.discoverySpec = discoverySpec;
        this.watermarkPolicy = watermarkPolicy;
        this.changelogMode = changelogMode;
        this.residentCdc = residentCdc;
        this.residentCdcStateVersion = residentCdcStateVersion;
        this.boundedTermination = boundedTermination;
        this.stableKeyFieldIds = stableKeys(
                schema, stableKeyFieldIds,
                changelogMode != StreamChangelogMode.APPEND);
        this.snapshotId = snapshotId;
        this.snapshotCutSequence = snapshotCutSequence;
        this.snapshotSplitIds = Collections.unmodifiableList(
                new ArrayList<>(snapshotSplitIds));
    }

    public static StreamSourceSpec exactOrdered(StreamSchema schema,
            String providerKey, String providerContractDigest,
            byte[] configurationFrame, List<StreamSplitSpec> splits) {
        return new StreamSourceSpec(schema, providerKey, providerContractDigest,
                configurationFrame, splits, null, null,
                StreamChangelogMode.APPEND, false, 0, false,
                Collections.emptyList(), null, -1L,
                Collections.emptyList());
    }

    /** Fixed replayable splits whose public reader emits an explicit terminal event. */
    public static StreamSourceSpec boundedExactOrdered(StreamSchema schema,
            String providerKey, String providerContractDigest,
            byte[] configurationFrame, List<StreamSplitSpec> splits) {
        return new StreamSourceSpec(schema, providerKey,
                providerContractDigest, configurationFrame, splits, null,
                null, StreamChangelogMode.APPEND, false, 0, true,
                Collections.<String>emptyList(), null, -1L,
                Collections.<String>emptyList());
    }

    /** Exact ordered source that emits canonical RETRACT change records. */
    public static StreamSourceSpec retractExactOrdered(StreamSchema schema,
            List<String> stableKeyFieldIds, String providerKey,
            String providerContractDigest, byte[] configurationFrame,
            List<StreamSplitSpec> splits) {
        return new StreamSourceSpec(schema, providerKey,
                providerContractDigest, configurationFrame, splits, null,
                null, StreamChangelogMode.RETRACT, false, 0, false,
                stableKeyFieldIds, null, -1L, Collections.emptyList());
    }

    public static StreamSourceSpec exactOrderedEventTime(StreamSchema schema,
            String providerKey, String providerContractDigest,
            byte[] configurationFrame, List<StreamSplitSpec> splits,
            StreamWatermarkPolicy watermarkPolicy) {
        if (watermarkPolicy == null) {
            throw new IllegalArgumentException("event-time watermark policy is required");
        }
        return new StreamSourceSpec(schema, providerKey, providerContractDigest,
                configurationFrame, splits, null, watermarkPolicy,
                StreamChangelogMode.APPEND, false, 0, false,
                Collections.emptyList(), null, -1L,
                Collections.emptyList());
    }

    /** Fixed event-time splits completed only by explicit END_OF_INPUT. */
    public static StreamSourceSpec boundedExactOrderedEventTime(
            StreamSchema schema, String providerKey,
            String providerContractDigest, byte[] configurationFrame,
            List<StreamSplitSpec> splits,
            StreamWatermarkPolicy watermarkPolicy) {
        if (watermarkPolicy == null) {
            throw new IllegalArgumentException(
                    "event-time watermark policy is required");
        }
        return new StreamSourceSpec(schema, providerKey,
                providerContractDigest, configurationFrame, splits, null,
                watermarkPolicy, StreamChangelogMode.APPEND, false, 0, true,
                Collections.emptyList(), null, -1L,
                Collections.emptyList());
    }

    /** Event-time source that emits canonical RETRACT change records. */
    public static StreamSourceSpec retractExactOrderedEventTime(
            StreamSchema schema, List<String> stableKeyFieldIds,
            String providerKey, String providerContractDigest,
            byte[] configurationFrame, List<StreamSplitSpec> splits,
            StreamWatermarkPolicy watermarkPolicy) {
        if (watermarkPolicy == null) {
            throw new IllegalArgumentException(
                    "event-time watermark policy is required");
        }
        return new StreamSourceSpec(schema, providerKey,
                providerContractDigest, configurationFrame, splits, null,
                watermarkPolicy, StreamChangelogMode.RETRACT, false, 0, false,
                stableKeyFieldIds, null, -1L, Collections.emptyList());
    }

    /** Exact ordered source whose split set is discovered by its provider. */
    public static StreamSourceSpec dynamicExactOrdered(StreamSchema schema,
            String providerKey, String providerContractDigest,
            byte[] configurationFrame,
            StreamSourceDiscoverySpec discoverySpec) {
        return new StreamSourceSpec(schema, providerKey,
                providerContractDigest, configurationFrame,
                Collections.<StreamSplitSpec>emptyList(), discoverySpec,
                null, StreamChangelogMode.APPEND, false, 0, false,
                Collections.<String>emptyList(), null, -1L,
                Collections.<String>emptyList());
    }

    /** Exact snapshot-to-log CDC source with a frozen canonical snapshot cut. */
    public static StreamSourceSpec residentCdc(
            StreamSchema schema, List<String> stableKeyFieldIds,
            String providerKey, String providerContractDigest,
            byte[] configurationFrame, String snapshotId,
            long snapshotCutSequence, List<String> snapshotSplitIds) {
        io.rowweave.stream.contract.StreamCdcEnumeratorState descriptor =
                io.rowweave.stream.contract.StreamCdcEnumeratorState.initial(
                        "resident-cdc-descriptor", snapshotId,
                        snapshotCutSequence, snapshotSplitIds);
        byte[] descriptorFrame = io.rowweave.stream.contract
                .StreamCdcEnumeratorStateCodec.encode(descriptor);
        StreamSplitSpec enumeratorSplit = new StreamSplitSpec(
                sha256(descriptorFrame), "STREAM_CDC_ENUMERATOR_V1",
                descriptorFrame);
        return new StreamSourceSpec(schema, providerKey,
                providerContractDigest, configurationFrame,
                Collections.singletonList(enumeratorSplit), null, null,
                StreamChangelogMode.UPSERT, true, 1, false,
                stableKeyFieldIds, descriptor.snapshotId(),
                descriptor.snapshotCutSequence(),
                descriptor.expectedSplitIds());
    }

    /** CDC v2 source with opaque external positions and transaction boundaries. */
    public static StreamSourceSpec residentCdcV2(
            StreamSchema schema, List<String> stableKeyFieldIds,
            String providerKey, String providerContractDigest,
            byte[] configurationFrame, String snapshotId,
            long snapshotCutSequence, List<String> snapshotSplitIds) {
        io.rowweave.stream.contract.StreamCdcEnumeratorState descriptor =
                io.rowweave.stream.contract.StreamCdcEnumeratorState.initialV2(
                        "resident-cdc-descriptor", snapshotId,
                        snapshotCutSequence, snapshotSplitIds);
        byte[] descriptorFrame = io.rowweave.stream.contract
                .StreamCdcEnumeratorStateCodec.encode(descriptor);
        StreamSplitSpec enumeratorSplit = new StreamSplitSpec(
                sha256(descriptorFrame), "STREAM_CDC_ENUMERATOR_V1",
                descriptorFrame);
        return new StreamSourceSpec(schema, providerKey,
                providerContractDigest, configurationFrame,
                Collections.singletonList(enumeratorSplit), null, null,
                StreamChangelogMode.UPSERT, true, 2, false,
                stableKeyFieldIds, descriptor.snapshotId(),
                descriptor.snapshotCutSequence(),
                descriptor.expectedSplitIds());
    }

    public StreamSchema schema() { return schema; }
    public String providerKey() { return providerKey; }
    public String providerContractDigest() { return providerContractDigest; }
    public byte[] configurationFrame() { return configurationFrame.clone(); }
    public List<StreamSplitSpec> splits() { return splits; }
    public boolean dynamicDiscovery() { return discoverySpec != null; }
    public StreamSourceDiscoverySpec discoverySpec() {
        if (discoverySpec == null) {
            throw new IllegalStateException(
                    "fixed source has no discovery specification");
        }
        return discoverySpec;
    }
    public StreamWatermarkPolicy watermarkPolicy() { return watermarkPolicy; }
    public StreamChangelogMode changelogMode() { return changelogMode; }
    public boolean residentCdc() { return residentCdc; }
    public int residentCdcStateVersion() {
        return residentCdcStateVersion;
    }
    public boolean boundedTermination() { return boundedTermination; }
    public List<String> stableKeyFieldIds() { return stableKeyFieldIds; }
    public String snapshotId() { return snapshotId; }
    public long snapshotCutSequence() { return snapshotCutSequence; }
    public List<String> snapshotSplitIds() { return snapshotSplitIds; }

    private static List<String> stableKeys(StreamSchema schema,
            List<String> values, boolean required) {
        if (required && values.isEmpty()) {
            throw new IllegalArgumentException(
                    "resident CDC requires stable key fields");
        }
        java.util.LinkedHashSet<String> unique = new java.util.LinkedHashSet<>();
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
                        "source stable keys must be unique non-null fields");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(unique));
    }

    private static String sha256(byte[] frame) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(frame);
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest) {
                result.append(Character.forDigit((value >>> 4) & 0xf, 16));
                result.append(Character.forDigit(value & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
