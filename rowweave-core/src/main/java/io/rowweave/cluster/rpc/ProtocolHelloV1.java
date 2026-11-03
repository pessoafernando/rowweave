package io.rowweave.cluster.rpc;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/** Immutable, canonical capability advertisement exchanged before public-v1 RPC traffic. */
public final class ProtocolHelloV1 {
    static final int MAX_ITEMS = 512;
    static final int MAX_CAPABILITY_BYTES = 256;
    static final int MAX_EXTENSION_BYTES = 4096;

    private final String manifestDigest;
    private final int protocolEpoch;
    private final ProtocolRevisionRange readerRevisions;
    private final ProtocolRevisionRange writerRevisions;
    private final List<Integer> messageIds;
    private final List<Integer> artifactFormats;
    private final List<Integer> stageCommandVersions;
    private final List<Integer> dataProtocolVersions;
    private final List<Integer> deploymentAbis;
    private final List<String> runtimeCapabilities;
    private final List<Integer> requiredExtensionIds;
    private final Map<Integer, byte[]> optionalExtensions;
    private final int maximumFrameBytes;
    private final long maximumResidentBytes;

    public ProtocolHelloV1(
            String manifestDigest, int protocolEpoch,
            ProtocolRevisionRange readerRevisions,
            ProtocolRevisionRange writerRevisions,
            Collection<Integer> messageIds,
            Collection<Integer> artifactFormats,
            Collection<Integer> stageCommandVersions,
            Collection<Integer> dataProtocolVersions,
            Collection<Integer> deploymentAbis,
            Collection<String> runtimeCapabilities,
            Collection<Integer> requiredExtensionIds,
            Map<Integer, byte[]> optionalExtensions,
            int maximumFrameBytes, long maximumResidentBytes) {
        requireDigest(manifestDigest);
        if (protocolEpoch <= 0) {
            throw new IllegalArgumentException("protocol epoch must be positive");
        }
        if (readerRevisions == null || writerRevisions == null) {
            throw new IllegalArgumentException("reader and writer revision ranges are required");
        }
        this.messageIds = positiveIds(messageIds, "message IDs", true);
        this.artifactFormats = positiveIds(artifactFormats, "artifact formats", true);
        this.stageCommandVersions = positiveIds(
                stageCommandVersions, "stage command versions", true);
        this.dataProtocolVersions = positiveIds(
                dataProtocolVersions, "data protocol versions", true);
        this.deploymentAbis = positiveIds(deploymentAbis, "deployment ABIs", true);
        this.runtimeCapabilities = capabilities(runtimeCapabilities);
        this.requiredExtensionIds = positiveIds(
                requiredExtensionIds, "required extension IDs", false);
        this.optionalExtensions = extensions(optionalExtensions);
        if (maximumFrameBytes <= 0 || maximumResidentBytes <= 0) {
            throw new IllegalArgumentException("protocol resource limits must be positive");
        }
        this.manifestDigest = manifestDigest;
        this.protocolEpoch = protocolEpoch;
        this.readerRevisions = readerRevisions;
        this.writerRevisions = writerRevisions;
        this.maximumFrameBytes = maximumFrameBytes;
        this.maximumResidentBytes = maximumResidentBytes;
    }

    private static void requireDigest(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("manifest digest must be canonical SHA-256 hex");
        }
    }

    private static List<Integer> positiveIds(
            Collection<Integer> values, String field, boolean requireNonEmpty) {
        if (values == null || values.size() > MAX_ITEMS
                || (requireNonEmpty && values.isEmpty())) {
            throw new IllegalArgumentException(field + " count is invalid");
        }
        TreeSet<Integer> sorted = new TreeSet<>();
        for (Integer value : values) {
            if (value == null || value <= 0 || !sorted.add(value)) {
                throw new IllegalArgumentException(field + " contain an invalid or duplicate ID");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(sorted));
    }

    private static List<String> capabilities(Collection<String> values) {
        if (values == null || values.isEmpty() || values.size() > MAX_ITEMS) {
            throw new IllegalArgumentException("runtime capability count is invalid");
        }
        TreeSet<String> sorted = new TreeSet<>();
        for (String value : values) {
            if (value == null || value.isEmpty()
                    || value.getBytes(StandardCharsets.UTF_8).length > MAX_CAPABILITY_BYTES
                    || !sorted.add(value)) {
                throw new IllegalArgumentException("runtime capability is invalid or duplicated");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(sorted));
    }

    private static Map<Integer, byte[]> extensions(Map<Integer, byte[]> values) {
        if (values == null || values.size() > MAX_ITEMS) {
            throw new IllegalArgumentException("optional extension count is invalid");
        }
        TreeMap<Integer, byte[]> sorted = new TreeMap<>();
        for (Map.Entry<Integer, byte[]> entry : values.entrySet()) {
            Integer id = entry.getKey();
            byte[] payload = entry.getValue();
            if (id == null || id <= 0 || payload == null
                    || payload.length > MAX_EXTENSION_BYTES) {
                throw new IllegalArgumentException("optional extension is invalid");
            }
            sorted.put(id, payload.clone());
        }
        return Collections.unmodifiableMap(sorted);
    }

    public String manifestDigest() { return manifestDigest; }
    public int protocolEpoch() { return protocolEpoch; }
    public ProtocolRevisionRange readerRevisions() { return readerRevisions; }
    public ProtocolRevisionRange writerRevisions() { return writerRevisions; }
    public List<Integer> messageIds() { return messageIds; }
    public List<Integer> artifactFormats() { return artifactFormats; }
    public List<Integer> stageCommandVersions() { return stageCommandVersions; }
    public List<Integer> dataProtocolVersions() { return dataProtocolVersions; }
    public List<Integer> deploymentAbis() { return deploymentAbis; }
    public List<String> runtimeCapabilities() { return runtimeCapabilities; }
    public List<Integer> requiredExtensionIds() { return requiredExtensionIds; }
    public int maximumFrameBytes() { return maximumFrameBytes; }
    public long maximumResidentBytes() { return maximumResidentBytes; }

    public Map<Integer, byte[]> optionalExtensions() {
        TreeMap<Integer, byte[]> copy = new TreeMap<>();
        for (Map.Entry<Integer, byte[]> entry : optionalExtensions.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().clone());
        }
        return Collections.unmodifiableMap(copy);
    }

    Map<Integer, byte[]> extensionValues() { return optionalExtensions; }

    boolean supportsExtension(int id) {
        return Collections.binarySearch(requiredExtensionIds, id) >= 0
                || optionalExtensions.containsKey(id);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ProtocolHelloV1)) return false;
        ProtocolHelloV1 that = (ProtocolHelloV1) other;
        if (protocolEpoch != that.protocolEpoch
                || maximumFrameBytes != that.maximumFrameBytes
                || maximumResidentBytes != that.maximumResidentBytes
                || !manifestDigest.equals(that.manifestDigest)
                || !readerRevisions.equals(that.readerRevisions)
                || !writerRevisions.equals(that.writerRevisions)
                || !messageIds.equals(that.messageIds)
                || !artifactFormats.equals(that.artifactFormats)
                || !stageCommandVersions.equals(that.stageCommandVersions)
                || !dataProtocolVersions.equals(that.dataProtocolVersions)
                || !deploymentAbis.equals(that.deploymentAbis)
                || !runtimeCapabilities.equals(that.runtimeCapabilities)
                || !requiredExtensionIds.equals(that.requiredExtensionIds)
                || optionalExtensions.size() != that.optionalExtensions.size()) return false;
        for (Map.Entry<Integer, byte[]> entry : optionalExtensions.entrySet()) {
            if (!java.util.Arrays.equals(entry.getValue(),
                    that.optionalExtensions.get(entry.getKey()))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(manifestDigest, protocolEpoch, readerRevisions,
                writerRevisions, messageIds, artifactFormats, stageCommandVersions,
                dataProtocolVersions, deploymentAbis, runtimeCapabilities,
                requiredExtensionIds, maximumFrameBytes, maximumResidentBytes);
        for (Map.Entry<Integer, byte[]> entry : optionalExtensions.entrySet()) {
            result = 31 * result + entry.getKey().hashCode();
            result = 31 * result + java.util.Arrays.hashCode(entry.getValue());
        }
        return result;
    }
}
