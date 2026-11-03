package io.rowweave.cluster.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable intersection authorized for one v1 control connection. */
public final class NegotiatedProtocol {
    private final int protocolEpoch;
    private final ProtocolRevisionRange readerRevisions;
    private final ProtocolRevisionRange writerRevisions;
    private final List<Integer> messageIds;
    private final List<Integer> artifactFormats;
    private final List<Integer> stageCommandVersions;
    private final List<Integer> dataProtocolVersions;
    private final List<Integer> deploymentAbis;
    private final List<String> runtimeCapabilities;
    private final List<Integer> optionalExtensionIds;
    private final int maximumFrameBytes;
    private final long maximumResidentBytes;

    NegotiatedProtocol(
            int protocolEpoch, ProtocolRevisionRange readerRevisions,
            ProtocolRevisionRange writerRevisions, List<Integer> messageIds,
            List<Integer> artifactFormats, List<Integer> stageCommandVersions,
            List<Integer> dataProtocolVersions, List<Integer> deploymentAbis,
            List<String> runtimeCapabilities, List<Integer> optionalExtensionIds,
            int maximumFrameBytes, long maximumResidentBytes) {
        this.protocolEpoch = protocolEpoch;
        this.readerRevisions = readerRevisions;
        this.writerRevisions = writerRevisions;
        this.messageIds = immutable(messageIds);
        this.artifactFormats = immutable(artifactFormats);
        this.stageCommandVersions = immutable(stageCommandVersions);
        this.dataProtocolVersions = immutable(dataProtocolVersions);
        this.deploymentAbis = immutable(deploymentAbis);
        this.runtimeCapabilities = Collections.unmodifiableList(
                new ArrayList<>(runtimeCapabilities));
        this.optionalExtensionIds = immutable(optionalExtensionIds);
        this.maximumFrameBytes = maximumFrameBytes;
        this.maximumResidentBytes = maximumResidentBytes;
    }

    private static List<Integer> immutable(List<Integer> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public int protocolEpoch() { return protocolEpoch; }
    public int minimumReaderRevision() { return readerRevisions.minimum(); }
    public int maximumReaderRevision() { return readerRevisions.maximum(); }
    public int minimumWriterRevision() { return writerRevisions.minimum(); }
    public int maximumWriterRevision() { return writerRevisions.maximum(); }
    public List<Integer> messageIds() { return messageIds; }
    public List<Integer> artifactFormats() { return artifactFormats; }
    public List<Integer> stageCommandVersions() { return stageCommandVersions; }
    public List<Integer> dataProtocolVersions() { return dataProtocolVersions; }
    public List<Integer> deploymentAbis() { return deploymentAbis; }
    public List<String> runtimeCapabilities() { return runtimeCapabilities; }
    public List<Integer> optionalExtensionIds() { return optionalExtensionIds; }
    public int maximumFrameBytes() { return maximumFrameBytes; }
    public long maximumResidentBytes() { return maximumResidentBytes; }

    public void requireMessage(RpcMessage.Type type) {
        int messageId = ControlMessageId.wireId(type);
        if (Collections.binarySearch(messageIds, messageId) < 0) {
            throw new ProtocolNegotiationException("MESSAGE_UNAVAILABLE",
                    "control message is outside the negotiated intersection");
        }
    }

    public void requireWriterRevision(int revision) {
        if (revision < writerRevisions.minimum() || revision > writerRevisions.maximum()) {
            throw new ProtocolNegotiationException("WRITER_REVISION_UNAVAILABLE",
                    "writer revision is outside the negotiated intersection");
        }
    }
}
