package io.rowweave.sched.net;

/**
 * Constants of the data plane frame protocol. This class is the single authority for the frame
 * layout, and every other declaration forwards to it. The layout is fixed at the byte level, so
 * encodings stay bit-identical.
 */
public final class FrameCodec {

    private FrameCodec() { }

    public static final byte FRAME_RECORD = 0;
    public static final byte FRAME_EOS = 1;
    public static final byte FRAME_ERROR = 2;
    /** Heartbeat frame: empty payload, not queued by the receiver, purely a liveness signal. */
    public static final byte FRAME_KEEPALIVE = 3;
    /**
     * Credit carried as a frame instead of a bare int: the payload is the 4-byte credit value,
     * and targetId names the sub-stream the credit is granted to.
     */
    public static final byte FRAME_CREDIT = 4;
    /** A logical producer opens a target: OPEN first, then initial credit, and only then RECORD. */
    public static final byte FRAME_OPEN = 5;

    /** OOM guard: the upper bound on the payload of one frame. */
    public static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    /** LOOPBACK V0 header remains byte-for-byte compatible. */
    public static final int V0_HEADER_BYTES = 1 + 16 + 4;
    /** MTLS V1 adds the stable 32-bit logical producer ordinal. */
    public static final int V1_HEADER_BYTES = 1 + 16 + 4 + 4;
    /** Historical alias used only by the V0 transport. */
    public static final int HEADER_BYTES = V0_HEADER_BYTES;

    static int headerBytes(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol) {
        return protocol == io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1
                ? V1_HEADER_BYTES : V0_HEADER_BYTES;
    }

    static void validateForProtocol(
            io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol,
            Frame frame) {
        if (frame == null || frame.payload() == null || !isKnownType(frame.type())) {
            throw new IllegalArgumentException("valid frame and payload are required");
        }
        validateHeaderForProtocol(protocol, frame.type(), frame.targetId(),
                frame.producerOrdinal(), frame.payload().length);
    }

    static void validateHeaderForProtocol(
            io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol,
            byte type, java.util.UUID targetId, int producerOrdinal, int payloadLength) {
        if (!isKnownType(type) || targetId == null || payloadLength < 0
                || payloadLength > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("invalid frame header");
        }
        if (protocol == io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0) {
            if (producerOrdinal != 0) {
                throw new IllegalArgumentException("LOOPBACK V0 frame must not carry an ordinal");
            }
            return;
        }
        boolean keepalive = type == FRAME_KEEPALIVE;
        if (keepalive) {
            if (!Frame.NO_TARGET.equals(targetId) || producerOrdinal != 0
                    || payloadLength != 0) {
                throw new IllegalArgumentException(
                        "MTLS V1 KEEPALIVE requires NO_TARGET, ordinal 0 and empty payload");
            }
        } else {
            if (Frame.NO_TARGET.equals(targetId)) {
                throw new IllegalArgumentException("MTLS V1 target frame requires a target");
            }
            ShuffleProducerGrant.requireOrdinal(producerOrdinal);
        }
    }

    static boolean isKnownType(byte type) {
        switch (type) {
            case FRAME_RECORD:
            case FRAME_EOS:
            case FRAME_ERROR:
            case FRAME_KEEPALIVE:
            case FRAME_CREDIT:
            case FRAME_OPEN:
                return true;
            default:
                return false;
        }
    }

    static String typeName(byte type) {
        switch (type) {
            case FRAME_RECORD: return "RECORD";
            case FRAME_EOS: return "EOS";
            case FRAME_ERROR: return "ERROR";
            case FRAME_KEEPALIVE: return "KEEPALIVE";
            case FRAME_CREDIT: return "CREDIT";
            case FRAME_OPEN: return "OPEN";
            default: return "UNKNOWN(" + Byte.toUnsignedInt(type) + ")";
        }
    }
}
