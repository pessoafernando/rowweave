package io.rowweave.sched.net;

import java.util.UUID;

/**
 * Data-plane wire frame (with multiplexing): {@code [type:byte][targetId:16(uuid)][len:int][payload]}.
 * See {@link FrameCodec} for the type semantics (RECORD / EOS / ERROR / KEEPALIVE / CREDIT / OPEN).
 *
 * <p>The frame header carries {@code targetId} ({@link UUID}, 16 bytes) as the routing key for multiplexing many
 * channels over one worker data port, matching Flink's 16-byte random receiverId / InputChannelID. The server
 * demultiplexes by targetId into the downstream stage's MultiProducerChannel. **A UUID is used rather than an
 * incrementing long**: global uniqueness does not depend on one master issuing monotonically increasing ids, so
 * HA failover, several masters or a restart cannot produce collisions. With an incrementing long, a new master
 * after failover restarting from 0 would collide with in-flight older targets and route frames across jobs to the
 * wrong consumer. When the legacy single port does not use multiplexing, targetId is {@link #NO_TARGET} (an
 * all-zero placeholder), which keeps one frame format instead of two branches.
 *
 * <p>An immutable value object. The payload holds ready application bytes: a serialized row for RECORD, a UTF-8
 * message for ERROR, empty for EOS and KEEPALIVE, and a 4-byte int credit value for CREDIT.
 */
public final class Frame {

    /** Placeholder routing key, an all-zero UUID, used when a single port does not multiplex. */
    public static final UUID NO_TARGET = new UUID(0L, 0L);

    private final byte type;
    private final UUID targetId;
    private final int producerOrdinal;
    private final byte[] payload;
    private final ShuffleIngressResidentBudget.Reservation ingressReservation;

    public Frame(byte type, UUID targetId, byte[] payload) {
        this(type, targetId, 0, payload, null);
    }

    /** MTLS V1 frame constructor; ordinal identifies one logical producer within a target. */
    public Frame(byte type, UUID targetId, int producerOrdinal, byte[] payload) {
        this(type, targetId, producerOrdinal, payload, null);
    }

    Frame(byte type, UUID targetId, byte[] payload,
          ShuffleIngressResidentBudget.Reservation ingressReservation) {
        this(type, targetId, 0, payload, ingressReservation);
    }

    Frame(byte type, UUID targetId, int producerOrdinal, byte[] payload,
          ShuffleIngressResidentBudget.Reservation ingressReservation) {
        this.type = type;
        this.targetId = targetId == null ? NO_TARGET : targetId;
        this.producerOrdinal = producerOrdinal;
        this.payload = payload;
        this.ingressReservation = ingressReservation;
    }

    /** Constructor for a single port that does not multiplex; the target id is the zero placeholder. */
    public Frame(byte type, byte[] payload) {
        this(type, NO_TARGET, payload);
    }

    public byte type() {
        return type;
    }

    /** Routing key used for multiplexing; {@link #NO_TARGET} means multiplexing is off. */
    public UUID targetId() {
        return targetId;
    }

    /** 0 for LOOPBACK V0/control-only frames; 1..4096 for MTLS V1 target frames. */
    public int producerOrdinal() { return producerOrdinal; }

    public byte[] payload() {
        return payload;
    }

    /** Ownership of the decoded ingress buffer: idempotent, and never written to the wire. */
    void releaseIngressReservation() {
        if (ingressReservation != null) ingressReservation.release();
    }
}
