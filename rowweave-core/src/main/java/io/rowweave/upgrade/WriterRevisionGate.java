package io.rowweave.upgrade;

import io.rowweave.cluster.rpc.ProtocolHelloV1;

import java.util.Collection;

/** Evaluates the store-bound writer revision supported by every writable cluster member. */
public final class WriterRevisionGate {
    private WriterRevisionGate() { }

    public static WriterRevisionDecision evaluate(
            StoreFormatHeader activeHeader,
            Collection<ProtocolHelloV1> writableNodes) {
        if (activeHeader == null || writableNodes == null
                || writableNodes.isEmpty()) {
            throw rejected("WRITER_SET_REQUIRED",
                    "active header and writable nodes are required");
        }
        int minimum = 0;
        int maximum = Integer.MAX_VALUE;
        for (ProtocolHelloV1 hello : writableNodes) {
            if (hello == null) {
                throw rejected("WRITER_HELLO_REQUIRED",
                        "writable node hello is required");
            }
            if (hello.protocolEpoch() != activeHeader.protocolEpoch()) {
                throw rejected("WRITER_PROTOCOL_EPOCH_MISMATCH",
                        "writable node protocol epoch differs from the active store");
            }
            if (!hello.manifestDigest().equals(activeHeader.manifestDigest())) {
                throw rejected("WRITER_MANIFEST_DIGEST_MISMATCH",
                        "writable node manifest differs from the active store");
            }
            minimum = Math.max(minimum, hello.writerRevisions().minimum());
            maximum = Math.min(maximum, hello.writerRevisions().maximum());
        }
        if (minimum > maximum) {
            throw rejected("WRITER_REVISION_INTERSECTION_EMPTY",
                    "writable node revision ranges do not overlap");
        }
        WriterRevisionDecision decision = new WriterRevisionDecision(
                activeHeader.activeRevision(), minimum, maximum);
        decision.requireCurrent();
        return decision;
    }

    private static WriterRevisionRejectedException rejected(
            String code, String message) {
        return new WriterRevisionRejectedException(code, message);
    }
}
