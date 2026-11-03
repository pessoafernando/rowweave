package io.rowweave.stream.cluster;

/** Bounded physical lane descriptor exchanged during task deployment. */
final class StreamTaskLaneSpec {
    private final String laneId;
    private final String host;
    private final int port;
    private final int maximumFrameBytes;
    private final long initialCreditBytes;

    private StreamTaskLaneSpec(String laneId, String host, int port,
            int maximumFrameBytes, long initialCreditBytes) {
        this.laneId = StreamControlMessage.text(laneId, "lane ID");
        if (port < 0 || port > 65_535 || maximumFrameBytes <= 0
                || initialCreditBytes < maximumFrameBytes) {
            throw new IllegalArgumentException(
                    "Stream task lane limits are invalid");
        }
        if (port > 0 && (host == null || host.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "Stream task lane host is required for a connected lane");
        }
        this.host = port == 0 ? "" : StreamControlMessage.text(host,
                "lane host");
        this.port = port;
        this.maximumFrameBytes = maximumFrameBytes;
        this.initialCreditBytes = initialCreditBytes;
    }

    static StreamTaskLaneSpec input(String laneId, int maximumFrameBytes,
            long initialCreditBytes) {
        return new StreamTaskLaneSpec(laneId, "", 0, maximumFrameBytes,
                initialCreditBytes);
    }

    static StreamTaskLaneSpec endpoint(String laneId, String host, int port,
            int maximumFrameBytes, long initialCreditBytes) {
        return new StreamTaskLaneSpec(laneId, host, port,
                maximumFrameBytes, initialCreditBytes);
    }

    String laneId() { return laneId; }
    String host() { return host; }
    int port() { return port; }
    int maximumFrameBytes() { return maximumFrameBytes; }
    long initialCreditBytes() { return initialCreditBytes; }
}
