package io.rowweave.stream.cluster;

/** Same-JVM Master transport with no listener, socket or transport frame copy. */
final class InProcessStreamProductControlTransport
        implements StreamProductControlTransport {
    private final StreamProductJobMasterServer master;

    InProcessStreamProductControlTransport(
            StreamProductJobMasterServer master) {
        if (master == null) {
            throw new IllegalArgumentException(
                    "in-process Stream Master is required");
        }
        this.master = master;
    }

    @Override
    public StreamProductJobView call(
            StreamProductControlWire.Request request) {
        return SocketStreamProductControlTransport.requireSuccess(
                master.dispatchInProcess(request));
    }
}
