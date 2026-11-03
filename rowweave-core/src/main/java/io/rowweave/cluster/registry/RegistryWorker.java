package io.rowweave.cluster.registry;

import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.ProtocolHelloV1;

import java.util.Objects;

/**
 * A worker record in the registry: an immutable value object whose identity is {@code workerId} (same id means the same worker,
 * and re-registering overwrites its resources). It carries the minimum the scheduler needs: id, data-plane and RPC endpoints (host, port) and slots.
 *
 * <p>Distinguished from {@code MasterServer.WorkerInfo}: that one holds the BeatHeartbeat used for Master's in-memory liveness decision, while this class is the
 * registry layer's pure resource record, which Master reads through {@link ServiceRegistry#liveWorkers()} to schedule, since worker registration goes through the registry.
 * Election roles reuse this type (workerId is a well-known role id, host and port are the master endpoint; slots and dataPort are
 * combined into the 64-bit term token, while the field semantics for an ordinary Worker are unchanged).
 */
public final class RegistryWorker {

    private final String workerId;
    private final String host;
    private final int port;
    private final int slots;
    private final int dataPort; // Worker's single data port (mux; -1 means mux OFF).
    private final String incarnationId; // Worker process generation; null means a legacy or non-Worker role.
    private final RpcSecurityMetadata securityMetadata;
    private final DataPlaneRegistration dataPlaneRegistration;
    private final ProtocolHelloV1 protocolHello;

    public RegistryWorker(String workerId, String host, int port, int slots) {
        this(workerId, host, port, slots, -1, null, null, null);
    }

    public RegistryWorker(String workerId, String host, int port, int slots, int dataPort) {
        this(workerId, host, port, slots, dataPort, null, null, null);
    }

    public RegistryWorker(String workerId, String host, int port, int slots, int dataPort,
                          String incarnationId) {
        this(workerId, host, port, slots, dataPort, incarnationId, null, null);
    }

    public RegistryWorker(String workerId, String host, int port, int slots, int dataPort,
                          String incarnationId, RpcSecurityMetadata securityMetadata) {
        this(workerId, host, port, slots, dataPort, incarnationId,
                securityMetadata, null);
    }

    public RegistryWorker(String workerId, String host, int port, int slots, int dataPort,
                          String incarnationId, RpcSecurityMetadata securityMetadata,
                          DataPlaneRegistration dataPlaneRegistration) {
        this(workerId, host, port, slots, dataPort, incarnationId,
                securityMetadata, dataPlaneRegistration, null);
    }

    public RegistryWorker(String workerId, String host, int port, int slots, int dataPort,
                          String incarnationId, RpcSecurityMetadata securityMetadata,
                          DataPlaneRegistration dataPlaneRegistration,
                          ProtocolHelloV1 protocolHello) {
        if (protocolHello != null && incarnationId == null) {
            throw new IllegalArgumentException(
                    "protocol hello requires an exact worker incarnation");
        }
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.slots = slots;
        this.dataPort = dataPort;
        this.incarnationId = incarnationId;
        this.securityMetadata = securityMetadata;
        this.dataPlaneRegistration = dataPlaneRegistration;
        this.protocolHello = protocolHello;
    }

    public String workerId() { return workerId; }
    public String host() { return host; }
    public int port() { return port; }
    public int slots() { return slots; }
    /** The worker's single data port (-1 means mux OFF). */
    public int dataPort() { return dataPort; }
    /** Process-level generation token for the Worker; null means the old protocol or a non-Worker registry role. */
    public String incarnationId() { return incarnationId; }
    public RpcSecurityMetadata securityMetadata() { return securityMetadata; }
    /** Candidate data identity; an MTLS leader must still authenticate REGISTER for this incarnation. */
    public DataPlaneRegistration dataPlaneRegistration() { return dataPlaneRegistration; }
    /** Exact public-v1 capability advertisement for this worker incarnation. */
    public ProtocolHelloV1 protocolHello() { return protocolHello; }

    @Override
    public String toString() {
        return "RegistryWorker{" + workerId + "@" + host + ":" + port + " slots=" + slots
                + (dataPort > 0 ? " dataPort=" + dataPort : "")
                + (incarnationId != null ? " incarnation=" + incarnationId : "") + "}";
    }
}
