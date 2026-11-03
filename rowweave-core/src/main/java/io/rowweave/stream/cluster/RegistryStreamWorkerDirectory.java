package io.rowweave.stream.cluster;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Registry-backed Worker discovery with endpoint/incarnation revalidation. */
public final class RegistryStreamWorkerDirectory
        implements StreamWorkerDirectory {
    private static final String PREFIX = "__rowweave_stream_worker__";

    private final ServiceRegistry registry;
    private final ControlSecurityConfig security;

    public RegistryStreamWorkerDirectory(ServiceRegistry registry,
            ControlSecurityConfig security) {
        if (registry == null || security == null) {
            throw new IllegalArgumentException(
                    "Stream Worker registry and security are required");
        }
        this.registry = registry;
        this.security = security;
    }

    /** Publishes one exact Worker incarnation into its owning registry session. */
    public static void register(ServiceRegistry registry, String host, int port,
            StreamWorkerCapabilityProfile profile,
            RpcSecurityMetadata securityMetadata) {
        if (registry == null || profile == null || host == null
                || host.trim().isEmpty() || port < 1 || port > 65_535) {
            throw new IllegalArgumentException(
                    "Stream Worker registry record is invalid");
        }
        registry.register(new RegistryWorker(registryId(profile.workerId()),
                host, port, profile.taskSlots(), port,
                profile.workerIncarnation(), securityMetadata));
    }

    public static void deregister(ServiceRegistry registry, String workerId) {
        if (registry == null || workerId == null || workerId.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream Worker registry identity is required");
        }
        registry.deregister(registryId(workerId));
    }

    /** Removes only the exact incarnation; an older lease cannot erase a replacement. */
    public static void deregisterExact(ServiceRegistry registry,
            StreamWorkerCapabilityProfile profile) {
        if (registry == null || profile == null) {
            throw new IllegalArgumentException(
                    "Stream Worker registry identity is required");
        }
        String id = registryId(profile.workerId());
        RegistryWorker current = registry.worker(id);
        if (current != null && profile.workerIncarnation().equals(
                current.incarnationId())) {
            registry.deregister(id);
        }
    }

    @Override
    public List<RemoteStreamWorkerEndpoint> snapshot() {
        List<RemoteStreamWorkerEndpoint> result = new ArrayList<>();
        RpcSecurityMetadata expected = security.securityMetadata();
        for (RegistryWorker record : registry.liveWorkers()) {
            if (!record.workerId().startsWith(PREFIX)) continue;
            if (!java.util.Objects.equals(
                    expected, record.securityMetadata())) {
                throw new SecurityException(
                        "Stream Worker registry security identity mismatch");
            }
            RemoteStreamWorkerEndpoint endpoint =
                    RemoteStreamWorkerEndpoint.connect(
                            record.host(), record.port(), security);
            StreamWorkerCapabilityProfile profile = endpoint.profile();
            if (!profile.workerIncarnation().equals(
                    record.incarnationId())
                    || !registryId(profile.workerId()).equals(
                    record.workerId())) {
                throw new IllegalStateException(
                        "Stream Worker registry incarnation changed");
            }
            result.add(endpoint);
        }
        Collections.sort(result, Comparator.comparing(
                endpoint -> endpoint.profile().workerId()));
        return Collections.unmodifiableList(result);
    }

    private static String registryId(String workerId) {
        if (workerId == null || workerId.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream Worker identity is required");
        }
        return PREFIX + hex(sha256(workerId.getBytes(StandardCharsets.UTF_8)));
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte item : value) {
            result.append(Character.forDigit((item >>> 4) & 0xf, 16));
            result.append(Character.forDigit(item & 0xf, 16));
        }
        return result.toString();
    }
}
