package io.rowweave;

import io.rowweave.stream.api.StreamClusterConfig;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSecretResolver;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;

/** Product process entrypoint for Stream Master-only, Worker-only and combined roles. */
public final class StreamClusterMain {
    private StreamClusterMain() { }

    public static StreamClusterRuntime start(Properties properties) {
        Properties snapshot = snapshot(properties);
        StreamClusterConfig config =
                StreamClusterConfig.fromProperties(snapshot);
        return startConfigured(config, snapshot);
    }

    public static void main(String[] arguments) throws Exception {
        io.rowweave.cluster.StreamClusterProcessBootstrap.run(arguments);
    }

    /** Called only after the cluster package has captured the process proof. */
    public static void runCertified(String[] arguments,
            io.rowweave.cluster.ProcessBootstrapEnvelope processEnvelope)
            throws Exception {
        if (arguments != null && arguments.length != 0) {
            throw new IllegalArgumentException(
                    "StreamClusterMain is configured with rowweave.* system properties");
        }
        Properties snapshot = snapshot(System.getProperties());
        StreamClusterConfig config = StreamClusterConfig.fromProperties(
                snapshot, processEnvelope);
        StreamClusterRuntime runtime = startConfigured(config, snapshot);
        Runtime.getRuntime().addShutdownHook(new Thread(runtime::close,
                "rowweave-stream-cluster-shutdown"));
        for (io.rowweave.stream.api.StreamWorkerEndpoint endpoint
                : runtime.workerEndpoints()) {
            System.out.println("STREAM_WORKER_READY=" + endpoint.host()
                    + ':' + endpoint.port());
        }
        try {
            io.rowweave.stream.api.StreamWorkerEndpoint master =
                    runtime.masterEndpoint();
            System.out.println("STREAM_MASTER_READY=" + master.host()
                    + ':' + master.port());
        } catch (IllegalStateException disabled) {
            // Worker-only is an intentional product role.
        }
        System.out.flush();
        new CountDownLatch(1).await();
    }

    private static StreamClusterRuntime startConfigured(
            StreamClusterConfig config, Properties properties) {
        io.rowweave.cluster.rpc.ControlSecurityConfig security =
                config.loadControlSecurity();
        if (config.masterEnabled()) {
            security.requireLocalRole(
                    io.rowweave.cluster.rpc.ControlRole.MASTER);
        }
        if (config.workerEnabled()) {
            security.requireLocalRole(
                    io.rowweave.cluster.rpc.ControlRole.WORKER);
        }
        DeploymentRuntime deployments = DeploymentRuntime.load(
                properties, security, Runtime.getRuntime().maxMemory());
        if (VerifiedDeploymentSet.emptyDeploymentSetId().equals(
                deployments.deploymentSetId())) {
            deployments.close();
            return StreamClusterRuntime.start(config,
                    load(StreamSourceProvider.class),
                    load(StreamSinkProvider.class),
                    load(StreamTransactionalSinkProvider.class),
                    load(StreamFunctionProvider.class));
        }
        try {
            StreamSecretResolver resolver = config.workerEnabled()
                    ? loadOptionalSingle(StreamSecretResolver.class) : null;
            return StreamClusterRuntime.startSigned(
                    config, deployments, resolver);
        } catch (RuntimeException | Error failure) {
            try {
                deployments.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static Properties snapshot(Properties source) {
        if (source == null) {
            throw new IllegalArgumentException(
                    "Stream cluster properties are required");
        }
        Properties result = new Properties();
        result.putAll(source);
        return result;
    }

    private static <T> List<T> load(Class<T> type) {
        List<T> result = new ArrayList<>();
        for (T provider : ServiceLoader.load(type)) {
            if (provider == null) {
                throw new IllegalStateException(
                        "Stream provider loader returned null");
            }
            result.add(provider);
        }
        return result;
    }

    static <T> T loadOptionalSingle(Class<T> type) {
        T selected = null;
        for (T provider : ServiceLoader.load(type)) {
            if (provider == null || selected != null) {
                throw new IllegalStateException(
                        "multiple Stream secret resolvers are configured");
            }
            selected = provider;
        }
        return selected;
    }
}
