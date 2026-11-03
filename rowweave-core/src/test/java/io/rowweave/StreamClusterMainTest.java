package io.rowweave;

import io.rowweave.stream.spi.StreamSecretResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** product entrypoint supports combined process deployment. */
class StreamClusterMainTest {

    @Test
    void startUsesServiceLoadedProvidersAndReturnsOwnedRuntime(
            @TempDir Path root) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.stream.master.enable", "true");
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "main-combined");
        properties.setProperty("rowweave.stream.state.dir", root.toString());

        try (StreamClusterRuntime runtime =
                     StreamClusterMain.start(properties)) {
            assertEquals(1, runtime.workerEndpoints().size());
            assertEquals("127.0.0.1", runtime.masterEndpoint().host());
        }
    }

    @Test
    void configuredSignedDeploymentsCannotBeIgnored(@TempDir Path root) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.stream.master.enable", "true");
        properties.setProperty("rowweave.stream.worker.enable", "true");
        properties.setProperty("rowweave.stream.worker.id", "signed-combined");
        properties.setProperty("rowweave.stream.state.dir", root.toString());
        properties.setProperty("rowweave.deployment.count", "1");

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> StreamClusterMain.start(properties));
        assertTrue(failure.getMessage().contains(
                "rowweave.deployment.trustRoot"), failure.getMessage());
    }

    @Test
    void multipleWorkerSecretResolversFailClosed(@TempDir Path root)
            throws Exception {
        Path service = root.resolve("META-INF/services/")
                .resolve(StreamSecretResolver.class.getName());
        Files.createDirectories(service.getParent());
        Files.write(service, (FirstResolver.class.getName() + '\n'
                + SecondResolver.class.getName() + '\n')
                .getBytes(StandardCharsets.UTF_8));
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(
                new java.net.URL[] {root.toUri().toURL()}, original)) {
            Thread.currentThread().setContextClassLoader(loader);
            IllegalStateException failure = assertThrows(
                    IllegalStateException.class,
                    () -> StreamClusterMain.loadOptionalSingle(
                            StreamSecretResolver.class));
            assertTrue(failure.getMessage().contains(
                    "multiple Stream secret resolvers"));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    public static final class FirstResolver
            implements StreamSecretResolver {
        public FirstResolver() { }
        @Override public byte[] resolve(String reference) {
            return new byte[] {1};
        }
    }

    public static final class SecondResolver
            implements StreamSecretResolver {
        public SecondResolver() { }
        @Override public byte[] resolve(String reference) {
            return new byte[] {2};
        }
    }
}
