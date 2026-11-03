package io.rowweave.stream.cluster;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/** Independent product Master fixture; lifecycle and dispatch remain production code. */
public final class StreamProductMasterProcessFixture {
    private StreamProductMasterProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "exchange directory and positive Master term required");
        }
        Path exchange = Paths.get(arguments[0]);
        long term = Long.parseLong(arguments[1]);
        Path sourcePort = Files.exists(exchange.resolve("use-source-b"))
                ? exchange.resolve("source-b.port")
                : exchange.resolve("source.port");
        RemoteStreamWorkerEndpoint source = RemoteStreamWorkerEndpoint.connect(
                "127.0.0.1", port(sourcePort));
        RemoteStreamWorkerEndpoint sink = RemoteStreamWorkerEndpoint.connect(
                "127.0.0.1", port(exchange.resolve("sink.port")));
        RemoteStreamSourceSinkRunFactory runs =
                new RemoteStreamSourceSinkRunFactory(
                        exchange.resolve("run-authority"), term,
                        "master-incarnation-" + term,
                        Arrays.asList(source, sink),
                        exchange.resolve("checkpoint-store"));
        String prefix = "master-" + term;
        try (StreamProductJobMasterServer server =
                     new StreamProductJobMasterServer(
                             exchange.resolve("job-authority"), runs)) {
            Files.write(exchange.resolve(prefix + ".identity"),
                    ManagementFactory.getRuntimeMXBean().getName()
                            .getBytes(StandardCharsets.UTF_8));
            Files.write(exchange.resolve(prefix + ".port"),
                    Integer.toString(server.port())
                            .getBytes(StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve(prefix + ".shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static int port(Path file) throws Exception {
        return Integer.parseInt(new String(
                Files.readAllBytes(file), StandardCharsets.US_ASCII));
    }
}
