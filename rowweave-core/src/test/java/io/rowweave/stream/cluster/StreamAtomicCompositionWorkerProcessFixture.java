package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.StreamWorkerRuntime;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamWorkerConfig;
import io.rowweave.stream.spi.StreamFunctionProvider;
import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceProvider;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Public SDK Worker fixture for the composed-DAG cluster gate. */
public final class StreamAtomicCompositionWorkerProcessFixture {
    private StreamAtomicCompositionWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "role and exchange directory required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        List<StreamSourceProvider> sources = "source".equals(role)
                ? Arrays.asList(
                new StreamAtomicCompositionProductTest.Source(
                        StreamAtomicCompositionProductTest.SOURCE, 1L, 2L),
                new StreamAtomicCompositionProductTest.Source(
                        StreamAtomicCompositionProductTest.SOURCE_RIGHT,
                        3L, 4L))
                : Collections.emptyList();
        List<StreamSinkProvider> sinks;
        if ("sink-1".equals(role)) {
            sinks = Collections.singletonList(new FileSink(
                    "atomic-composition-sink-1",
                    exchange.resolve("atomic-composition-result-1")));
        } else if ("sink-2".equals(role)) {
            sinks = Collections.singletonList(new FileSink(
                    "atomic-composition-sink-2",
                    exchange.resolve("atomic-composition-result-2")));
        } else {
            sinks = Collections.emptyList();
        }
        List<StreamFunctionProvider> functions = role.startsWith("compute-")
                ? Collections.singletonList(new Functions(
                exchange.resolve("atomic-composition-functions-" + role)))
                : Collections.emptyList();
        StreamWorkerConfig config = StreamWorkerConfig.defaults(
                "atomic-composition-" + role,
                exchange.resolve("atomic-composition-master"));
        try (StreamWorkerRuntime worker = RowWeave.startStreamWorker(
                config, sources, sinks, Collections.emptyList(), functions)) {
            Files.write(exchange.resolve(
                            "atomic-composition-" + role + ".port"),
                    Integer.toString(worker.endpoint().port()).getBytes(
                            StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve(
                    "atomic-composition-shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static final class Functions implements StreamFunctionProvider {
        private final Path marker;

        private Functions(Path marker) {
            this.marker = marker;
        }

        @Override public String deploymentSetId() {
            return StreamAtomicCompositionProductTest.DEPLOYMENT;
        }

        @Override public Serializable resolve(
                FunctionDeploymentReference reference) {
            try {
                Files.write(marker, (reference.deploymentId() + '\n')
                                .getBytes(StandardCharsets.US_ASCII),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (java.io.IOException failure) {
                throw new IllegalStateException(failure);
            }
            if (reference.equals(StreamAtomicCompositionProductTest.function(
                    "flat-map", FunctionAbi.FLAT_MAP_V1,
                    StreamAtomicCompositionProductTest.PassFlatMap.class))) {
                return new StreamAtomicCompositionProductTest.PassFlatMap(
                        new AtomicInteger());
            }
            if (reference.equals(StreamAtomicCompositionProductTest.function(
                    "process", FunctionAbi.KEYED_PROCESS_V1,
                    StreamAtomicCompositionProductTest.CountProcess.class))) {
                return new StreamAtomicCompositionProductTest.CountProcess(
                        new AtomicInteger());
            }
            if (reference.equals(StreamAtomicCompositionProductTest.function(
                    "async", FunctionAbi.STREAM_ASYNC_V1,
                    StreamAtomicCompositionProductTest.PassAsync.class))) {
                return new StreamAtomicCompositionProductTest.PassAsync(
                        new AtomicInteger());
            }
            if (reference.equals(StreamAtomicCompositionProductTest.function(
                    "window", FunctionAbi.STREAM_WINDOW_AGGREGATE_V1,
                    StreamAtomicCompositionProductTest.PassWindow.class))) {
                return new StreamAtomicCompositionProductTest.PassWindow(
                        new AtomicInteger());
            }
            if (reference.equals(StreamAtomicCompositionProductTest.function(
                    "co-process", FunctionAbi.KEYED_CO_PROCESS_V1,
                    StreamAtomicCompositionProductTest.LeftOnlyCoProcess
                            .class))) {
                return new StreamAtomicCompositionProductTest
                        .LeftOnlyCoProcess(new AtomicInteger(),
                        new AtomicInteger());
            }
            throw new IllegalArgumentException("unknown function");
        }
    }

    private static final class FileSink implements StreamSinkProvider {
        private final String providerKey;
        private final Path result;
        private final StreamDataRowCodec codec = new StreamDataRowCodec(
                StreamAtomicCompositionProductTest.schema());

        private FileSink(String providerKey, Path result) {
            this.providerKey = providerKey;
            this.result = result;
        }

        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() {
            return StreamAtomicCompositionProductTest.SINK_CONTRACT;
        }

        @Override public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> {
                long value = codec.decode(frame).getLong(0);
                Files.write(result, (Long.toString(value) + '\n').getBytes(
                                StandardCharsets.US_ASCII),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            };
        }
    }
}
