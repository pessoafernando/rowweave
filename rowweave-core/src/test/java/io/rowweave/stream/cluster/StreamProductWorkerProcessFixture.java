package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.EagerStreamSinkWriter;
import io.rowweave.stream.runtime.StreamSinkOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

/** Isolated product Worker fixture; orchestration is entirely production code. */
public final class StreamProductWorkerProcessFixture {
    private static final int RECORDS = 1_000;
    private static final String SOURCE_CONTRACT = sha256("source-contract");
    private static final String SINK_CONTRACT = sha256("sink-contract");

    private StreamProductWorkerProcessFixture() { }

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException("role and exchange directory required");
        }
        String role = arguments[0];
        Path exchange = Paths.get(arguments[1]);
        StreamWorkerCapabilityProfile profile = new StreamWorkerCapabilityProfile(
                "worker-" + role, "incarnation-" + role, 2, 1,
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        PortableStreamPlan
                                .NODE_EXECUTION_CONTRACT_CAPABILITY,
                        StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                        StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                        StreamWorkerCapabilityProfile
                                .CHECKPOINT_TRANSFER_CAPABILITY),
                8, 16, 8_192, 8_388_608L, 32);
        DurableStreamSourceProvider source = role.startsWith("source")
                ? new CountingSourceProvider() : null;
        EagerStreamSinkProvider sink = role.startsWith("sink")
                ? new FileDigestSinkProvider(exchange.resolve("result")) : null;
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                profile,
                source == null ? Collections.emptyMap()
                        : Collections.singletonMap(source.providerKey(), source),
                sink == null ? Collections.emptyMap()
                        : Collections.singletonMap(sink.providerKey(), sink),
                exchange.resolve("checkpoint-store"))) {
            Files.write(exchange.resolve(role + ".identity"),
                    ManagementFactory.getRuntimeMXBean().getName()
                            .getBytes(StandardCharsets.UTF_8));
            Files.write(exchange.resolve(role + ".port"),
                    Integer.toString(server.port()).getBytes(StandardCharsets.US_ASCII));
            while (!Files.exists(exchange.resolve("shutdown"))) {
                Thread.sleep(5L);
            }
        }
    }

    private static final class CountingSourceProvider
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return "source-v1"; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private final Object wakeupMonitor = new Object();
                private int next;
                private long wakeupGeneration;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next < RECORDS) {
                        int value = next++;
                        return StreamSourceRecord.of(
                                new byte[] {(byte) value},
                                position(next));
                    }
                    synchronized (wakeupMonitor) {
                        long observed = wakeupGeneration;
                        while (observed == wakeupGeneration) {
                            wakeupMonitor.wait();
                        }
                    }
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return position(next);
                }
                @Override public void wakeUp() {
                    synchronized (wakeupMonitor) {
                        wakeupGeneration++;
                        wakeupMonitor.notifyAll();
                    }
                }
                @Override public void cancel() { wakeUp(); }
                @Override public void close() { wakeUp(); }
            };
        }
    }

    private static byte[] position(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    private static final class FileDigestSinkProvider
            implements EagerStreamSinkProvider {
        private final Path result;
        private FileDigestSinkProvider(Path result) { this.result = result; }
        @Override public String providerKey() { return "sink-v1"; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public EagerStreamSinkWriter open(StreamSinkOpenRequest request) {
            return new EagerStreamSinkWriter() {
                private final MessageDigest digest = digest();
                private int count;
                @Override public synchronized void write(byte[] recordFrame)
                        throws Exception {
                    digest.update(recordFrame);
                    count++;
                    if (count == RECORDS) {
                        Files.write(result, (count + ":" + hex(digest.digest()))
                                .getBytes(StandardCharsets.US_ASCII));
                    }
                }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String sha256(String value) {
        return hex(digest().digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }
}
