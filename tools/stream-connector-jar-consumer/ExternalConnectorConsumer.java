package external;

import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;
import io.rowweave.stream.spi.StreamSinkWriter;
import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceEvent;
import io.rowweave.stream.spi.StreamSourceProvider;
import io.rowweave.stream.spi.StreamSourceReader;
import io.rowweave.stream.spi.StreamSourceRecord;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;

import java.io.InputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Compiled and run with only the published core JAR on the classpath. */
public final class ExternalConnectorConsumer {
    private ExternalConnectorConsumer() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("core and consumer JARs required");
        }
        verifyJava8Jar(args[0]);
        verifyJava8Jar(args[1]);
        byte[] frame = StreamConnectorConfiguration.of(
                Collections.singletonMap("mode", "bounded"),
                Collections.<String, String>emptyMap()).encode();
        if (!"bounded".equals(StreamConnectorConfiguration.decode(frame)
                .options().get("mode"))) {
            throw new AssertionError("public configuration ABI changed");
        }
        if (StreamSourceEvent.endOfInput().kind()
                != StreamSourceEvent.Kind.END_OF_INPUT) {
            throw new AssertionError("public bounded event ABI changed");
        }
        if (!StreamJobStatus.FINISHED.terminal()
                || StreamJobStatus.FINISHED.wireId() != 9) {
            throw new AssertionError("public bounded job status ABI changed");
        }
        if (!(new Source() instanceof StreamSourceProvider)
                || !(new EagerSink() instanceof StreamSinkProvider)
                || !(new TransactionalSink()
                        instanceof StreamTransactionalSinkProvider)) {
            throw new AssertionError("public provider ABI changed");
        }
    }

    private static void verifyJava8Jar(String path) throws Exception {
        int classes = 0;
        try (JarFile jar = new JarFile(path)) {
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!entry.getName().endsWith(".class")) continue;
                classes++;
                try (InputStream input = jar.getInputStream(entry)) {
                    byte[] header = new byte[8];
                    int read = 0;
                    while (read < header.length) {
                        int count = input.read(header, read, header.length - read);
                        if (count < 0) break;
                        read += count;
                    }
                    int major = ((header[6] & 0xff) << 8)
                            | (header[7] & 0xff);
                    if (read != 8 || header[0] != (byte) 0xca
                            || header[1] != (byte) 0xfe
                            || header[2] != (byte) 0xba
                            || header[3] != (byte) 0xbe
                            || major != 52) {
                        throw new AssertionError("non-Java-8 class: "
                                + entry.getName() + " major=" + major);
                    }
                }
            }
        }
        if (classes == 0) throw new AssertionError("empty JAR: " + path);
    }

    public static final class Source implements StreamSourceProvider {
        public String providerKey() { return "external.source"; }
        public String contractDigest() { return "test-only"; }
        public StreamSourceReader open(StreamSourceContext context) {
            return new StreamSourceReader() {
                private boolean ended;
                public StreamSourceRecord next() { return null; }
                public StreamSourceEvent nextEvent() {
                    if (ended) return null;
                    ended = true;
                    return StreamSourceEvent.endOfInput();
                }
                public byte[] checkpointPositionFrame() {
                    return new byte[] {(byte) (ended ? 1 : 0)};
                }
                public void cancel() { }
                public void close() { }
            };
        }
    }

    public static final class EagerSink implements StreamSinkProvider {
        public String providerKey() { return "external.eager"; }
        public String contractDigest() { return "test-only"; }
        public StreamSinkWriter open(StreamSinkContext context) {
            return frame -> { };
        }
    }

    public static final class TransactionalSink
            implements StreamTransactionalSinkProvider {
        public String providerKey() { return "external.transactional"; }
        public String contractDigest() { return "test-only"; }
        public StreamTransactionalSinkWriter open(
                StreamTransactionalSinkContext context) {
            return new StreamTransactionalSinkWriter() {
                public void write(byte[] frame) { }
                public byte[] prepare(String identity) {
                    return identity.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                public StreamTransactionStatus query(String identity) {
                    return StreamTransactionStatus.UNKNOWN;
                }
                public void commit(String identity) { }
                public void finalizeTransaction(String identity) { }
                public void abort(String identity) { }
                public void close() { }
            };
        }
    }
}
