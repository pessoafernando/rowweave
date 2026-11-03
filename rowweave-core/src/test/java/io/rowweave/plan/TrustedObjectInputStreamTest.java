package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FilterFunction;
import io.rowweave.Source;
import io.rowweave.MapFunction;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** An object graph must be measured, must have a root type, and must be resolved through an explicit trust context. */
class TrustedObjectInputStreamTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void localTrustedAcceptsApplicationInputButRootTypeIsExact() throws Exception {
        byte[] blob = serialize(new TestInput());
        AtomicLong charged = new AtomicLong();

        Source input = TrustedObjectInputStream.read(blob, Source.class,
                ObjectTrustContext.localTrusted(getClass().getClassLoader()),
                charged::addAndGet);

        assertEquals(TestInput.class, input.getClass());
        assertTrue(charged.get() >= 3L * blob.length);
        assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(blob, Sink.class,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }));
    }

    @Test
    void mtlsAcceptsExactCoreBuiltInAndRejectsSameInterfaceApplicationClass() throws Exception {
        byte[] builtIn = serialize(new FileSource(META, "/tmp/never-opened"));
        byte[] application = serialize(new TestInput());

        assertEquals(FileSource.class, TrustedObjectInputStream.read(builtIn, Source.class,
                ObjectTrustContext.mtlsBuiltIns(), ignored -> { }).getClass());
        InvalidClassException rejected = assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(application, Source.class,
                        ObjectTrustContext.mtlsBuiltIns(), ignored -> { }));
        assertTrue(rejected.getMessage().contains("trusted deployment"), rejected.getMessage());
    }

    @Test
    void dynamicProxyAndBroadJdkSerializableClassAreRejected() throws Exception {
        Source proxy = (Source) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Source.class}, new SerializableHandler());

        assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(serialize(proxy), Source.class,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }));
        InvalidClassException jdkRejected = assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(serialize(new FileHoldingInput()), Source.class,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }));
        assertTrue(jdkRejected.getMessage().contains("JDK class"), jdkRejected.getMessage());
    }

    @Test
    void depthAndArrayLimitsRejectBeforeMaterializingOversizedGraph() throws Exception {
        assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(serialize(new DeepInput(70)), Source.class,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }));
        assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(serialize(new BigArrayInput(1_000_001)), Source.class,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }));
    }

    @Test
    void concreteFunctionInterfaceAndTrailingBytesAreEnforced() throws Exception {
        FilterFunction filter = row -> true;
        byte[] blob = serialize(filter);
        assertTrue(TrustedObjectInputStream.read(blob, FilterFunction.class,
                ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { })
                instanceof FilterFunction);
        assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(blob, MapFunction.class,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }));

        byte[] trailing = java.util.Arrays.copyOf(blob, blob.length + 1);
        assertThrows(InvalidClassException.class,
                () -> TrustedObjectInputStream.read(trailing, FilterFunction.class,
                        ObjectTrustContext.localTrusted(getClass().getClassLoader()), ignored -> { }));
    }

    private static byte[] serialize(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static class TestInput implements Source {
        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    private static final class FileHoldingInput extends TestInput {
        private final File file = new File("not-allowed");
    }

    private static final class BigArrayInput extends TestInput {
        private final byte[] bytes;
        private BigArrayInput(int length) { bytes = new byte[length]; }
    }

    private static final class DeepInput extends TestInput {
        private final Link root;
        private DeepInput(int depth) {
            Link current = null;
            for (int i = 0; i < depth; i++) current = new Link(current);
            root = current;
        }
    }

    private static final class Link implements Serializable {
        private final Link next;
        private Link(Link next) { this.next = next; }
    }

    private static final class SerializableHandler implements InvocationHandler, Serializable {
        @Override public Object invoke(Object proxy, Method method, Object[] args) {
            if ("getSchema".equals(method.getName())) return META;
            if ("iterator".equals(method.getName())) return Collections.emptyIterator();
            return null;
        }
    }
}
