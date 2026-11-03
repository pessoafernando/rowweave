package io.rowweave.plan;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSink;
import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkConfigurationEnvelope;
import io.rowweave.DurableSinkConfigurationEnvelopeCodec;
import io.rowweave.Field;
import io.rowweave.DiscardSink;
import io.rowweave.PrintSink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** provider config uses native -4 wire, never Java serialization. */
class ProviderSinkConfigurationEnvelopeTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void providerProxyRoundTripsThroughNativeEnvelopeAndScannerView() {
        ProviderBatchSink output = output(new byte[]{3, 1, 4});
        PlanGraph graph = graph(output);

        byte[] wire = graph.toBytes();
        PlanGraphEnvelopeScanner.Scan scan = PlanGraphEnvelopeScanner.scan(wire);
        assertEquals(1, scan.providerSinkEnvelopes().size());
        PlanGraphEnvelopeScanner.ProviderSinkEnvelope envelope =
                scan.providerSinkEnvelopes().get(0);
        assertEquals(1, envelope.nodeIndex());
        assertEquals(output.batchSinkDescriptor(), envelope.descriptor());
        assertEquals(output.configurationCodec(), envelope.configurationCodec());
        assertArrayEquals(output.configurationBytes(), envelope.configurationBytes());
        assertEquals(0L, scan.objectBlobBytes(),
                "provider config must not be charged or decoded as an OIS blob");

        PlanGraph decoded = PlanGraph.fromBytes(wire);
        assertTrue(decoded.node(1).output() instanceof ProviderBatchSink);
        ProviderBatchSink rebuilt =
                (ProviderBatchSink) decoded.node(1).output();
        assertEquals(output.batchSinkDescriptor(), rebuilt.batchSinkDescriptor());
        assertEquals(output.configurationCodec(), rebuilt.configurationCodec());
        assertArrayEquals(output.configurationBytes(), rebuilt.configurationBytes());
    }

    @Test
    void envelopeCodecRejectsChecksumTrailingAndPayloadBounds() {
        DurableSinkCodecDescriptor codec =
                new DurableSinkCodecDescriptor("test.config", 1, 4);
        byte[] frame = DurableSinkConfigurationEnvelopeCodec.encode(
                codec, new byte[]{1, 2, 3, 4});
        DurableSinkConfigurationEnvelope decoded =
                DurableSinkConfigurationEnvelopeCodec.decode(frame);
        assertEquals(codec, decoded.configurationCodec());
        assertArrayEquals(new byte[]{1, 2, 3, 4}, decoded.configurationBytes());

        byte[] corrupt = frame.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> DurableSinkConfigurationEnvelopeCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> DurableSinkConfigurationEnvelopeCodec.decode(
                        Arrays.copyOf(frame, frame.length + 1)));
        assertThrows(IllegalArgumentException.class,
                () -> DurableSinkConfigurationEnvelopeCodec.encode(
                        codec, new byte[5]));
    }

    @Test
    void artifactDeclaresProviderEnvelopeReaderFeature() {
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap(
                "provider-config", graph(output(new byte[0])));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_SINK_PROVIDER_CONFIG_ENVELOPE));
    }

    @Test
    void durableWriterRejectsCustomOutputBeforeSerializationHook() {
        AtomicBoolean hookCalled = HookedOutput.HOOK_CALLED;
        hookCalled.set(false);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.sink(META, 0, new HookedOutput()).build()));

        assertThrows(IllegalArgumentException.class,
                () -> graph.toDurableBytes(ObjectTrustContext.localTrusted(),
                        ignored -> { }));
        assertEquals(false, hookCalled.get());
    }

    @Test
    void durableArtifactCompilerRejectsCustomOutputBeforeSerializationHook()
            throws Exception {
        HookedOutput.HOOK_CALLED.set(false);
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.sink(META, 0, new HookedOutput()).build()));

        java.lang.reflect.Method durableCompiler =
                LegacyPlanArtifactAdapter.class.getMethod(
                        "wrapDurable", String.class, PlanGraph.class,
                        ObjectTrustContext.class,
                        java.util.function.LongConsumer.class);
        java.lang.reflect.InvocationTargetException rejected = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> durableCompiler.invoke(null, "durable-hook", graph,
                        ObjectTrustContext.localTrusted(),
                        (java.util.function.LongConsumer) ignored -> { }));
        assertTrue(rejected.getCause() instanceof IllegalArgumentException);
        assertEquals(false, HookedOutput.HOOK_CALLED.get());
    }

    @Test
    void durableScannerAcceptsOnlyExactCoreLegacyOutputRoots() {
        byte[] discard = new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()))
                .toBytes();
        PlanGraphEnvelopeScanner.scanDurable(discard, ignored -> { });

        byte[] print = new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.sink(META, 0, new PrintSink(META)).build()))
                .toBytes();
        assertThrows(IllegalArgumentException.class,
                () -> PlanGraphEnvelopeScanner.scanDurable(
                        print, ignored -> { }));
    }

    @Test
    void legacyRootScannerRejectsNestedDeploymentHookBeforeReadObject(
            @TempDir Path temp) throws Exception {
        System.clearProperty("rowweave.test.legacy-root-hook");
        Path sources = temp.resolve("src");
        Path classes = temp.resolve("classes");
        Files.createDirectories(sources.resolve("io/rowweave"));
        Files.createDirectories(sources.resolve("evil"));
        Files.createDirectories(classes);
        Files.write(sources.resolve("io/rowweave/DiscardSink.java"),
                ("package io.rowweave;"
                        + "public final class DiscardSink implements java.io.Serializable {"
                        + "private static final long serialVersionUID=1L;"
                        + "private final evil.ReadHook hook=new evil.ReadHook();}")
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(sources.resolve("evil/ReadHook.java"),
                ("package evil;public final class ReadHook implements java.io.Serializable {"
                        + "private static final long serialVersionUID=1L;"
                        + "private void readObject(java.io.ObjectInputStream in) throws Exception {"
                        + "System.setProperty(\"rowweave.test.legacy-root-hook\",\"called\");"
                        + "in.defaultReadObject();}}")
                        .getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertEquals(0, compiler.run(null, null, null, "-source", "8", "-target", "8",
                "-d", classes.toString(),
                sources.resolve("io/rowweave/DiscardSink.java").toString(),
                sources.resolve("evil/ReadHook.java").toString()));
        byte[] blob;
        try (URLClassLoader loader = new URLClassLoader(
                new URL[]{classes.toUri().toURL()}, null)) {
            Object spoof = Class.forName(
                    "io.rowweave.DiscardSink", true, loader)
                    .getConstructor().newInstance();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(spoof);
            }
            blob = bytes.toByteArray();
        }

        assertThrows(IllegalArgumentException.class,
                () -> SerializedRootClassScanner.requireExactOutputRoot(
                        blob, 0, blob.length));
        assertEquals(null, System.getProperty("rowweave.test.legacy-root-hook"));
    }

    private static PlanGraph graph(ProviderBatchSink output) {
        return new PlanGraph(Arrays.asList(
                PlanNode.placeholderSource(META),
                PlanNode.sink(META, 0, output).build()));
    }

    private static ProviderBatchSink output(byte[] config) {
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "test.adversarial", 1,
                SchemaFingerprint.of(META), "test:destination", "test:domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        return new ProviderBatchSink(META, descriptor,
                new DurableSinkCodecDescriptor("test.config", 1, 1024), config);
    }

    private static final class HookedOutput implements BatchSink {
        private static final long serialVersionUID = 1L;
        private static final AtomicBoolean HOOK_CALLED = new AtomicBoolean();

        @Override public DataSchema getSchema() { return META; }
        @Override public void collect(io.rowweave.DataRow row) { }
        @Override public String exclusiveDestinationId() {
            return "test:hooked";
        }
        @Override public BatchSinkDescriptor batchSinkDescriptor() {
            return new BatchSinkDescriptor(
                    BatchSinkDescriptor.CURRENT_VERSION, "test.hooked", 1,
                    SchemaFingerprint.of(META), "test:hooked", "test:domain",
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        }
        private Object writeReplace() {
            HOOK_CALLED.set(true);
            return this;
        }
    }
}
