package io.rowweave.deployment;

import io.rowweave.incremental.IncrementalCaptureResponseCodec;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierCodec;
import io.rowweave.incremental.IncrementalMaterializationResponseCodec;
import io.rowweave.incremental.IncrementalSourceActionBudget;
import io.rowweave.incremental.IncrementalSourceActionExecutor;
import io.rowweave.incremental.IncrementalSourceCodecDescriptor;
import io.rowweave.incremental.IncrementalSourceProviderContract;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;
import io.rowweave.incremental.IncrementalSourceProviderKey;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/** Reusable signed source provider used by the F5 Worker control-path tests. */
public final class SignedIncrementalSourceProviderFixture {
    public static final String PROVIDER_CLASS =
            "rowweave.conformance.SignedIncrementalSourceProvider";
    private static final String CONTRACT_RESOURCE =
            "META-INF/rowweave/incremental-source/contracts/conformance.bin";
    private static volatile Artifact cached;

    private SignedIncrementalSourceProviderFixture() { }

    public static IncrementalSourceProviderContract contract() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.incremental", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1, 64),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID, 1,
                        IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                false,
                new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L, 1, 60_000L),
                IncrementalSourceProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    public static io.rowweave.DataSchema sourceDataSchema() {
        return new io.rowweave.DataSchema(
                new io.rowweave.Field("id", io.rowweave.DataType.LongType),
                new io.rowweave.Field("op", io.rowweave.DataType.StringType),
                new io.rowweave.Field(
                        "position", io.rowweave.DataType.LongType));
    }

    public static Session open(Path directory) throws Exception {
        Artifact artifact = artifact();
        Path jar = directory.resolve("signed-incremental.jar").toAbsolutePath();
        Files.write(jar, artifact.jar);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(artifact.jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(artifact.signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                "signed-incremental", artifact.jar.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                "signed-incremental", jar, artifact.jar.length, digest,
                "conformance-signer", signature.sign());
        VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "conformance-signer", artifact.signer.getPublic()),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L));
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceProviderAdmissionPolicy policy =
                IncrementalSourceProviderAdmissionPolicy.forTesting(
                        Collections.singletonList(
                                IncrementalSourceProviderAdmissionPolicy.entry(
                                        "signed-incremental", PROVIDER_CLASS,
                                        contract.providerKey(),
                                        IncrementalSourceProviderContractCodec
                                                .digest(contract))));
        IncrementalSourceProviderRegistry sourceRegistry =
                IncrementalSourceProviderRegistry.scanForTesting(
                        deployments, policy,
                        IncrementalSourceProviderRegistry.Role.WORKER);
        IncrementalSourceProviderRegistry staticSourceRegistry =
                IncrementalSourceProviderRegistry.scanForTesting(
                        deployments, policy,
                        IncrementalSourceProviderRegistry.Role.STATIC_ONLY);
        DurableBatchSinkProviderRegistry sinkRegistry =
                DurableBatchSinkProviderRegistry.scan(
                        deployments,
                        DurableBatchSinkProviderRegistry.Role.WORKER);
        return new Session(deployments, sinkRegistry, sourceRegistry,
                staticSourceRegistry);
    }

    public static final class Session implements AutoCloseable {
        private final VerifiedDeploymentSet deployments;
        private final DurableBatchSinkProviderRegistry sinkRegistry;
        private final IncrementalSourceProviderRegistry sourceRegistry;
        private final IncrementalSourceProviderRegistry staticSourceRegistry;

        private Session(VerifiedDeploymentSet deployments,
                        DurableBatchSinkProviderRegistry sinkRegistry,
                        IncrementalSourceProviderRegistry sourceRegistry,
                        IncrementalSourceProviderRegistry staticSourceRegistry) {
            this.deployments = deployments;
            this.sinkRegistry = sinkRegistry;
            this.sourceRegistry = sourceRegistry;
            this.staticSourceRegistry = staticSourceRegistry;
        }

        public DeploymentRuntime workerRuntime() {
            return DeploymentRuntime.forTesting(
                    deployments, sinkRegistry, sourceRegistry, true);
        }

        public DeploymentRuntime staticRuntime() {
            return DeploymentRuntime.forTesting(
                    deployments, sinkRegistry, staticSourceRegistry, false);
        }

        public boolean blockOnceConsumed() {
            try {
                Class<?> provider = Class.forName(
                        PROVIDER_CLASS, false,
                        deployments.classLoader("signed-incremental"));
                java.lang.reflect.Field field =
                        provider.getDeclaredField("BLOCK_ONCE");
                field.setAccessible(true);
                return !((java.util.concurrent.atomic.AtomicBoolean)
                        field.get(null)).get();
            } catch (ReflectiveOperationException failure) {
                throw new IllegalStateException(
                        "cannot observe block-once provider state", failure);
            }
        }

        @Override
        public void close() { deployments.close(); }
    }

    private static Artifact artifact() throws Exception {
        Artifact result = cached;
        if (result != null) return result;
        synchronized (SignedIncrementalSourceProviderFixture.class) {
            result = cached;
            if (result != null) return result;
            Path build = Files.createTempDirectory(
                    "rowweave-signed-incremental-provider-");
            try {
                byte[] jar = buildJar(build);
                KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
                generator.initialize(3072);
                cached = result = new Artifact(jar, generator.generateKeyPair());
                return result;
            } finally {
                deleteTree(build);
            }
        }
    }

    private static byte[] buildJar(Path directory) throws Exception {
        Path classes = directory.resolve("classes");
        Path source = directory.resolve("src").resolve(
                PROVIDER_CLASS.replace('.', '/') + ".java");
        Files.createDirectories(source.getParent());
        Files.createDirectories(classes);
        Files.write(source, providerSource().getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString()) != 0) {
            throw new AssertionError(
                    "signed incremental provider compilation failed");
        }
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                try {
                    entries.put(classes.relativize(file).toString()
                                    .replace('\\', '/'),
                            Files.readAllBytes(file));
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
        entries.put(IncrementalSourceProviderRegistry.SERVICE_RESOURCE,
                (PROVIDER_CLASS + '=' + CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(CONTRACT_RESOURCE,
                IncrementalSourceProviderContractCodec.encode(contract()));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream output = new JarOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry item = new JarEntry(entry.getKey());
                item.setTime(0L);
                output.putNextEntry(item);
                output.write(entry.getValue());
                output.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static String providerSource() {
        return "package rowweave.conformance;"
                + "public final class SignedIncrementalSourceProvider implements io.rowweave.IncrementalSourceProvider{"
                + "private static final java.util.concurrent.atomic.AtomicBoolean BLOCK_ONCE=new java.util.concurrent.atomic.AtomicBoolean(true);"
                + "public SignedIncrementalSourceProvider(){}"
                + "public io.rowweave.incremental.IncrementalSourceProviderContract contract(){return io.rowweave.deployment.SignedIncrementalSourceProviderFixture.contract();}"
                + "public byte[] canonicalizeConfiguration(io.rowweave.incremental.IncrementalSourceConfig c){return c.configurationBytes();}"
                + "public io.rowweave.incremental.IncrementalCaptureResponse captureTarget(io.rowweave.incremental.IncrementalSourceConfig c,io.rowweave.incremental.IncrementalCaptureRequest r){io.rowweave.incremental.IncrementalFrontier t=r.mode()==io.rowweave.incremental.IncrementalRunMode.BACKFILL_V1?r.requestedTarget():io.rowweave.incremental.IncrementalFrontier.int64Sequence(r.mode()==io.rowweave.incremental.IncrementalRunMode.INCREMENTAL_V1?20L:10L);return new io.rowweave.incremental.IncrementalCaptureResponse(t,Thread.currentThread().getName().getBytes(java.nio.charset.StandardCharsets.UTF_8));}"
                + "private static byte[] hash(byte[] b){try{return java.security.MessageDigest.getInstance(\"SHA-256\").digest(b);}catch(Exception e){throw new RuntimeException(e);}}"
                + "private static String hex(byte[] b){StringBuilder s=new StringBuilder();for(byte x:b){s.append(Character.forDigit((x>>>4)&15,16));s.append(Character.forDigit(x&15,16));}return s.toString();}"
                + "public io.rowweave.incremental.IncrementalMaterializationResponse materializeInterval(io.rowweave.incremental.IncrementalSourceConfig c,io.rowweave.incremental.CapturedTargetReceipt r){io.rowweave.incremental.IncrementalSourceProviderKey k=contract().providerKey();io.rowweave.BatchSourceSplitDescriptor d0=new io.rowweave.BatchSourceSplitDescriptor(1,k.connectorType(),k.connectorVersion(),\"p0\",io.rowweave.commit.SchemaFingerprint.of(io.rowweave.deployment.SignedIncrementalSourceProviderFixture.sourceDataSchema()),hex(hash(r.snapshotIdentity())),io.rowweave.BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);byte[] q=Thread.currentThread().getName().getBytes(java.nio.charset.StandardCharsets.UTF_8);java.util.List<io.rowweave.incremental.IncrementalMaterializedSplit> s;if(java.util.Arrays.equals(c.configurationBytes(),\"two-splits\".getBytes(java.nio.charset.StandardCharsets.UTF_8))){io.rowweave.BatchSourceSplitDescriptor d1=new io.rowweave.BatchSourceSplitDescriptor(1,k.connectorType(),k.connectorVersion(),\"p1\",io.rowweave.commit.SchemaFingerprint.of(io.rowweave.deployment.SignedIncrementalSourceProviderFixture.sourceDataSchema()),hex(hash(r.snapshotIdentity())),io.rowweave.BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);s=java.util.Arrays.asList(new io.rowweave.incremental.IncrementalMaterializedSplit(d0,q),new io.rowweave.incremental.IncrementalMaterializedSplit(d1,q));}else{s=java.util.Collections.singletonList(new io.rowweave.incremental.IncrementalMaterializedSplit(d0,q));}return new io.rowweave.incremental.IncrementalMaterializationResponse(k,r,hash(\"partitions\".getBytes(java.nio.charset.StandardCharsets.UTF_8)),s);}"
                + "public io.rowweave.SourceExecutor<io.rowweave.IncrementalSourceSplit> openSplit(final io.rowweave.IncrementalSourceSplit i){return new io.rowweave.SourceExecutor<io.rowweave.IncrementalSourceSplit>(i,i.getSchema()){private int n;public void open(){}public boolean hasNext(){return n<2;}public io.rowweave.DataRow next(){if(java.util.Arrays.equals(i.configuration().configurationBytes(),\"fail-read\".getBytes(java.nio.charset.StandardCharsets.UTF_8)))throw new IllegalStateException(\"injected incremental source failure\");if(java.util.Arrays.equals(i.configuration().configurationBytes(),\"block-read\".getBytes(java.nio.charset.StandardCharsets.UTF_8))||(java.util.Arrays.equals(i.configuration().configurationBytes(),\"block-once\".getBytes(java.nio.charset.StandardCharsets.UTF_8))&&BLOCK_ONCE.compareAndSet(true,false))){try{Thread.sleep(60000L);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(\"incremental source interrupted\",e);}}long b=i.capturedTarget().mode()==io.rowweave.incremental.IncrementalRunMode.SNAPSHOT_V1?0L:i.capturedTarget().from().sequence();io.rowweave.DataRow r=new io.rowweave.DataRow(3);r.set(0,Long.valueOf(n+1L));r.set(1,\"UPSERT\");r.set(2,Long.valueOf(b+n+1L));n++;return r;}public void close(){}};}"
                + "}";
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try { Files.deleteIfExists(path); }
                catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
    }

    private static final class Artifact {
        private final byte[] jar;
        private final KeyPair signer;

        private Artifact(byte[] jar, KeyPair signer) {
            this.jar = jar;
            this.signer = signer;
        }
    }
}
