package io.rowweave.deployment;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DurableBatchSinkProvider;
import io.rowweave.DurableBatchSinkProviderContract;
import io.rowweave.DurableBatchSinkProviderContractCodec;
import io.rowweave.DurableSinkAttemptBudget;
import io.rowweave.DurableSinkCleanupContract;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.internal.ProviderActionSupervisor;

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

/** Reusable signed test deployment that executes the adversarial provider itself. */
public final class SignedAdversarialProviderFixture {
    public static final String PROVIDER_CLASS =
            "rowweave.conformance.SignedAdversarialProvider";
    private static final String CONTRACT_RESOURCE =
            "META-INF/rowweave/durable-sink/contracts/adversarial.bin";
    private static volatile Artifact cached;

    private SignedAdversarialProviderFixture() { }

    public static DurableBatchSinkProviderContract contract() {
        return new DurableBatchSinkProviderContract(
                1,
                new DurableSinkProviderKey("test.adversarial", 1,
                        BatchSinkDescriptor.CapabilityProfile
                                .MANIFEST_ATOMIC_GROUP_V1),
                new DurableSinkCodecDescriptor("test.config", 1, 64 * 1024),
                new DurableSinkCodecDescriptor("test.locator", 1, 4096),
                new DurableSinkCodecDescriptor(
                        "rowweave.batch-sink-outcome", 1, 1024),
                DurableBatchSinkProviderContract.IdempotencyScope
                        .ATTEMPT_AND_PUBLICATION_V1,
                new DurableSinkCleanupContract(
                        1L, 16, 8, true,
                        DurableBatchSinkProviderContract.OrphanOwnership
                                .ATTEMPT_PRIVATE_V1,
                        DurableBatchSinkProviderContract.QuarantinePolicy
                                .PRESERVE_ON_UNKNOWN_OR_CONFLICT,
                        DurableBatchSinkProviderContract.SafeDeleteProof
                                .ABORT_DECISION_OR_UNREFERENCED_RETENTION_V1),
                new DurableSinkAttemptBudget(
                        1024L * 1024L, 8, 4096, 64 * 1024, 500L),
                DurableBatchSinkProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    public static Session open(Path directory) throws Exception {
        Artifact artifact = artifact();
        Path jar = directory.resolve("signed-adversarial.jar").toAbsolutePath();
        Files.write(jar, artifact.jar);
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(artifact.jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(artifact.signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                "signed-adversarial", artifact.jar.length, digest));
        DeploymentManifest manifest = new DeploymentManifest(
                "signed-adversarial", jar, artifact.jar.length, digest,
                "conformance-signer", signature.sign());
        VerifiedDeploymentSet deployments = VerifiedDeploymentSet.load(
                Collections.singletonList(manifest),
                Collections.singletonMap(
                        "conformance-signer", artifact.signer.getPublic()),
                DeploymentMemoryBudget.forTesting(16L * 1024L * 1024L));
        DurableBatchSinkProviderContract contract = contract();
        DurableBatchSinkProviderAdmissionPolicy policy =
                DurableBatchSinkProviderAdmissionPolicy.forTesting(
                        Collections.singletonList(
                                DurableBatchSinkProviderAdmissionPolicy.entry(
                                        "signed-adversarial", PROVIDER_CLASS,
                                        contract.providerKey(),
                                        DurableBatchSinkProviderContractCodec
                                                .digest(contract))));
        DurableBatchSinkProviderRegistry registry =
                DurableBatchSinkProviderRegistry.scanForTesting(
                        deployments, policy,
                        DurableBatchSinkProviderRegistry.Role.WORKER);
        DurableBatchSinkProvider provider = registry.loadProvider(
                contract.providerKey(),
                ProviderActionSupervisor.processWide(8),
                System.nanoTime() + 5_000_000_000L);
        return new Session(deployments, registry, provider, contract);
    }

    public static ProviderBatchSink output(
            DataSchema dataSchema, Path root, String member) {
        String canonicalRoot = root.toAbsolutePath().normalize().toString();
        byte[] config = (canonicalRoot + "\n" + member)
                .getBytes(StandardCharsets.UTF_8);
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "test.adversarial", 1, SchemaFingerprint.of(dataSchema),
                "adversarial:" + canonicalRoot + '#' + member,
                "adversarial:" + canonicalRoot,
                BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1);
        return new ProviderBatchSink(dataSchema, descriptor,
                contract().configurationCodec(), config);
    }

    public static final class Session implements AutoCloseable {
        private final VerifiedDeploymentSet deployments;
        private final DurableBatchSinkProviderRegistry registry;
        private final DurableBatchSinkProvider provider;
        private final DurableBatchSinkProviderContract contract;

        private Session(
                VerifiedDeploymentSet deployments,
                DurableBatchSinkProviderRegistry registry,
                DurableBatchSinkProvider provider,
                DurableBatchSinkProviderContract contract) {
            this.deployments = deployments;
            this.registry = registry;
            this.provider = provider;
            this.contract = contract;
        }

        public DurableBatchSinkProvider provider() { return provider; }
        public DurableBatchSinkProviderContract contract() { return contract; }
        public DurableBatchSinkProviderRegistry registry() { return registry; }
        public DeploymentRuntime workerRuntime() {
            return DeploymentRuntime.forTesting(deployments, registry, true);
        }
        @Override public void close() { deployments.close(); }
    }

    private static Artifact artifact() throws Exception {
        Artifact result = cached;
        if (result != null) return result;
        synchronized (SignedAdversarialProviderFixture.class) {
            result = cached;
            if (result != null) return result;
            Path build = Files.createTempDirectory(
                    "rowweave-signed-provider-build-");
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
        Path sources = directory.resolve("src");
        Path classes = directory.resolve("classes");
        Path source = sources.resolve(
                PROVIDER_CLASS.replace('.', '/') + ".java");
        Files.createDirectories(source.getParent());
        Files.createDirectories(classes);
        Files.write(source, providerSource().getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString()) != 0) {
            throw new AssertionError("signed adversarial provider compilation failed");
        }
        Map<String, byte[]> entries = new LinkedHashMap<>();
        try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                try {
                    entries.put(classes.relativize(file).toString()
                                    .replace('\\', '/'), Files.readAllBytes(file));
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
        entries.put(DurableBatchSinkProviderRegistry.SERVICE_RESOURCE,
                (PROVIDER_CLASS + '=' + CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        entries.put(CONTRACT_RESOURCE,
                DurableBatchSinkProviderContractCodec.encode(contract()));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream out = new JarOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry jarEntry = new JarEntry(entry.getKey());
                jarEntry.setTime(0L);
                out.putNextEntry(jarEntry);
                out.write(entry.getValue());
                out.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static String providerSource() {
        return "package rowweave.conformance;"
                + "public final class SignedAdversarialProvider implements io.rowweave.DurableBatchSinkProvider {"
                + "public SignedAdversarialProvider(){}"
                + "public io.rowweave.DurableBatchSinkProviderContract contract(){return io.rowweave.deployment.SignedAdversarialProviderFixture.contract();}"
                + "private static String[] config(io.rowweave.ProviderBatchSink o){String s=new String(o.configurationBytes(),java.nio.charset.StandardCharsets.UTF_8);int n=s.indexOf('\\n');if(n<=0||n==s.length()-1)throw new IllegalArgumentException(\"PROVIDER_CONFIG_NONCANONICAL\");String root=java.nio.file.Paths.get(s.substring(0,n)).toAbsolutePath().normalize().toString();String member=s.substring(n+1);if(member.indexOf('/')>=0||member.indexOf('\\\\')>=0||member.isEmpty())throw new IllegalArgumentException(\"PROVIDER_CONFIG_NONCANONICAL\");return new String[]{root,member};}"
                + "public byte[] canonicalizeConfiguration(io.rowweave.ProviderBatchSink o){String[] c=config(o);return (c[0]+\"\\n\"+c[1]).getBytes(java.nio.charset.StandardCharsets.UTF_8);}"
                + "public byte[] canonicalizeLocator(io.rowweave.run.PreparedSinkLocator l){String m=new String(l.payload(),java.nio.charset.StandardCharsets.UTF_8);if(m.isEmpty()||m.indexOf('/')>=0||m.indexOf('\\\\')>=0)throw new IllegalArgumentException(\"PROVIDER_LOCATOR_INVALID\");return m.getBytes(java.nio.charset.StandardCharsets.UTF_8);}"
                + "private static byte[] hash(byte[] b){try{return java.security.MessageDigest.getInstance(\"SHA-256\").digest(b);}catch(Exception e){throw new RuntimeException(e);}}"
                + "private static void block(String member,String action){if(member.equals(\"block-\"+action)){for(;;){try{Thread.sleep(1000L);}catch(InterruptedException ignored){}}}}"
                + "public io.rowweave.DurableBatchSinkWriteAttempt openWriteAttempt(final io.rowweave.ProviderBatchSink o,final io.rowweave.run.SinkAttemptIdentity i,final io.rowweave.DurableSinkActionContext c){final String[] x=config(o);block(x[1],\"open\");return new io.rowweave.DurableBatchSinkWriteAttempt(){java.io.ByteArrayOutputStream rows=new java.io.ByteArrayOutputStream();boolean prepared;public void write(io.rowweave.DataRow r){block(x[1],\"write\");try{rows.write(r.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));rows.write('\\n');}catch(Exception e){throw new RuntimeException(e);}}public io.rowweave.run.PreparedSinkLocator prepare(){block(x[1],\"prepare\");try{byte[] data=rows.toByteArray();java.nio.file.Path root=java.nio.file.Paths.get(x[0]);java.nio.file.Path dir=root.resolve(\"data\").resolve(Long.toString(c.publicationGeneration()));java.nio.file.Files.createDirectories(dir);java.nio.file.Path target=dir.resolve(x[1]+\".data\");if(java.nio.file.Files.exists(target)){if(!java.util.Arrays.equals(java.nio.file.Files.readAllBytes(target),data))throw new IllegalStateException(\"provider member conflict\");}else java.nio.file.Files.write(target,data,java.nio.file.StandardOpenOption.CREATE_NEW);byte[] payload=x[1].getBytes(java.nio.charset.StandardCharsets.UTF_8);prepared=true;return new io.rowweave.run.PreparedSinkLocator(2,i,i.capabilityProfile(),i.commitDomainId(),contract().providerKey(),contract().locatorCodec(),payload,hash(payload),data.length,hash(data),System.currentTimeMillis());}catch(java.io.IOException e){throw new java.io.UncheckedIOException(e);}}public void close(){rows=null;}};}"
                + "public io.rowweave.DurableBatchSinkPreparedGroup openPreparedGroup(final java.util.List outputs,final java.util.List locators,final io.rowweave.DurableSinkActionContext c){if(outputs.size()!=locators.size()||outputs.isEmpty())throw new IllegalArgumentException(\"PROVIDER_GROUP_INVALID\");final String[] first=config((io.rowweave.ProviderBatchSink)outputs.get(0));final java.util.List names=new java.util.ArrayList();for(int n=0;n<outputs.size();n++){String[] x=config((io.rowweave.ProviderBatchSink)outputs.get(n));io.rowweave.run.PreparedSinkLocator l=(io.rowweave.run.PreparedSinkLocator)locators.get(n);String m=new String(l.payload(),java.nio.charset.StandardCharsets.UTF_8);if(!first[0].equals(x[0])||!x[1].equals(m)||!names.add(m))throw new IllegalArgumentException(\"PROVIDER_GROUP_INVALID\");}return new io.rowweave.DurableBatchSinkPreparedGroup(){java.nio.file.Path marker=java.nio.file.Paths.get(first[0]).resolve(\"publications\").resolve(c.publicationGeneration()+\".visible\");byte[] evidence=names.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);public void publish(){block(first[1],\"publish\");try{java.nio.file.Files.createDirectories(marker.getParent());if(java.nio.file.Files.exists(marker)){if(!java.util.Arrays.equals(java.nio.file.Files.readAllBytes(marker),evidence))throw new IllegalStateException(\"publication conflict\");}else java.nio.file.Files.write(marker,evidence,java.nio.file.StandardOpenOption.CREATE_NEW);}catch(java.io.IOException e){throw new java.io.UncheckedIOException(e);}}public void abort(){block(first[1],\"abort\");if(java.nio.file.Files.exists(marker))throw new IllegalStateException(\"publication already visible\");try{java.nio.file.Path dir=java.nio.file.Paths.get(first[0]).resolve(\"data\").resolve(Long.toString(c.publicationGeneration()));for(Object name:names)java.nio.file.Files.deleteIfExists(dir.resolve(name+\".data\"));}catch(java.io.IOException e){throw new java.io.UncheckedIOException(e);}}public void finalizeAttempt(){block(first[1],\"finalize\");}public io.rowweave.commit.BatchSinkOutcome queryOutcome(){block(first[1],\"query\");if(!java.nio.file.Files.exists(marker))return io.rowweave.commit.BatchSinkOutcome.of(io.rowweave.commit.BatchSinkOutcome.State.PREPARING,c.publicationGeneration(),null,null);try{byte[] seen=java.nio.file.Files.readAllBytes(marker);if(java.util.Arrays.equals(seen,\"CORRUPT\".getBytes(java.nio.charset.StandardCharsets.UTF_8)))return io.rowweave.commit.BatchSinkOutcome.of(io.rowweave.commit.BatchSinkOutcome.State.CORRUPT_OR_CONFLICT,c.publicationGeneration(),null,\"PROVIDER_CONFLICT\");return io.rowweave.commit.BatchSinkOutcome.of(io.rowweave.commit.BatchSinkOutcome.State.COMMITTED,c.publicationGeneration(),hash(seen),null);}catch(java.io.IOException e){throw new java.io.UncheckedIOException(e);}}public void close(){}};}"
                + "}";
    }

    private static void deleteTree(Path root) throws Exception {
        if (root == null || !Files.exists(root)) return;
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
