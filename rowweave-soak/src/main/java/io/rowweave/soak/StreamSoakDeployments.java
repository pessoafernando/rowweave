package io.rowweave.soak;

import io.rowweave.deployment.DeploymentManifest;
import io.rowweave.deployment.DeploymentManifestCodec;
import io.rowweave.deployment.DeploymentMemoryBudget;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.contract.StreamConnectorProviderContract;
import io.rowweave.stream.contract.StreamConnectorProviderContractCodec;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

final class StreamSoakDeployments implements AutoCloseable {
    static final String SIGNER_ID = "stream-soak-signer";
    static final String SINK_DEPLOYMENT = "rowweave-stream-soak-sink";
    static final String SINK_PROVIDER = "rowweave.stream.soak.transactional-sink";
    static final String SINK_CLASS = "stream.soak.SoakSink";
    static final String CONTRACT_RESOURCE =
            "META-INF/rowweave/stream-connector/contracts/stream-soak-sink.bin";
    static final StreamConnectorProviderContract SINK_CONTRACT =
            StreamConnectorProviderContract.sink(
                    SINK_DEPLOYMENT, SINK_PROVIDER,
                    StreamConnectorConfiguration.CODEC_ID,
                    16 * 1024, 64, 60_000L,
                    StreamConnectorProviderContract.SinkVisibility
                            .CHECKPOINT_TRANSACTIONAL);

    final Path trustRoot;
    final Path signerPath;
    final List<Path> manifests;
    final String deploymentSetId;
    private final VerifiedDeploymentSet verified;

    private StreamSoakDeployments(Path trustRoot, Path signerPath,
            List<Path> manifests, VerifiedDeploymentSet verified) {
        this.trustRoot = trustRoot;
        this.signerPath = signerPath;
        this.manifests = Collections.unmodifiableList(
                new ArrayList<>(manifests));
        this.verified = verified;
        this.deploymentSetId = verified.deploymentSetId();
    }

    static StreamSoakDeployments create(Path root, Path s3Image,
            String s3DeploymentId, Path postgresImage,
            String postgresDeploymentId) throws Exception {
        requireFile(s3Image);
        requireFile(postgresImage);
        Path sinkImage = createSinkImage(root.resolve("sink-image"));
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        KeyPair signer = generator.generateKeyPair();
        List<DeploymentManifest> entries = Arrays.asList(
                sign(s3Image, s3DeploymentId, signer),
                sign(postgresImage, postgresDeploymentId, signer),
                sign(sinkImage, SINK_DEPLOYMENT, signer));
        Path trustRoot = Files.createDirectories(
                root.resolve("deployment-trust")).toAbsolutePath().normalize();
        Path signerPath = trustRoot.resolve(SIGNER_ID + ".der");
        Files.write(signerPath, signer.getPublic().getEncoded());
        List<Path> manifests = new ArrayList<>();
        for (int index = 0; index < entries.size(); index++) {
            Path manifest = trustRoot.resolve("deployment-" + index
                    + ".rwdep");
            Files.write(manifest,
                    DeploymentManifestCodec.encode(entries.get(index)));
            manifests.add(manifest);
        }
        Map<String, java.security.PublicKey> keys =
                Collections.singletonMap(SIGNER_ID, signer.getPublic());
        VerifiedDeploymentSet verified = VerifiedDeploymentSet.load(
                entries, keys, DeploymentMemoryBudget.withCapacity(
                        256L * 1024L * 1024L));
        readOnly(signerPath, false);
        for (Path manifest : manifests) readOnly(manifest, false);
        readOnly(trustRoot, true);
        return new StreamSoakDeployments(
                trustRoot, signerPath, manifests, verified);
    }

    void addProperties(List<String> command) {
        command.add("-D" + DeploymentRuntime.DEPLOYMENT_COUNT_PROPERTY
                + '=' + manifests.size());
        command.add("-D" + DeploymentRuntime.SIGNER_COUNT_PROPERTY + "=1");
        command.add("-D" + DeploymentRuntime.SIGNER_ID_PROPERTY_PREFIX
                + "0=" + SIGNER_ID);
        command.add("-D" + DeploymentRuntime.SIGNER_PATH_PROPERTY_PREFIX
                + "0=" + signerPath);
        command.add("-D" + DeploymentRuntime.TRUST_ROOT_PROPERTY
                + '=' + trustRoot);
        command.add("-Drowweave.deployment.memoryBytes=134217728");
        for (int index = 0; index < manifests.size(); index++) {
            command.add("-D" + DeploymentRuntime.MANIFEST_PROPERTY_PREFIX
                    + index + '=' + manifests.get(index));
        }
    }

    @Override public void close() { verified.close(); }

    private static DeploymentManifest sign(Path image, String deploymentId,
            KeyPair signer) throws Exception {
        byte[] bytes = Files.readAllBytes(image);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(
                deploymentId, bytes.length, digest));
        return new DeploymentManifest(deploymentId,
                image.toAbsolutePath().normalize(), bytes.length, digest,
                SIGNER_ID, signature.sign());
    }

    private static Path createSinkImage(Path root) throws Exception {
        Path source = root.resolve("src/stream/soak/SoakSink.java");
        Path classes = Files.createDirectories(root.resolve("classes"));
        Files.createDirectories(source.getParent());
        Files.write(source, sinkSource().getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null || compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString()) != 0) {
            throw new IllegalStateException("STREAM SOAK sink compilation failed");
        }
        Map<String, byte[]> files = new LinkedHashMap<>();
        for (String name : Arrays.asList(SINK_CLASS, SINK_CLASS + "$1")) {
            String resource = name.replace('.', '/') + ".class";
            files.put(resource, Files.readAllBytes(classes.resolve(resource)));
        }
        files.put(StreamConnectorProviderRegistry
                        .TRANSACTIONAL_SINK_SERVICE_RESOURCE,
                (SINK_CLASS + '=' + CONTRACT_RESOURCE + '\n')
                        .getBytes(StandardCharsets.UTF_8));
        files.put(CONTRACT_RESOURCE,
                StreamConnectorProviderContractCodec.encode(SINK_CONTRACT));
        Path image = root.resolve("stream-soak-sink.jar");
        try (OutputStream file = Files.newOutputStream(image);
             JarOutputStream jar = new JarOutputStream(file)) {
            for (Map.Entry<String, byte[]> entry : files.entrySet()) {
                JarEntry jarEntry = new JarEntry(entry.getKey());
                jarEntry.setTime(0L);
                jar.putNextEntry(jarEntry);
                jar.write(entry.getValue());
                jar.closeEntry();
            }
        }
        return image.toAbsolutePath().normalize();
    }

    private static String sinkSource() {
        String digest = StreamConnectorProviderContractCodec.digestHex(
                SINK_CONTRACT);
        return "package stream.soak;"
                + "public final class SoakSink implements io.rowweave.stream.spi.StreamTransactionalSinkProvider{"
                + "public String providerKey(){return \"" + SINK_PROVIDER + "\";}"
                + "public String contractDigest(){return \"" + digest + "\";}"
                + "public io.rowweave.stream.spi.StreamTransactionalSinkWriter open(io.rowweave.stream.spi.StreamTransactionalSinkContext x)throws Exception{"
                + "io.rowweave.stream.api.StreamConnectorConfiguration c=io.rowweave.stream.api.StreamConnectorConfiguration.decode(x.configurationFrame());"
                + "if(c.options().size()!=3||!c.options().containsKey(\"output.path\")||!c.options().containsKey(\"mode\")||!c.options().containsKey(\"delay.millis\")||c.secretReferences().size()!=1)throw new IllegalArgumentException(\"STREAM_SOAK_SINK_CONFIG\");"
                + "x.secrets().withSecret(\"marker\",b->{if(b.length!=1)throw new IllegalArgumentException(\"STREAM_SOAK_SINK_SECRET\");return null;});"
                + "final java.nio.file.Path root=java.nio.file.Paths.get(c.options().get(\"output.path\"));java.nio.file.Files.createDirectories(root);"
                + "final String mode=c.options().get(\"mode\");final long delay=Long.parseLong(c.options().get(\"delay.millis\"));"
                + "return new io.rowweave.stream.spi.StreamTransactionalSinkWriter(){"
                + "private final java.io.ByteArrayOutputStream pending=new java.io.ByteArrayOutputStream();"
                + "public void write(byte[] frame)throws Exception{if(delay>0)java.lang.Thread.sleep(delay);String line;"
                + "if(\"s3\".equals(mode)){io.rowweave.stream.api.StreamSchema s=new io.rowweave.stream.api.StreamSchema(java.util.Collections.singletonList(new io.rowweave.stream.api.StreamField(\"id\",\"id\",\"id\",io.rowweave.DataType.LongType,false)));long id=new io.rowweave.stream.api.StreamDataRowCodec(s).decode(frame).getLong(0);line=\"S|\"+id;}"
                + "else if(\"postgres\".equals(mode)){io.rowweave.stream.contract.StreamUpsertRecord r=io.rowweave.stream.contract.StreamUpsertRecordCodec.decode(frame,4194304,4194304);io.rowweave.stream.api.StreamSchema k=new io.rowweave.stream.api.StreamSchema(java.util.Collections.singletonList(new io.rowweave.stream.api.StreamField(\"id\",\"id\",\"id\",io.rowweave.DataType.LongType,false)));long id=new io.rowweave.stream.api.StreamDataRowCodec(k).decode(r.stableKeyFrame()).getLong(0);if(r.hasAfterImage()){java.util.List<io.rowweave.stream.api.StreamField> fs=java.util.Arrays.asList(new io.rowweave.stream.api.StreamField(\"id\",\"id\",\"id\",io.rowweave.DataType.LongType,false),new io.rowweave.stream.api.StreamField(\"amount\",\"amount\",\"amount\",io.rowweave.DataType.BigDecimalType,true));io.rowweave.DataRow row=new io.rowweave.stream.api.StreamDataRowCodec(new io.rowweave.stream.api.StreamSchema(fs)).decode(r.afterImageFrame());line=\"A|\"+id+\"|\"+row.getBigDecimal(1).toPlainString();}else line=\"D|\"+id;}"
                + "else throw new IllegalArgumentException(\"STREAM_SOAK_SINK_MODE\");pending.write((line+\"\\n\").getBytes(java.nio.charset.StandardCharsets.UTF_8));}"
                + "public byte[] prepare(String id)throws Exception{java.nio.file.Path p=file(\"prepared-\",id);if(!java.nio.file.Files.exists(p))java.nio.file.Files.write(p,pending.toByteArray(),java.nio.file.StandardOpenOption.CREATE_NEW);pending.reset();return hash(id).getBytes(java.nio.charset.StandardCharsets.US_ASCII);}"
                + "public io.rowweave.stream.spi.StreamTransactionStatus query(String id){if(java.nio.file.Files.exists(file(\"visible-\",id)))return io.rowweave.stream.spi.StreamTransactionStatus.VISIBLE;if(java.nio.file.Files.exists(file(\"prepared-\",id)))return io.rowweave.stream.spi.StreamTransactionStatus.PREPARED;return io.rowweave.stream.spi.StreamTransactionStatus.UNKNOWN;}"
                + "public void commit(String id)throws Exception{java.nio.file.Path v=file(\"visible-\",id);if(java.nio.file.Files.exists(v))return;java.nio.file.Path p=file(\"prepared-\",id);if(!java.nio.file.Files.exists(p))throw new IllegalStateException(\"STREAM_SOAK_TRANSACTION_MISSING\");java.nio.file.Files.move(p,v,java.nio.file.StandardCopyOption.ATOMIC_MOVE);}"
                + "public void finalizeTransaction(String id){}public void abort(String id)throws Exception{if(!java.nio.file.Files.exists(file(\"visible-\",id)))java.nio.file.Files.deleteIfExists(file(\"prepared-\",id));}public void close(){}"
                + "private java.nio.file.Path file(String p,String id){return root.resolve(p+hash(id));}private String hash(String id){try{byte[] d=java.security.MessageDigest.getInstance(\"SHA-256\").digest(id.getBytes(java.nio.charset.StandardCharsets.UTF_8));StringBuilder b=new StringBuilder(64);for(byte v:d){b.append(Character.forDigit((v>>>4)&15,16));b.append(Character.forDigit(v&15,16));}return b.toString();}catch(Exception e){throw new IllegalStateException(e);}}"
                + "};}}";
    }

    private static void requireFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            throw new IllegalArgumentException("deployment JAR missing: "
                    + path);
        }
    }

    private static void readOnly(Path target, boolean directory)
            throws Exception {
        Files.setPosixFilePermissions(target, directory
                ? EnumSet.of(PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.GROUP_EXECUTE,
                PosixFilePermission.OTHERS_READ,
                PosixFilePermission.OTHERS_EXECUTE)
                : EnumSet.of(PosixFilePermission.OWNER_READ,
                PosixFilePermission.GROUP_READ,
                PosixFilePermission.OTHERS_READ));
    }
}
