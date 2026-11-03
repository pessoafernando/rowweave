package io.rowweave.deployment;

import io.rowweave.Source;
import io.rowweave.FilterFunction;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotSourceProvider;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.plan.BatchSourcePlanValidator;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.TrustedObjectCodec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** immutable lazy loader and resource leases. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeploymentClassLoaderTest {
    private KeyPair signer;

    @BeforeAll
    void createSigner() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(3072);
        signer = generator.generateKeyPair();
    }

    @Test
    void signedStatelessObjectProviderSurvivesTrustedPlanMaterialization(
            @TempDir Path temp) throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("app.SignedObjectProvider",
                "package app;"
                        + "public final class SignedObjectProvider implements "
                        + "io.rowweave.ObjectStorageSourceProvider {"
                        + " public SignedObjectProvider(){}"
                        + " public String providerType(){return \"signed-object\";}"
                        + " public int providerVersion(){return 7;}"
                        + " public io.rowweave.ObjectStorageReadHandle open("
                        + "io.rowweave.ObjectStorageReadRequest request){"
                        + "  throw new AssertionError(\"admission must not open provider\");}"
                        + "}");
        byte[] jar = compiledJar(temp.resolve("compile-object"), sources,
                Collections.<String, byte[]>emptyMap());
        Loaded loaded = load(temp.resolve("load-object"), "object-provider",
                jar, jar.length + 200_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader loader = set.classLoader("object-provider")) {
            ObjectStorageSourceProvider provider = (ObjectStorageSourceProvider)
                    loader.loadClass("app.SignedObjectProvider")
                            .getConstructor().newInstance();
            DataSchema meta = new DataSchema();
            ObjectStorageSnapshotSource input = new ObjectStorageSnapshotSource(
                    meta, provider, "endpoint-prod", "credential-prod", "domain",
                    "ledger.tsv", "version-1", 0L,
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES);
            PlanGraph graph = new PlanGraph(Collections.singletonList(
                    PlanNode.sourceFromInputs(meta,
                            Collections.<Source>singletonList(input))));
            ObjectTrustContext trust = ObjectTrustContext.mtlsDeployments(set);

            byte[] encoded = graph.toBytes(trust, ignored -> { });
            PlanGraph decoded = PlanGraph.fromBytes(
                    encoded, trust, ignored -> { });
            BatchSourcePlanValidator.validateDurable(decoded, trust);

            ObjectStorageSnapshotSource decodedInput =
                    (ObjectStorageSnapshotSource) decoded.node(0).inputs().get(0);
            assertEquals("rowweave.object-storage/signed-object",
                    decodedInput.batchSourceSplit().connectorType());
            assertEquals(loader,
                    decodedInput.sourceProvider().getClass().getClassLoader());
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void signedStatelessJdbcProviderSurvivesTrustedPlanMaterialization(
            @TempDir Path temp) throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("app.SignedJdbcProvider",
                "package app;"
                        + "public final class SignedJdbcProvider implements "
                        + "io.rowweave.JdbcSnapshotSourceProvider {"
                        + " public SignedJdbcProvider(){}"
                        + " public String providerType(){return \"signed-jdbc\";}"
                        + " public int providerVersion(){return 4;}"
                        + " public io.rowweave.JdbcSnapshotReadHandle open("
                        + "io.rowweave.JdbcSnapshotReadRequest request){"
                        + "  throw new AssertionError(\"admission must not open provider\");}"
                        + "}");
        byte[] jar = compiledJar(temp.resolve("compile-jdbc"), sources,
                Collections.<String, byte[]>emptyMap());
        Loaded loaded = load(temp.resolve("load-jdbc"), "jdbc-provider",
                jar, jar.length + 200_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader loader = set.classLoader("jdbc-provider")) {
            JdbcSnapshotSourceProvider provider = (JdbcSnapshotSourceProvider)
                    loader.loadClass("app.SignedJdbcProvider")
                            .getConstructor().newInstance();
            DataSchema meta = new DataSchema();
            String query = String.join("", Collections.nCopies(64, "a"));
            String partition = String.join("", Collections.nCopies(64, "b"));
            String snapshot = String.join("", Collections.nCopies(64, "c"));
            JdbcSnapshotSource input = new JdbcSnapshotSource(
                    meta, provider, "endpoint", "credential", "ledger",
                    "ledger-query", "partition-0", "snapshot",
                    query, partition, snapshot, 100, 30, 4096);
            PlanGraph graph = new PlanGraph(Collections.singletonList(
                    PlanNode.sourceFromInputs(meta,
                            Collections.<Source>singletonList(input))));
            ObjectTrustContext trust = ObjectTrustContext.mtlsDeployments(set);

            byte[] encoded = graph.toBytes(trust, ignored -> { });
            PlanGraph decoded = PlanGraph.fromBytes(
                    encoded, trust, ignored -> { });
            BatchSourcePlanValidator.validateDurable(decoded, trust);

            JdbcSnapshotSource decodedInput =
                    (JdbcSnapshotSource) decoded.node(0).inputs().get(0);
            assertEquals("rowweave.jdbc-snapshot/signed-jdbc",
                    decodedInput.batchSourceSplit().connectorType());
            assertEquals(loader,
                    decodedInput.sourceProvider().getClass().getClassLoader());
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void applicationClassesAreChildFirstButRowWeaveApiIsParentFirst(@TempDir Path temp)
            throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("app.SignedInput",
                "package app;"
                        + "public final class SignedInput implements io.rowweave.Source {"
                        + " public io.rowweave.DataSchema getSchema(){return new io.rowweave.DataSchema();}"
                        + " public java.util.Iterator<io.rowweave.DataRow> iterator(){"
                        + "  return java.util.Collections.<io.rowweave.DataRow>emptyList().iterator();}"
                        + " public String marker(){return \"signed\";}"
                        + "}");
        sources.put("shadow.fixture.Shadowed",
                "package shadow.fixture; public final class Shadowed {"
                        + " public String origin(){return \"deployment\";} }");
        byte[] jar = compiledJar(temp.resolve("compile"), sources,
                Collections.singletonMap("app/config.txt", "hello".getBytes(StandardCharsets.UTF_8)));
        Loaded loaded = load(temp.resolve("load"), "signed", jar, jar.length + 200_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader loader = set.classLoader("signed")) {
            long baseline = loaded.budget.usedBytes();
            Class<?> inputClass = loader.loadClass("app.SignedInput");
            Object input = inputClass.getDeclaredConstructor().newInstance();

            assertTrue(input instanceof Source);
            assertEquals("signed", inputClass.getMethod("marker").invoke(input));
            assertEquals(Source.class, inputClass.getInterfaces()[0]);
            assertEquals(baseline, loaded.budget.usedBytes(),
                    "class expanded bytes must be released immediately after defineClass");

            Class<?> shadow = loader.loadClass("shadow.fixture.Shadowed");
            assertNotSame(shadow.fixture.Shadowed.class, shadow);
            assertEquals("deployment", shadow.getMethod("origin")
                    .invoke(shadow.getDeclaredConstructor().newInstance()));
            assertEquals(loader, shadow.getClassLoader());
            try (InputStream resource = loader.getResource("app/config.txt").openStream()) {
                byte[] text = new byte[5];
                assertEquals(5, resource.read(text));
                assertEquals("hello", new String(text, StandardCharsets.UTF_8));
            }
            assertEquals(baseline, loaded.budget.usedBytes());
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void resourceLeaseDefersImageReleaseAndCloseRejectsEveryNewLoad(@TempDir Path temp)
            throws Exception {
        byte[] resource = new byte[4096];
        for (int i = 0; i < resource.length; i++) resource[i] = (byte) i;
        byte[] jar = compiledJar(temp.resolve("compile"),
                Collections.singletonMap("app.Plugin",
                        "package app; public final class Plugin {}"),
                Collections.singletonMap("data.bin", resource));
        Loaded loaded = load(temp.resolve("load"), "resources", jar, jar.length + 100_000L);
        VerifiedDeploymentSet set = loaded.set;
        DeploymentClassLoader loader = set.classLoader("resources");
        long baseline = loaded.budget.usedBytes();
        InputStream stream = loader.getResourceAsStream("data.bin");
        assertTrue(stream != null);
        assertEquals(baseline + resource.length, loaded.budget.usedBytes());

        loader.close();
        assertEquals(baseline + resource.length, loaded.budget.usedBytes(),
                "compressed/index charge must remain until the last borrowed stream closes");
        assertThrows(ClassNotFoundException.class, () -> loader.loadClass("app.Plugin"));
        assertNull(loader.getResourceAsStream("data.bin"));
        assertEquals(resource[0] & 0xff, stream.read(),
                "already borrowed stream remains readable while closing drains");

        stream.close();
        stream.close();
        assertTrue(loaded.budget.usedBytes() < baseline,
                "loader close releases its compressed/archive index; set identity remains owned by set");
        set.close();
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void leakedResourceCannotTurnIntoUnbudgetedCopies(@TempDir Path temp) throws Exception {
        byte[] resource = new byte[8192];
        new java.util.Random(17L).nextBytes(resource);
        byte[] jar = compiledJar(temp.resolve("compile"), Collections.<String, String>emptyMap(),
                Collections.singletonMap("large.bin", resource));
        Loaded loaded = load(temp.resolve("load"), "bounded", jar, jar.length + 12_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader loader = set.classLoader("bounded")) {
            InputStream first = loader.getResourceAsStream("large.bin");
            assertTrue(first != null);
            DeploymentValidationException exhausted = assertThrows(
                    DeploymentValidationException.class,
                    () -> loader.getResourceAsStream("large.bin"));
            assertEquals("DEPLOYMENT_MEMORY_BUDGET_EXCEEDED", exhausted.code());
            first.close();
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void lazyLoadRechecksIndexedDigestAgainstPrivateImage(@TempDir Path temp) throws Exception {
        byte[] jar = compiledJar(temp.resolve("compile"),
                Collections.singletonMap("app.Plugin",
                        "package app; public final class Plugin {}"),
                Collections.<String, byte[]>emptyMap());
        Loaded loaded = load(temp.resolve("load"), "digest", jar, jar.length + 100_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader loader = set.classLoader("digest")) {
            corruptEntryBytes(set.image("digest"), "app/Plugin.class");
            DeploymentValidationException failure = assertThrows(
                    DeploymentValidationException.class,
                    () -> loader.loadClass("app.Plugin"));
            assertEquals("DEPLOYMENT_ENTRY_INTEGRITY_FAILED", failure.code());
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void deploymentsCannotResolveEachOthersPrivateClasses(@TempDir Path temp) throws Exception {
        Map<String, String> all = new LinkedHashMap<>();
        all.put("a.UsesHelper", "package a; public final class UsesHelper {"
                + " public Object create(){return new b.Helper();} }");
        all.put("b.Helper", "package b; public final class Helper {}");
        Map<String, byte[]> compiled = compile(temp.resolve("compile"), all);
        byte[] leftJar = jar(Collections.singletonMap(
                "a/UsesHelper.class", compiled.get("a/UsesHelper.class")));
        byte[] rightJar = jar(Collections.singletonMap(
                "b/Helper.class", compiled.get("b/Helper.class")));
        Loaded loaded = load(temp.resolve("load"),
                manifests(temp.resolve("load"), leftJar, rightJar), 1_000_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader left = set.classLoader("left");
             DeploymentClassLoader right = set.classLoader("right")) {
            assertEquals("b.Helper", right.loadClass("b.Helper").getName());
            Object value = left.loadClass("a.UsesHelper").getDeclaredConstructor().newInstance();
            java.lang.reflect.InvocationTargetException failure = assertThrows(
                    java.lang.reflect.InvocationTargetException.class,
                    () -> value.getClass().getMethod("create").invoke(value));
            assertTrue(failure.getCause() instanceof NoClassDefFoundError,
                    String.valueOf(failure.getCause()));
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void mtlsObjectTrustRoundTripsSignedInputAndRejectsSameNameClasspathWriter(@TempDir Path temp)
            throws Exception {
        String source = "package shadow.fixture;"
                + "public final class UnsignedInput implements io.rowweave.Source {"
                + " private static final long serialVersionUID=1L;"
                + " public io.rowweave.DataSchema getSchema(){return new io.rowweave.DataSchema();}"
                + " public java.util.Iterator<io.rowweave.DataRow> iterator(){"
                + "  return java.util.Collections.<io.rowweave.DataRow>emptyList().iterator();}"
                + " public String marker(){return \"deployment\";}"
                + "}";
        byte[] jar = compiledJar(temp.resolve("compile"),
                Collections.singletonMap("shadow.fixture.UnsignedInput", source),
                Collections.<String, byte[]>emptyMap());
        Loaded loaded = load(temp.resolve("load"), "objects", jar, jar.length + 200_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader loader = set.classLoader("objects")) {
            ObjectTrustContext trust = ObjectTrustContext.mtlsDeployments(set);
            Source signed = (Source) loader.loadClass("shadow.fixture.UnsignedInput")
                    .getDeclaredConstructor().newInstance();

            byte[] encoded = TrustedObjectCodec.write(
                    signed, Source.class, trust, ignored -> { });
            Source decoded = TrustedObjectCodec.readSlice(
                    encoded, 0, encoded.length, Source.class, trust, ignored -> { });

            assertEquals(loader, decoded.getClass().getClassLoader());
            assertEquals("deployment", decoded.getClass().getMethod("marker").invoke(decoded));
            RuntimeException rejected = assertThrows(RuntimeException.class,
                    () -> TrustedObjectCodec.write(new shadow.fixture.UnsignedInput(),
                            Source.class, trust, ignored -> { }));
            assertTrue(hasMessage(rejected, "trusted deployment"), rejected.toString());
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    @Test
    void serializableLambdaCapturingClassMustBelongToSignedIndex(@TempDir Path temp)
            throws Exception {
        String source = "package app; public final class FunctionFactory {"
                + " public static io.rowweave.FilterFunction create(){return row -> true;}"
                + "}";
        byte[] jar = compiledJar(temp.resolve("compile"),
                Collections.singletonMap("app.FunctionFactory", source),
                Collections.<String, byte[]>emptyMap());
        Loaded loaded = load(temp.resolve("load"), "lambda", jar, jar.length + 200_000L);
        try (VerifiedDeploymentSet set = loaded.set;
             DeploymentClassLoader loader = set.classLoader("lambda")) {
            ObjectTrustContext trust = ObjectTrustContext.mtlsDeployments(set);
            FilterFunction function = (FilterFunction) loader.loadClass("app.FunctionFactory")
                    .getMethod("create").invoke(null);
            byte[] encoded = TrustedObjectCodec.write(
                    function, FilterFunction.class, trust, ignored -> { });
            FilterFunction decoded = TrustedObjectCodec.readSlice(
                    encoded, 0, encoded.length, FilterFunction.class, trust, ignored -> { });
            assertTrue(decoded.filter(null));
            assertEquals(loader, decoded.getClass().getClassLoader());
        }
        assertEquals(0L, loaded.budget.usedBytes());
    }

    private Loaded load(Path directory, String id, byte[] jar, long capacity) throws Exception {
        Files.createDirectories(directory);
        Path source = directory.resolve(id + ".jar");
        Files.write(source, jar);
        return load(directory, Collections.singletonList(manifest(id, source, jar)), capacity);
    }

    private Loaded load(Path directory, java.util.List<DeploymentManifest> manifests, long capacity) {
        DeploymentMemoryBudget budget = DeploymentMemoryBudget.forTesting(capacity);
        return new Loaded(VerifiedDeploymentSet.load(manifests,
                Collections.singletonMap("key", signer.getPublic()), budget), budget);
    }

    private java.util.List<DeploymentManifest> manifests(
            Path directory, byte[] left, byte[] right) throws Exception {
        Files.createDirectories(directory);
        Path leftPath = directory.resolve("left.jar");
        Path rightPath = directory.resolve("right.jar");
        Files.write(leftPath, left);
        Files.write(rightPath, right);
        return java.util.Arrays.asList(
                manifest("left", leftPath, left), manifest("right", rightPath, right));
    }

    private DeploymentManifest manifest(String id, Path source, byte[] jar) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(jar);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signer.getPrivate());
        signature.update(DeploymentManifest.signingPayload(id, jar.length, digest));
        return new DeploymentManifest(id, source.toAbsolutePath(), jar.length, digest,
                "key", signature.sign());
    }

    private static byte[] compiledJar(Path directory, Map<String, String> sources,
                                      Map<String, byte[]> resources) throws Exception {
        Map<String, byte[]> entries = compile(directory, sources);
        entries.putAll(resources);
        return jar(entries);
    }

    private static Map<String, byte[]> compile(Path directory, Map<String, String> sources)
            throws Exception {
        Path sourceRoot = directory.resolve("src");
        Path classes = directory.resolve("classes");
        Files.createDirectories(sourceRoot);
        Files.createDirectories(classes);
        java.util.List<String> arguments = new java.util.ArrayList<>();
        arguments.add("-source"); arguments.add("8");
        arguments.add("-target"); arguments.add("8");
        arguments.add("-classpath"); arguments.add(System.getProperty("java.class.path"));
        arguments.add("-d"); arguments.add(classes.toString());
        for (Map.Entry<String, String> source : sources.entrySet()) {
            Path file = sourceRoot.resolve(source.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.write(file, source.getValue().getBytes(StandardCharsets.UTF_8));
            arguments.add(file.toString());
        }
        if (!sources.isEmpty()) {
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            assertTrue(compiler != null, "tests require a JDK compiler");
            assertEquals(0, compiler.run(null, null, null, arguments.toArray(new String[0])));
        }
        Map<String, byte[]> compiled = new LinkedHashMap<>();
        if (Files.exists(classes)) {
            try (java.util.stream.Stream<Path> files = Files.walk(classes)) {
                files.filter(Files::isRegularFile).forEach(file -> {
                    try {
                        compiled.put(classes.relativize(file).toString().replace('\\', '/'),
                                Files.readAllBytes(file));
                    } catch (java.io.IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                });
            }
        }
        return compiled;
    }

    private static byte[] jar(Map<String, byte[]> entries) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes)) {
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                JarEntry item = new JarEntry(entry.getKey());
                item.setTime(0L);
                jar.putNextEntry(item);
                jar.write(entry.getValue());
                jar.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static void corruptEntryBytes(VerifiedDeploymentImage image, String name) throws Exception {
        java.lang.reflect.Field compressed = VerifiedDeploymentImage.class.getDeclaredField("compressedBytes");
        compressed.setAccessible(true);
        byte[] bytes = (byte[]) compressed.get(image);
        java.lang.reflect.Field indexField = VerifiedDeploymentImage.class.getDeclaredField("index");
        indexField.setAccessible(true);
        DeploymentArchiveScanner.Index index = (DeploymentArchiveScanner.Index) indexField.get(image);
        DeploymentArchiveScanner.Entry entry = index.entries.get(name);
        bytes[entry.dataOffset + Math.min(3, entry.compressedSize - 1)] ^= 1;
    }

    private static boolean hasMessage(Throwable failure, String text) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(text)) return true;
            if (current.getCause() == current) break;
        }
        return false;
    }

    private static final class Loaded {
        final VerifiedDeploymentSet set;
        final DeploymentMemoryBudget budget;
        Loaded(VerifiedDeploymentSet set, DeploymentMemoryBudget budget) {
            this.set = set;
            this.budget = budget;
        }
    }
}
