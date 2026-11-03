package io.rowweave.connector.postgres;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Public connector SDK compiled to class-major 52 and run by Java 8. */
class PostgresPublishedJava8ConsumerIT {
    @Test
    void publicConnectorSdkLoadsPublishedJarsOnJava8(@TempDir Path temp)
            throws Exception {
        Java8Home resolved = java8Home();
        if (resolved.home == null) {
            String detail = resolved.reason.isEmpty()
                    ? "nothing was offered and the host probe found no JDK 8"
                    : resolved.reason;
            assertTrue(!resolved.explicitlyOffered,
                    "a Java 8 home was named but is not a usable JDK 8: " + detail);
            assertTrue(!java8GateRequired(),
                    "this run claims Java 8 evidence and no JDK 8 resolved: " + detail);
            Assumptions.assumeTrue(false,
                    "no Java 8 home offered: set -Drowweave.java8.home, "
                            + "ROWWEAVE_JAVA8_HOME or JAVA8_HOME");
        }
        Path java = resolved.home.resolve("bin").resolve("java");
        Path deployment = requiredJar("postgres.deployment.jar");
        Path core = requiredJar("postgres.core.jar");
        Path source = temp.resolve(
                "src/external/PostgresPublishedConsumer.java");
        Path classes = Files.createDirectories(temp.resolve("classes"));
        Files.createDirectories(source.getParent());
        Files.write(source, consumerSource().getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertTrue(compiler != null, "current JDK has no Java compiler");
        String classpath = deployment + File.pathSeparator + core;
        assertEquals(0, compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", classpath,
                "-d", classes.toString(), source.toString()));
        byte[] consumer = Files.readAllBytes(classes.resolve(
                "external/PostgresPublishedConsumer.class"));
        assertEquals(52, ((consumer[6] & 0xff) << 8)
                | (consumer[7] & 0xff));

        Path log = temp.resolve("java8.log");
        Process process = new ProcessBuilder(java.toString(),
                "-Xms16m", "-Xmx96m", "-cp",
                classes + File.pathSeparator + classpath,
                "external.PostgresPublishedConsumer",
                deployment.toString(), core.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile())
                .start();
        boolean exited = process.waitFor(30L, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5L, TimeUnit.SECONDS);
        }
        assertTrue(exited, read(log));
        assertEquals(0, process.exitValue(), read(log));
        assertTrue(read(log).contains("POSTGRES_JAVA8_OK"), read(log));
    }

    private static String consumerSource() {
        return "package external;\n"
                + "import java.nio.charset.StandardCharsets;\n"
                + "import java.nio.file.Path;\n"
                + "import java.nio.file.Paths;\n"
                + "import java.util.Arrays;\n"
                + "import java.util.Collections;\n"
                + "import io.rowweave.RowWeave;\n"
                + "import io.rowweave.connector.postgres.*;\n"
                + "import io.rowweave.stream.api.StreamJobEvidence;\n"
                + "import io.rowweave.stream.api.StreamJobId;\n"
                + "import io.rowweave.stream.api.StreamJobProgress;\n"
                + "import io.rowweave.stream.api.StreamSourceSpec;\n"
                + "public final class PostgresPublishedConsumer {\n"
                + " public static void main(String[] args)throws Exception {\n"
                + "  Path connector=Paths.get(args[0]).toRealPath();\n"
                + "  Path core=Paths.get(args[1]).toRealPath();\n"
                + "  Path actualConnector=Paths.get(PostgresConnectorProvider.class"
                + ".getProtectionDomain().getCodeSource().getLocation().toURI())"
                + ".toRealPath();\n"
                + "  Path actualCore=Paths.get(RowWeave.class.getProtectionDomain()"
                + ".getCodeSource().getLocation().toURI()).toRealPath();\n"
                + "  if(!connector.equals(actualConnector)||!core.equals(actualCore))"
                + " throw new IllegalStateException(\"artifact origin\");\n"
                + "  PostgresTableBinding table=new PostgresTableBinding("
                + "\"ledger\",\"entries\",16384L,PostgresReplicaIdentity.DEFAULT,"
                + "0L,Arrays.asList(new PostgresColumnBinding(\"id\",\"id\",1,20L,-1,false),"
                + "new PostgresColumnBinding(\"amount\",\"amount\",2,1700L,1179654,true)),"
                + "Collections.singletonList(1));\n"
                + "  PostgresConnectorConfiguration config="
                + "PostgresConnectorConfiguration.builder().host(\"db.invalid\")"
                + ".database(\"ledger\").slot(\"rowweave_ledger\")"
                + ".publication(\"rowweave_publication\")"
                + ".databaseSystemIdentifier(\"123456789\").databaseOid(16000L)"
                + ".tlsMode(PostgresTlsMode.REQUIRE)"
                + ".credentialsReference(\"vault/postgres\")"
                + ".tables(Collections.singletonList(table)).build();\n"
                + "  StreamSourceSpec spec=config.toSourceSpec();\n"
                + "  if(!spec.residentCdc()||spec.splits().size()!=1)"
                + " throw new IllegalStateException(\"source contract\");\n"
                + "  PostgresCredentials secret=PostgresCredentials.of("
                + "\"reader\".getBytes(StandardCharsets.UTF_8),"
                + "\"secret\".getBytes(StandardCharsets.UTF_8));\n"
                + "  byte[] frame=PostgresCredentialCodec.encode(secret);"
                + " secret.destroy();\n"
                + "  PostgresCredentials decoded=PostgresCredentialCodec.decode(frame);"
                + " decoded.destroy();\n"
                + "  PostgresConnectorProvider provider="
                + "new PostgresConnectorProvider();\n"
                + "  if(!PostgresConnectorContract.PROVIDER_KEY.equals("
                + "provider.providerKey())||!PostgresConnectorContract"
                + ".contractDigest().equals(provider.contractDigest()))"
                + " throw new IllegalStateException(\"provider contract\");\n"
                + "  if(io.rowweave.StreamJobHandle.class.getMethod("
                + "\"evidence\").getReturnType()!=StreamJobEvidence.class)"
                + " throw new IllegalStateException(\"evidence API drift\");\n"
                + "  StreamJobProgress progress=StreamJobProgress.empty();\n"
                + "  StreamJobEvidence evidence=StreamJobEvidence.unavailable("
                + "StreamJobId.of(\"published-postgres-consumer\"),progress,1L);\n"
                + "  if(evidence.available()||evidence.complete()"
                + "||evidence.attemptProgress().recordsWritten()!=0L"
                + "||evidence.lifetimeProgress().backlogEvents()!=0L)"
                + " throw new IllegalStateException(\"invalid public evidence\");\n"
                + "  System.out.println(\"POSTGRES_JAVA8_OK\");\n"
                + " }\n} \n";
    }

    private static Path requiredJar(String property) throws Exception {
        String configured = System.getProperty(property);
        if (configured == null) {
            throw new AssertionError("missing system property " + property);
        }
        Path path = Paths.get(configured).toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(path), path.toString());
        return path.toRealPath();
    }

    /**
     * The one Java 8 home contract shared with tools/java8-home.sh: an explicit
     * -Drowweave.java8.home, ROWWEAVE_JAVA8_HOME or JAVA8_HOME wins, and the
     * macOS probe is only a last resort. A candidate counts as offered whenever
     * it was named, so a named home that is not a JDK 8 fails the gate instead
     * of being traded for the host JDK. Acceptance needs both bin/java and
     * bin/javac: this gate compiles the consumer with the host toolchain and
     * only runs it on Java 8, so a runtime without javac would silently widen
     * the claim being made.
     */
    private static final class Java8Home {
        private final Path home;
        private final boolean explicitlyOffered;
        private final String reason;

        private Java8Home(Path home, boolean explicitlyOffered, String reason) {
            this.home = home;
            this.explicitlyOffered = explicitlyOffered;
            this.reason = reason;
        }
    }

    private static Java8Home java8Home() throws Exception {
        List<String> rejected = new ArrayList<>();
        boolean explicitlyOffered = false;
        for (String[] candidate : offeredJava8Homes()) {
            explicitlyOffered = true;
            Path home = validateJava8Home(candidate[1], candidate[0], rejected);
            if (home != null) return new Java8Home(home, true, "");
        }
        Path probed = macOsJava8Probe();
        if (probed != null) {
            Path home = validateJava8Home(
                    probed.toString(), "java_home -v 1.8", rejected);
            if (home != null) {
                return new Java8Home(home, explicitlyOffered, "");
            }
        }
        return new Java8Home(null, explicitlyOffered, String.join("; ", rejected));
    }

    private static List<String[]> offeredJava8Homes() {
        List<String[]> candidates = new ArrayList<>();
        for (String[] source : new String[][] {
                {"-Drowweave.java8.home",
                        System.getProperty("rowweave.java8.home")},
                {"ROWWEAVE_JAVA8_HOME", System.getenv("ROWWEAVE_JAVA8_HOME")},
                {"JAVA8_HOME", System.getenv("JAVA8_HOME")}}) {
            if (source[1] != null && !source[1].trim().isEmpty()) {
                candidates.add(new String[] {source[0], source[1].trim()});
            }
        }
        return candidates;
    }

    /** Null when the candidate is not a usable JDK 8, with the reason recorded. */
    private static Path validateJava8Home(String value, String label,
            List<String> rejected) {
        Path home;
        try {
            home = Paths.get(value).toAbsolutePath().normalize();
        } catch (java.nio.file.InvalidPathException invalid) {
            rejected.add(label + " is not a path: " + invalid.getMessage());
            return null;
        }
        if (!Files.isDirectory(home)) {
            rejected.add(label + " is not a directory: " + home);
            return null;
        }
        for (String tool : new String[] {"java", "javac"}) {
            Path executable = home.resolve("bin").resolve(tool);
            if (!Files.isExecutable(executable)) {
                rejected.add(label + " has no executable bin/" + tool
                        + (tool.equals("javac")
                                ? ", so it is a runtime rather than a JDK: "
                                : ": ") + home);
                return null;
            }
        }
        return home;
    }

    private static Path macOsJava8Probe() throws Exception {
        Path launcher = Paths.get("/usr/libexec/java_home");
        if (!Files.isExecutable(launcher)) return null;
        Process process = new ProcessBuilder(launcher.toString(), "-v", "1.8")
                .redirectErrorStream(true).start();
        byte[] output = readBytes(process);
        if (!process.waitFor(10L, TimeUnit.SECONDS)
                || process.exitValue() != 0) {
            process.destroyForcibly();
            return null;
        }
        String home = new String(output, StandardCharsets.UTF_8).trim();
        return home.isEmpty() ? null : Paths.get(home);
    }

    /** True when the caller has already claimed Java 8 evidence for this run. */
    private static boolean java8GateRequired() {
        return Boolean.parseBoolean(
                System.getProperty("rowweave.java8.required", "false"));
    }

    private static byte[] readBytes(Process process) throws Exception {
        java.io.ByteArrayOutputStream bytes =
                new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read; (read = process.getInputStream().read(buffer)) >= 0; ) {
            if (read > 0) bytes.write(buffer, 0, read);
        }
        return bytes.toByteArray();
    }

    private static String read(Path log) throws Exception {
        return Files.exists(log) ? new String(Files.readAllBytes(log),
                StandardCharsets.UTF_8) : "<missing log>";
    }
}
