package io.rowweave.connector.s3;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Black-box consumer compiled and executed by an actual Java 8 JDK. */
class S3PublishedJava8ConsumerIT {
    @Test
    void publicConnectorSdkCompilesAndRunsFromPublishedJarsOnJava8(
            @TempDir Path temp) throws Exception {
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
        Path javaHome = resolved.home;
        Path javac = executable(javaHome, "javac");
        Path java = executable(javaHome, "java");
        assertTrue(javac != null && java != null,
                "resolved Java 8 JDK lost its executables: " + javaHome);

        Path deployment = requiredJar("s3.deployment.jar");
        Path core = requiredJar("s3.core.jar");
        Path source = temp.resolve("src/external/S3PublishedConsumer.java");
        Path classes = Files.createDirectories(temp.resolve("classes"));
        Files.createDirectories(source.getParent());
        Files.write(source, consumerSource().getBytes(StandardCharsets.UTF_8));
        String classpath = deployment + File.pathSeparator + core;

        Path compileLog = temp.resolve("javac.log");
        requireSuccess(new ProcessBuilder(javac.toString(),
                        "-source", "8", "-target", "8",
                        "-classpath", classpath,
                        "-d", classes.toString(), source.toString()),
                compileLog, "Java 8 consumer compilation");

        Path runLog = temp.resolve("java.log");
        requireSuccess(new ProcessBuilder(java.toString(),
                        "-Xms16m", "-Xmx96m", "-cp",
                        classes + File.pathSeparator + classpath,
                        "external.S3PublishedConsumer",
                        deployment.toString(), core.toString()),
                runLog, "Java 8 consumer execution");
        assertTrue(read(runLog).contains("S3_JAVA8_OK"), read(runLog));
    }

    private static String consumerSource() {
        return "package external;\n"
                + "import java.net.URI;\n"
                + "import java.nio.file.Path;\n"
                + "import java.nio.file.Paths;\n"
                + "import java.util.Collections;\n"
                + "import io.rowweave.RowWeave;\n"
                + "import io.rowweave.DataType;\n"
                + "import io.rowweave.connector.s3.S3ConnectorConfiguration;\n"
                + "import io.rowweave.connector.s3.S3ConnectorContract;\n"
                + "import io.rowweave.connector.s3.S3ConnectorProvider;\n"
                + "import io.rowweave.connector.s3.S3Manifest;\n"
                + "import io.rowweave.connector.s3.S3ObjectDescriptor;\n"
                + "import io.rowweave.connector.s3.S3ObjectFormat;\n"
                + "import io.rowweave.stream.api.StreamField;\n"
                + "import io.rowweave.stream.api.StreamJobEvidence;\n"
                + "import io.rowweave.stream.api.StreamJobId;\n"
                + "import io.rowweave.stream.api.StreamJobProgress;\n"
                + "import io.rowweave.stream.api.StreamSchema;\n"
                + "import io.rowweave.stream.api.StreamSourceSpec;\n"
                + "public final class S3PublishedConsumer {\n"
                + "  public static void main(String[] args) throws Exception {\n"
                + "    Path expectedConnector=Paths.get(args[0]).toRealPath();\n"
                + "    Path expectedCore=Paths.get(args[1]).toRealPath();\n"
                + "    Path actualConnector=Paths.get(S3ConnectorProvider.class"
                + ".getProtectionDomain().getCodeSource().getLocation().toURI())"
                + ".toRealPath();\n"
                + "    Path actualCore=Paths.get(RowWeave.class.getProtectionDomain()"
                + ".getCodeSource().getLocation().toURI()).toRealPath();\n"
                + "    if(!expectedConnector.equals(actualConnector)) throw new "
                + "IllegalStateException(\"connector not loaded from deployment\");\n"
                + "    if(!expectedCore.equals(actualCore)) throw new "
                + "IllegalStateException(\"core not loaded from packaged JAR\");\n"
                + "    StreamSchema schema=new StreamSchema(Collections.singletonList("
                + "new StreamField(\"id\",\"id\",\"id\",DataType.LongType,false)));\n"
                + "    S3ObjectDescriptor object=S3ObjectDescriptor.versioned("
                + "\"ledger-snapshots\",\"frozen/ledger.ndjson\",\"v1\","
                + "\"etag\",10L,\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                + "aaaaaaaaaaaaaaaa\",S3ObjectFormat.NDJSON_UTF8,schema);\n"
                + "    S3ConnectorConfiguration config=S3ConnectorConfiguration.builder()"
                + ".endpoint(URI.create(\"https://objects.invalid\"))"
                + ".region(\"us-east-1\").bucket(\"ledger-snapshots\")"
                + ".credentialsReference(\"vault/s3\").maximumObjectBytes(1024L)"
                + ".maximumRowBytes(1024).temporaryDiskBudgetBytes(1024L).build();\n"
                + "    StreamSourceSpec spec=S3Manifest.of(schema,"
                + "Collections.singletonList(object)).toSourceSpec(config);\n"
                + "    if(!spec.boundedTermination() || spec.splits().size()!=1)"
                + " throw new IllegalStateException(\"invalid bounded source\");\n"
                + "    S3ConnectorProvider provider=new S3ConnectorProvider();\n"
                + "    if(!S3ConnectorContract.PROVIDER_KEY.equals(provider.providerKey())"
                + " || !S3ConnectorContract.contractDigest().equals("
                + "provider.contractDigest())) throw new IllegalStateException("
                + "\"provider contract drift\");\n"
                + "    if(io.rowweave.StreamJobHandle.class.getMethod("
                + "\"evidence\").getReturnType()!=StreamJobEvidence.class)"
                + " throw new IllegalStateException(\"evidence API drift\");\n"
                + "    StreamJobProgress progress=StreamJobProgress.empty();\n"
                + "    StreamJobEvidence evidence=StreamJobEvidence.unavailable("
                + "StreamJobId.of(\"published-s3-consumer\"),progress,1L);\n"
                + "    if(evidence.available()||evidence.complete()"
                + "||evidence.attemptProgress().recordsRead()!=0L"
                + "||evidence.lifetimeProgress().backlogBytes()!=0L)"
                + " throw new IllegalStateException(\"invalid public evidence\");\n"
                + "    System.out.println(\"S3_JAVA8_OK\");\n"
                + "  }\n"
                + "}\n";
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
     * bin/javac, because the browser-plugin runtime on macOS offers java without
     * javac and would silently drop the compile step.
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

    /** True when the caller has already claimed Java 8 evidence for this run. */
    private static boolean java8GateRequired() {
        return Boolean.parseBoolean(
                System.getProperty("rowweave.java8.required", "false"));
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
        if (executable(home, "java") == null) {
            rejected.add(label + " has no executable bin/java: " + home);
            return null;
        }
        if (executable(home, "javac") == null) {
            rejected.add(label + " is a Java 8 runtime, not a JDK: " + home
                    + " has no executable bin/javac");
            return null;
        }
        return home;
    }

    private static Path macOsJava8Probe() throws Exception {
        Path launcher = Paths.get("/usr/libexec/java_home");
        if (!Files.isExecutable(launcher)) return null;
        Process process = new ProcessBuilder(
                launcher.toString(), "-v", "1.8").redirectErrorStream(true)
                .start();
        byte[] output = bytes(process.getInputStream());
        if (!process.waitFor(10L, TimeUnit.SECONDS)
                || process.exitValue() != 0) {
            process.destroyForcibly();
            return null;
        }
        String home = new String(output, StandardCharsets.UTF_8).trim();
        return home.isEmpty() ? null : Paths.get(home);
    }

    private static Path executable(Path home, String name) {
        Path executable = home.resolve("bin").resolve(name);
        return Files.isExecutable(executable) ? executable : null;
    }

    private static void requireSuccess(ProcessBuilder builder, Path log,
            String operation) throws Exception {
        Process process = builder.redirectErrorStream(true)
                .redirectOutput(log.toFile()).start();
        boolean exited = process.waitFor(30L, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5L, TimeUnit.SECONDS);
        }
        assertTrue(exited, operation + " timed out:\n" + read(log));
        assertEquals(0, process.exitValue(),
                operation + " failed:\n" + read(log));
    }

    private static byte[] bytes(InputStream input) throws Exception {
        java.io.ByteArrayOutputStream output =
                new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            if (read > 0) output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static String read(Path log) throws Exception {
        return Files.exists(log)
                ? new String(Files.readAllBytes(log), StandardCharsets.UTF_8)
                : "<missing log>";
    }
}
