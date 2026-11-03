package io.rowweave.examples;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Test-only isolated compiler/runner for the published Stream examples. */
final class PublishedStreamJarConsumerVerifier {
    private static final String EXAMPLE_PACKAGE = "io/rowweave/examples/";
    private static final String CONSUMER = "external.PublishedStreamConsumer";
    private static final List<String> EXAMPLES = Arrays.asList(
            "StreamLocalAppendExample",
            "StreamAtomicCompositionExample",
            "StreamElasticExample",
            "StreamLocalEventTimeRecoveryExample",
            "StreamConnectorConformanceExample",
            "StreamClusterCdcExample");

    private PublishedStreamJarConsumerVerifier() { }

    static List<String> verify(Path coreJar, Path sourceRoot)
            throws Exception {
        Path publishedCore = coreJar.toRealPath();
        if (!Files.isRegularFile(publishedCore)
                || !publishedCore.getFileName().toString().endsWith(".jar")) {
            throw new IllegalStateException(
                    "packaged core JAR is unavailable: " + publishedCore);
        }
        Path workspace = Files.createTempDirectory(
                "rowweave-stream-published-consumer-");
        try {
            Path classes = Files.createDirectories(workspace.resolve("classes"));
            List<Path> sources = sources(sourceRoot);
            rejectInternalImports(sources);
            sources.add(consumerSource(workspace));
            List<Path> runtimeJars = runtimeJars(publishedCore);
            compile(sources, classes, publishedCore, runtimeJars);
            return runExamples(workspace, classes, publishedCore, runtimeJars);
        } finally {
            deleteRecursively(workspace);
        }
    }

    private static List<Path> sources(Path sourceRoot) {
        List<Path> result = new ArrayList<>();
        for (String example : EXAMPLES) {
            Path source = sourceRoot.resolve(
                    EXAMPLE_PACKAGE + example + ".java");
            if (!Files.isRegularFile(source)) {
                throw new IllegalStateException(
                        "published example source is missing: " + source);
            }
            result.add(source);
        }
        return result;
    }

    private static void rejectInternalImports(List<Path> sources)
            throws Exception {
        List<String> forbidden = Arrays.asList(
                "import io.rowweave.stream.cluster.",
                "import io.rowweave.stream.contract.",
                "import io.rowweave.stream.runtime.",
                "import io.rowweave.internal.");
        for (Path source : sources) {
            String content = new String(Files.readAllBytes(source),
                    StandardCharsets.UTF_8);
            for (String prefix : forbidden) {
                if (content.contains(prefix)) {
                    throw new IllegalStateException(
                            "example imports an internal package: " + source);
                }
            }
        }
    }

    private static Path consumerSource(Path workspace) throws Exception {
        Path source = workspace.resolve(
                "consumer-src/external/PublishedStreamConsumer.java");
        Files.createDirectories(source.getParent());
        String code = "package external;\n"
                + "import java.nio.file.Path;\n"
                + "import java.nio.file.Paths;\n"
                + "import io.rowweave.RowWeave;\n"
                + "import io.rowweave.examples.StreamClusterCdcExample;\n"
                + "import io.rowweave.examples.StreamConnectorConformanceExample;\n"
                + "import io.rowweave.examples.StreamLocalAppendExample;\n"
                + "import io.rowweave.examples.StreamAtomicCompositionExample;\n"
                + "import io.rowweave.examples.StreamElasticExample;\n"
                + "import io.rowweave.examples.StreamLocalEventTimeRecoveryExample;\n"
                + "public final class PublishedStreamConsumer {\n"
                + "  public static void main(String[] args) throws Exception {\n"
                + "    Path expected = Paths.get(args[2]).toRealPath();\n"
                + "    Path actual = Paths.get(RowWeave.class.getProtectionDomain()"
                + ".getCodeSource().getLocation().toURI()).toRealPath();\n"
                + "    if (!expected.equals(actual)) throw new IllegalStateException("
                + "\"RowWeave not loaded from packaged JAR: \" + actual);\n"
                + "    Path state = Paths.get(args[1]);\n"
                + "    Object result;\n"
                + "    if (\"append\".equals(args[0])) result = "
                + "StreamLocalAppendExample.run(state);\n"
                + "    else if (\"atomic\".equals(args[0])) result = "
                + "StreamAtomicCompositionExample.run();\n"
                + "    else if (\"elastic\".equals(args[0])) result = "
                + "StreamElasticExample.run(state);\n"
                + "    else if (\"event-time\".equals(args[0])) result = "
                + "StreamLocalEventTimeRecoveryExample.run(state);\n"
                + "    else if (\"conformance\".equals(args[0])) result = "
                + "StreamConnectorConformanceExample.run();\n"
                + "    else if (\"cluster\".equals(args[0])) result = "
                + "StreamClusterCdcExample.runMaster(state);\n"
                + "    else throw new IllegalArgumentException(args[0]);\n"
                + "    System.out.println(\"RESULT=\" + result);\n"
                + "  }\n"
                + "}\n";
        Files.write(source, code.getBytes(StandardCharsets.UTF_8));
        return source;
    }

    private static List<Path> runtimeJars(Path coreJar) throws Exception {
        String classpath = System.getProperty("surefire.test.class.path",
                System.getProperty("java.class.path", ""));
        List<Path> result = new ArrayList<>();
        for (String entry : classpath.split(
                java.util.regex.Pattern.quote(File.pathSeparator))) {
            if (entry.isEmpty()) continue;
            Path path = Paths.get(entry);
            String name = path.getFileName() == null ? ""
                    : path.getFileName().toString();
            if (!name.endsWith(".jar") || name.startsWith("rowweave-")
                    || testOnly(name) || !Files.isRegularFile(path)) {
                continue;
            }
            Path real = path.toRealPath();
            if (!real.equals(coreJar) && !result.contains(real)) {
                result.add(real);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException(
                    "published runtime dependency JARs are unavailable");
        }
        return Collections.unmodifiableList(result);
    }

    private static boolean testOnly(String name) {
        return name.startsWith("junit-")
                || name.startsWith("opentest4j-")
                || name.startsWith("apiguardian-");
    }

    private static void compile(List<Path> sources, Path classes,
            Path coreJar, List<Path> runtimeJars) throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("a JDK compiler is required");
        }
        DiagnosticCollector<JavaFileObject> diagnostics =
                new DiagnosticCollector<>();
        try (StandardJavaFileManager files = compiler.getStandardFileManager(
                diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
            List<File> sourceFiles = new ArrayList<>();
            for (Path source : sources) sourceFiles.add(source.toFile());
            List<String> options = Arrays.asList(
                    "-source", "8", "-target", "8",
                    "-classpath", classpath(coreJar, runtimeJars),
                    "-d", classes.toString());
            Boolean success = compiler.getTask(null, files, diagnostics,
                    options, null,
                    files.getJavaFileObjectsFromFiles(sourceFiles)).call();
            if (!Boolean.TRUE.equals(success)) {
                StringBuilder message = new StringBuilder(
                        "published Stream example compilation failed");
                for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
                    message.append('\n').append(diagnostic);
                }
                throw new IllegalStateException(message.toString());
            }
        }
    }

    private static List<String> runExamples(Path workspace, Path classes,
            Path coreJar, List<Path> runtimeJars) throws Exception {
        String classpath = classes + File.pathSeparator
                + classpath(coreJar, runtimeJars);
        List<String> result = new ArrayList<>();
        result.add("append=" + runConsumer("append",
                workspace.resolve("append"), workspace.resolve("append.log"),
                coreJar, classpath));
        result.add("atomic=" + runConsumer("atomic",
                workspace.resolve("atomic"), workspace.resolve("atomic.log"),
                coreJar, classpath));
        result.add("elastic=" + runConsumer("elastic",
                workspace.resolve("elastic"), workspace.resolve("elastic.log"),
                coreJar, classpath));
        result.add("event-time=" + runConsumer("event-time",
                workspace.resolve("event-time"),
                workspace.resolve("event-time.log"), coreJar, classpath));
        result.add("conformance=" + runConsumer("conformance",
                workspace.resolve("conformance"),
                workspace.resolve("conformance.log"), coreJar, classpath));
        result.add("cluster=" + runCluster(workspace.resolve("cluster"),
                coreJar, classpath));
        return Collections.unmodifiableList(result);
    }

    private static String runCluster(
            Path exchange, Path coreJar, String classpath) throws Exception {
        Files.createDirectories(exchange);
        Process source = launch(classpath,
                "io.rowweave.examples.StreamClusterCdcExample",
                Arrays.asList("source-worker", exchange.toString()),
                exchange.resolve("source-worker.log"));
        Process sink = launch(classpath,
                "io.rowweave.examples.StreamClusterCdcExample",
                Arrays.asList("sink-worker", exchange.toString()),
                exchange.resolve("sink-worker.log"));
        try {
            awaitNonEmpty(exchange.resolve("source-worker.port"), 5_000L);
            awaitNonEmpty(exchange.resolve("sink-worker.port"), 5_000L);
            return runConsumer("cluster", exchange,
                    exchange.resolve("master.log"), coreJar, classpath);
        } finally {
            Files.write(exchange.resolve("shutdown"), new byte[] {1});
            requireExit(source, exchange.resolve("source-worker.log"));
            requireExit(sink, exchange.resolve("sink-worker.log"));
        }
    }

    private static String runConsumer(String mode, Path state,
            Path log, Path coreJar, String classpath) throws Exception {
        Files.createDirectories(state);
        Process process = launch(classpath, CONSUMER,
                Arrays.asList(mode, state.toString(), coreJar.toString()), log);
        requireExit(process, log);
        String output = new String(
                Files.readAllBytes(log), StandardCharsets.UTF_8);
        int marker = output.lastIndexOf("RESULT=");
        if (marker < 0) {
            throw new IllegalStateException(
                    "published consumer emitted no result: " + output);
        }
        int start = marker + "RESULT=".length();
        int end = output.indexOf('\n', start);
        return output.substring(start, end < 0 ? output.length() : end).trim();
    }

    private static Process launch(String classpath, String main,
            List<String> arguments, Path log) throws Exception {
        String java = Paths.get(System.getProperty("java.home"), "bin", "java")
                .toString();
        List<String> command = new ArrayList<>(Arrays.asList(
                java, "-Xms32m", "-Xmx128m",
                "-XX:MaxDirectMemorySize=32m",
                "-XX:MaxMetaspaceSize=128m",
                "-XX:ReservedCodeCacheSize=32m", "-Xss512k",
                "-XX:ActiveProcessorCount=2", "-cp", classpath, main));
        command.addAll(arguments);
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
    }

    private static void requireExit(Process process, Path log)
            throws Exception {
        boolean exited = process.waitFor(30L, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5L, TimeUnit.SECONDS);
        }
        if (!exited || process.exitValue() != 0) {
            String output = Files.exists(log)
                    ? new String(Files.readAllBytes(log), StandardCharsets.UTF_8)
                    : "missing process log";
            throw new IllegalStateException(
                    "published Stream process failed: " + output);
        }
    }

    private static void awaitNonEmpty(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while ((!Files.exists(file) || Files.size(file) == 0L)
                && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        if (!Files.exists(file) || Files.size(file) == 0L) {
            throw new IllegalStateException(
                    "published Worker did not become ready: " + file);
        }
    }

    private static String classpath(
            Path coreJar, List<Path> runtimeJars) {
        StringBuilder value = new StringBuilder(coreJar.toString());
        for (Path jar : runtimeJars) {
            value.append(File.pathSeparator).append(jar);
        }
        return value.toString();
    }

    private static void deleteRecursively(Path root) throws Exception {
        if (!Files.exists(root)) return;
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths
                    .sorted(Comparator.reverseOrder())::iterator) {
                Files.deleteIfExists(path);
            }
        }
    }
}
