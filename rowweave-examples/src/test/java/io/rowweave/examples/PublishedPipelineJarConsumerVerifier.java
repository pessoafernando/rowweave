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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Test-only isolated compiler/runner for the published Pipeline SDK example. */
final class PublishedPipelineJarConsumerVerifier {
    private PublishedPipelineJarConsumerVerifier() { }

    static String verify(Path coreJar, Path pipelineJar, Path sourceRoot)
            throws Exception {
        Path core = requireJar(coreJar);
        Path pipeline = requireJar(pipelineJar);
        Path workspace = Files.createTempDirectory("rowweave-pipeline-consumer-");
        try {
            Path classes = Files.createDirectories(workspace.resolve("classes"));
            Path example = sourceRoot.resolve(
                    "io/rowweave/examples/PipelineCompositionExample.java");
            if (!Files.isRegularFile(example)) {
                throw new IllegalStateException("Pipeline example is missing: " + example);
            }
            String content = new String(Files.readAllBytes(example), StandardCharsets.UTF_8);
            if (content.contains("import io.rowweave.internal.")
                    || content.contains("import io.rowweave.stream.cluster.")) {
                throw new IllegalStateException("Pipeline example imports internal APIs");
            }
            Path consumer = consumerSource(workspace);
            compile(Arrays.asList(example, consumer), classes, core, pipeline);
            return run(classes, core, pipeline, workspace.resolve("consumer.log"));
        } finally {
            deleteRecursively(workspace);
        }
    }

    private static Path requireJar(Path path) throws Exception {
        Path real = path.toRealPath();
        if (!Files.isRegularFile(real) || !real.getFileName().toString().endsWith(".jar")) {
            throw new IllegalStateException("published JAR is unavailable: " + real);
        }
        return real;
    }

    private static Path consumerSource(Path workspace) throws Exception {
        Path source = workspace.resolve("src/external/PublishedPipelineConsumer.java");
        Files.createDirectories(source.getParent());
        String code = "package external;\n"
                + "import java.nio.file.Paths;\n"
                + "import io.rowweave.artifact.PlanArtifact;\n"
                + "import io.rowweave.examples.PipelineCompositionExample;\n"
                + "import io.rowweave.pipeline.PipelineDefinition;\n"
                + "public final class PublishedPipelineConsumer {\n"
                + "  public static void main(String[] args) throws Exception {\n"
                + "    String core = Paths.get(args[0]).toRealPath().toString();\n"
                + "    String pipeline = Paths.get(args[1]).toRealPath().toString();\n"
                + "    String actualCore = Paths.get(PlanArtifact.class.getProtectionDomain()"
                + ".getCodeSource().getLocation().toURI()).toRealPath().toString();\n"
                + "    String actualPipeline = Paths.get(PipelineDefinition.class.getProtectionDomain()"
                + ".getCodeSource().getLocation().toURI()).toRealPath().toString();\n"
                + "    if (!core.equals(actualCore)) throw new IllegalStateException(\"wrong core \" + actualCore);\n"
                + "    if (!pipeline.equals(actualPipeline)) throw new IllegalStateException(\"wrong pipeline \" + actualPipeline);\n"
                + "    System.out.println(\"JAVA_SPEC=\" + System.getProperty(\"java.specification.version\"));\n"
                + "    System.out.println(\"RESULT=\" + PipelineCompositionExample.run());\n"
                + "  }\n"
                + "}\n";
        Files.write(source, code.getBytes(StandardCharsets.UTF_8));
        return source;
    }

    private static void compile(
            List<Path> sources, Path classes, Path core, Path pipeline)
            throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new IllegalStateException("a JDK compiler is required");
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager files = compiler.getStandardFileManager(
                diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
            List<File> sourceFiles = new ArrayList<>();
            for (Path source : sources) sourceFiles.add(source.toFile());
            List<String> options = Arrays.asList(
                    "-source", "8", "-target", "8", "-classpath",
                    core + File.pathSeparator + pipeline,
                    "-d", classes.toString());
            Boolean success = compiler.getTask(null, files, diagnostics,
                    options, null,
                    files.getJavaFileObjectsFromFiles(sourceFiles)).call();
            if (!Boolean.TRUE.equals(success)) {
                StringBuilder failure = new StringBuilder(
                        "published Pipeline consumer compilation failed");
                for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
                    failure.append('\n').append(diagnostic);
                }
                throw new IllegalStateException(failure.toString());
            }
        }
    }

    private static String run(
            Path classes, Path core, Path pipeline, Path log) throws Exception {
        // An unqualified Maven build forwards rowweave.java8.home as an empty
        // property, so the default in getProperty(key, default) would never be
        // used and the launcher path would come out as "bin/java".
        String requestedJavaHome = System.getProperty("rowweave.java8.home");
        if (requestedJavaHome == null || requestedJavaHome.trim().isEmpty()) {
            requestedJavaHome = System.getProperty("java.home");
        }
        String javaExecutable = java.nio.file.Paths.get(
                requestedJavaHome, "bin", "java").toString();
        String classpath = classes + File.pathSeparator + core
                + File.pathSeparator + pipeline;
        Process process = new ProcessBuilder(javaExecutable, "-Xms16m", "-Xmx64m",
                "-XX:MaxDirectMemorySize=16m", "-XX:MaxMetaspaceSize=64m",
                "-XX:ReservedCodeCacheSize=32m", "-Xss512k",
                "-XX:ActiveProcessorCount=2", "-cp", classpath,
                "external.PublishedPipelineConsumer", core.toString(), pipeline.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        boolean exited = process.waitFor(30L, TimeUnit.SECONDS);
        if (!exited) {
            process.destroyForcibly();
            process.waitFor(5L, TimeUnit.SECONDS);
        }
        String output = Files.exists(log)
                ? new String(Files.readAllBytes(log), StandardCharsets.UTF_8) : "";
        if (!exited || process.exitValue() != 0) {
            throw new IllegalStateException("published Pipeline consumer failed: " + output);
        }
        String requestedJava8Home = System.getProperty("rowweave.java8.home");
        if (requestedJava8Home != null && !requestedJava8Home.isEmpty()
                && !output.contains("JAVA_SPEC=1.8")) {
            throw new IllegalStateException(
                    "published Pipeline consumer did not run on Java 8: " + output);
        }
        int marker = output.lastIndexOf("RESULT=");
        if (marker < 0) throw new IllegalStateException("consumer emitted no result: " + output);
        int start = marker + "RESULT=".length();
        int end = output.indexOf('\n', start);
        return output.substring(start, end < 0 ? output.length() : end).trim();
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
