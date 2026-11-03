package io.rowweave.soak;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Reads actual OS process and filesystem usage; never substitutes ceilings. */
final class StreamSoakResourceSampler {
    private StreamSoakResourceSampler() { }

    static ProcessUsage process(Process process) throws Exception {
        if (process == null || !process.isAlive()) {
            throw new IllegalStateException("STREAM_SOAK_PROCESS_NOT_LIVE");
        }
        long pid = pid(process);
        String rss = command("ps", "-o", "rss=", "-p",
                Long.toString(pid)).trim();
        if (rss.isEmpty()) {
            throw new IllegalStateException("STREAM_SOAK_PROCESS_RSS_UNAVAILABLE");
        }
        long rssBytes = Math.multiplyExact(Long.parseLong(rss), 1024L);
        String descriptors = command("lsof", "-n", "-P", "-p",
                Long.toString(pid));
        int lines = descriptors.isEmpty() ? 0
                : descriptors.split("\\R").length;
        if (lines < 2) {
            throw new IllegalStateException("STREAM_SOAK_PROCESS_FD_UNAVAILABLE");
        }
        return new ProcessUsage(pid, rssBytes, lines - 1L);
    }

    static long diskBytes(Path root) throws Exception {
        if (!Files.exists(root)) return 0L;
        final long[] total = {0L};
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(Path file,
                    BasicFileAttributes attributes) {
                if (attributes.isRegularFile()) {
                    total[0] = Math.addExact(total[0], attributes.size());
                }
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFileFailed(Path file,
                    IOException failure) throws IOException {
                return onVisitFailure(failure);
            }
        });
        return total[0];
    }

    static FileVisitResult onVisitFailure(IOException failure)
            throws IOException {
        if (failure instanceof NoSuchFileException) {
            return FileVisitResult.CONTINUE;
        }
        throw failure;
    }

    private static long pid(Process process) throws Exception {
        Method method = Process.class.getMethod("pid");
        return ((Number) method.invoke(process)).longValue();
    }

    private static String command(String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        Collections.addAll(command, arguments);
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).start();
        byte[] output;
        try (InputStream input = process.getInputStream()) {
            output = readAll(input);
        }
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("STREAM_SOAK_RESOURCE_COMMAND_FAILED: "
                    + command + ": "
                    + new String(output, StandardCharsets.UTF_8));
        }
        return new String(output, StandardCharsets.UTF_8);
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    static final class ProcessUsage {
        final long pid;
        final long rssBytes;
        final long fileDescriptors;

        ProcessUsage(long pid, long rssBytes, long fileDescriptors) {
            this.pid = pid;
            this.rssBytes = rssBytes;
            this.fileDescriptors = fileDescriptors;
        }
    }
}
