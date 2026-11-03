package io.rowweave.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Shared resource envelope for every JVM forked by the core test suite. */
public final class ForkedJvm {

    private ForkedJvm() {
    }

    public static List<String> baseCommand() {
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + File.separator + "bin"
                + File.separator + "java");
        command.add("-Xms64m");
        command.add("-Xmx2048m");
        command.add("-XX:MaxDirectMemorySize=256m");
        command.add("-XX:MaxMetaspaceSize=256m");
        command.add("-XX:ReservedCodeCacheSize=128m");
        command.add("-Xss1m");
        command.add("-XX:ActiveProcessorCount=4");
        command.add("-Drowweave.process.memoryLimitBytes=8589934592");
        command.add(temporaryDirectoryArgument(""));
        return command;
    }

    /** Append JVM arguments while replacing, rather than duplicating, hard resource caps. */
    public static void appendJvmArguments(List<String> command,
                                          List<String> arguments) {
        if (!containsPrefix(command, "-Djava.io.tmpdir=")) {
            command.add(temporaryDirectoryArgument(""));
        }
        for (String argument : arguments) {
            String prefix = replaceablePrefix(argument);
            if (prefix != null) {
                command.removeIf(existing -> existing.startsWith(prefix));
            }
            command.add(argument);
        }
    }

    public static List<String> command(String mainClass, String... arguments) {
        List<String> command = baseCommand();
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(mainClass);
        command.addAll(Arrays.asList(arguments));
        return command;
    }

    public static List<String> keytoolCommand(String... arguments) {
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + File.separator + "bin"
                + File.separator + "keytool");
        command.add("-J-Xms64m");
        command.add("-J-Xmx512m");
        command.add("-J-XX:MaxDirectMemorySize=64m");
        command.add("-J-XX:MaxMetaspaceSize=128m");
        command.add("-J-XX:ReservedCodeCacheSize=64m");
        command.add("-J-Xss1m");
        command.add("-J-XX:ActiveProcessorCount=2");
        command.add(temporaryDirectoryArgument("-J"));
        command.addAll(Arrays.asList(arguments));
        return command;
    }

    private static boolean containsPrefix(List<String> command, String prefix) {
        for (String argument : command) {
            if (argument.startsWith(prefix)) return true;
        }
        return false;
    }

    private static String temporaryDirectoryArgument(String toolPrefix) {
        String temporaryDirectory = System.getProperty("java.io.tmpdir");
        if (temporaryDirectory == null || temporaryDirectory.trim().isEmpty()) {
            throw new IllegalStateException("java.io.tmpdir is not configured");
        }
        return toolPrefix + "-Djava.io.tmpdir=" + temporaryDirectory;
    }

    private static String replaceablePrefix(String argument) {
        if (argument.startsWith("-Xms")) return "-Xms";
        if (argument.startsWith("-Xmx")) return "-Xmx";
        if (argument.startsWith("-Xss")) return "-Xss";
        if (argument.startsWith("-XX:MaxDirectMemorySize=")) {
            return "-XX:MaxDirectMemorySize=";
        }
        if (argument.startsWith("-XX:MaxMetaspaceSize=")) {
            return "-XX:MaxMetaspaceSize=";
        }
        if (argument.startsWith("-XX:ReservedCodeCacheSize=")) {
            return "-XX:ReservedCodeCacheSize=";
        }
        if (argument.startsWith("-XX:ActiveProcessorCount=")) {
            return "-XX:ActiveProcessorCount=";
        }
        if (argument.startsWith("-Drowweave.process.memoryLimitBytes=")) {
            return "-Drowweave.process.memoryLimitBytes=";
        }
        if (argument.startsWith("-Djava.io.tmpdir=")) {
            return "-Djava.io.tmpdir=";
        }
        return null;
    }
}
