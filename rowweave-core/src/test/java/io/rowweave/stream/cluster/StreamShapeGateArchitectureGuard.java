package io.rowweave.stream.cluster;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/** Source-level architecture guard that rejects topology-shaped runtime dispatch. */
final class StreamShapeGateArchitectureGuard {
    private static final Pattern TOPOLOGY_CLASSIFIER = Pattern.compile(
            "\\b(?:is|has|accepts|allows|requires|validate|classify|select|"
                    + "choose|dispatch|route|admit)[A-Za-z0-9_$]*"
                    + "(?:Shape|Topology|Graph)[A-Za-z0-9_$]*\\s*\\(");
    private static final Pattern GRAPH_BOOLEAN_PREDICATE = Pattern.compile(
            "\\b(?:boolean|Boolean)\\s+"
                    + "(?:is|has|accepts|allows|requires|validate|classify|"
                    + "select|choose|dispatch|route|admit|eligible|supports|"
                    + "matches)[A-Za-z0-9_$]*\\s*\\([^)]*"
                    + "(?:StreamExecutionGraph(?!\\s*\\.)|"
                    + "StreamProductExecutionGraph(?!\\s*\\.)|"
                    + "PortableStreamPlan(?!\\s*\\.)|PlanArtifact)\\b[^)]*\\)");
    private static final String GRAPH_CARDINALITY =
            "(?:[A-Za-z_$][A-Za-z0-9_$]*\\s*\\.\\s*)*"
                    + "(?:nodes\\s*\\(\\s*\\)|edges\\s*\\(\\s*\\)|rootSinkOperatorUids\\s*\\(\\s*\\)"
                    + "|sources|sinks)\\s*\\.\\s*size\\s*\\(\\s*\\)";
    private static final String COMPARISON = "(?:==|!=|<=|>=|<|>)";
    private static final Pattern FIXED_GRAPH_CARDINALITY = Pattern.compile(
            "(?:" + GRAPH_CARDINALITY + "\\s*" + COMPARISON
                    + "\\s*\\d+|\\d+\\s*" + COMPARISON + "\\s*"
                    + GRAPH_CARDINALITY + ")");
    private static final Pattern PREDECESSOR_OPCODE = Pattern.compile(
            "\\b(?:producer|predecessor|upstream|parent)[A-Za-z0-9_$]*\\s*"
                    + "(?:\\(\\s*\\))?\\s*\\.\\s*"
                    + "(?:portableNode\\s*\\(\\s*\\)\\s*\\.\\s*)?"
                    + "opcode\\s*\\(");
    private static final Pattern OPCODE_INSPECTION = Pattern.compile(
            "(?:\\.\\s*opcode\\s*\\(|\\bStreamOperatorCode\\b)");

    private StreamShapeGateArchitectureGuard() { }

    static Set<String> findings(String source) {
        String code = codeOnly(source);
        Set<String> findings = new LinkedHashSet<>();
        if (TOPOLOGY_CLASSIFIER.matcher(code).find()) {
            findings.add("topology-classifier");
        }
        if (GRAPH_BOOLEAN_PREDICATE.matcher(code).find()) {
            findings.add("graph-boolean-predicate");
        }
        if (FIXED_GRAPH_CARDINALITY.matcher(code).find()) {
            findings.add("fixed-graph-cardinality");
        }
        if (PREDECESSOR_OPCODE.matcher(code).find()) {
            findings.add("predecessor-opcode-classifier");
        }
        return Collections.unmodifiableSet(findings);
    }

    static boolean inspectsOpcode(String source) {
        return OPCODE_INSPECTION.matcher(codeOnly(source)).find();
    }

    static String methodBody(String source, String signatureToken) {
        if (source == null || signatureToken == null
                || signatureToken.isEmpty()) {
            throw new IllegalArgumentException(
                    "source and method signature token are required");
        }
        String code = codeOnly(source);
        int signature = code.indexOf(signatureToken);
        if (signature < 0) {
            throw new IllegalArgumentException(
                    "method signature is absent: " + signatureToken);
        }
        int start = code.indexOf('{', signature + signatureToken.length());
        if (start < 0) {
            throw new IllegalArgumentException(
                    "method body is absent: " + signatureToken);
        }
        int depth = 0;
        for (int index = start; index < code.length(); index++) {
            char current = code.charAt(index);
            if (current == '{') depth++;
            if (current == '}' && --depth == 0) {
                return code.substring(start, index + 1);
            }
        }
        throw new IllegalArgumentException(
                "method body is not balanced: " + signatureToken);
    }

    /** Blanks comments and literals without moving braces or identifiers. */
    private static String codeOnly(String source) {
        if (source == null) return "";
        StringBuilder result = new StringBuilder(source.length());
        int state = 0;
        for (int index = 0; index < source.length(); index++) {
            char current = source.charAt(index);
            char next = index + 1 < source.length()
                    ? source.charAt(index + 1) : '\0';
            if (state == 0 && current == '/' && next == '/') {
                result.append(' ').append(' ');
                index++;
                state = 1;
            } else if (state == 0 && current == '/' && next == '*') {
                result.append(' ').append(' ');
                index++;
                state = 2;
            } else if (state == 0 && current == '"') {
                result.append(' ');
                state = 3;
            } else if (state == 0 && current == '\'') {
                result.append(' ');
                state = 4;
            } else if (state == 1 && (current == '\n' || current == '\r')) {
                result.append(current);
                state = 0;
            } else if (state == 2 && current == '*' && next == '/') {
                result.append(' ').append(' ');
                index++;
                state = 0;
            } else if ((state == 3 || state == 4) && current == '\\') {
                result.append(' ');
                if (index + 1 < source.length()) {
                    result.append(' ');
                    index++;
                }
            } else if (state == 3 && current == '"') {
                result.append(' ');
                state = 0;
            } else if (state == 4 && current == '\'') {
                result.append(' ');
                state = 0;
            } else {
                result.append(state == 0 ? current
                        : current == '\n' || current == '\r'
                        ? current : ' ');
            }
        }
        return result.toString();
    }
}
