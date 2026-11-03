package io.rowweave.build;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the public vocabulary approved by ADR 0001. The contract intentionally
 * uses names rather than compile-time imports so it can fail before migration.
 */
class PublicApiVocabularyContractTest {

    private static List<String> strings(Object value) {
        List<?> raw = (List<?>) value;
        List<String> result = new ArrayList<>();
        for (Object item : raw) {
            result.add((String) item);
        }
        return result;
    }

    private static Class<?> parameterType(String name) throws ClassNotFoundException {
        switch (name) {
            case "boolean": return boolean.class;
            case "byte": return byte.class;
            case "char": return char.class;
            case "short": return short.class;
            case "int": return int.class;
            case "long": return long.class;
            case "float": return float.class;
            case "double": return double.class;
            case "void": return void.class;
            default: break;
        }
        if (name.equals("byte[]")) return byte[].class;
        if (name.endsWith("[]")) {
            return Class.forName("[L" + name.substring(0, name.length() - 2) + ";");
        }
        return Class.forName(name);
    }

    private static Method findMethod(Class<?> owner, String name, Class<?>[] parameters) {
        for (Method method : owner.getMethods()) {
            if (!method.getName().equals(name)) {
                continue;
            }
            if (Arrays.equals(method.getParameterTypes(), parameters)) {
                return method;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> contract() throws IOException {
        InputStream rawInput = PublicApiVocabularyContractTest.class
                .getResourceAsStream("/api/public-api-vocabulary-0.2.0.json");
        if (rawInput == null) {
            throw new IllegalStateException("public API contract fixture is missing");
        }
        try (InputStream input = rawInput) {
            java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            for (int read; (read = input.read(buffer)) >= 0; ) {
                bytes.write(buffer, 0, read);
            }
            String json = new String(bytes.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
            return (java.util.Map<String, Object>) readJson(json);
        }
    }

    private static Object readJson(String json) throws IOException {
        // Avoid adding a production JSON dependency to a build gate.
        return JsonContractParser.parse(json);
    }

    @Test
    void approvedVocabularyIsPresent() throws Exception {
        List<String> missing = new ArrayList<>();
        for (String className : strings(contract().get("requiredClasses"))) {
            if (classForNameOrNull(className) == null) {
                missing.add(className);
            }
        }
        assertTrue(missing.isEmpty(), () -> "missing approved public classes: " + missing);
    }

    @Test
    void legacyVocabularyIsAbsent() throws Exception {
        List<String> present = new ArrayList<>();
        for (String className : strings(contract().get("forbiddenClasses"))) {
            if (classForNameOrNull(className) != null) {
                present.add(className);
            }
        }
        assertTrue(present.isEmpty(), () -> "forbidden pre-0.2.0 public classes remain: " + present);
    }

    @Test
    void retainedVocabularyIsPresent() throws Exception {
        List<String> missing = new ArrayList<>();
        for (String className : strings(contract().get("retainedClasses"))) {
            if (classForNameOrNull(className) == null) {
                missing.add(className);
            }
        }
        assertTrue(missing.isEmpty(), () -> "missing retained public classes: " + missing);
    }

    @Test
    void approvedSignaturesArePresent() throws Exception {
        List<String> problems = new ArrayList<>();
        for (Object rawMethod : (List<?>) contract().get("methods")) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> method =
                    (java.util.Map<String, Object>) rawMethod;
            String ownerName = (String) method.get("owner");
            String methodName = (String) method.get("name");
            String returnName = (String) method.get("returns");
            List<String> parameterNames = strings(method.get("parameters"));

            Class<?> owner = classForNameOrNull(ownerName);
            if (owner == null) {
                problems.add(ownerName + "#" + methodName + ": owner is absent");
                continue;
            }
            Class<?>[] parameters = new Class<?>[parameterNames.size()];
            for (int i = 0; i < parameters.length; i++) {
                parameters[i] = parameterType(parameterNames.get(i));
            }
            Method actual = findMethod(owner, methodName, parameters);
            if (actual == null) {
                problems.add(ownerName + "#" + methodName + parameterNames);
                continue;
            }
            assertEquals(parameterType(returnName), actual.getReturnType(),
                    ownerName + "#" + methodName + " return type");
        }
        assertTrue(problems.isEmpty(), () -> "missing approved public signatures: " + problems);
    }

    private static Class<?> classForNameOrNull(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException absent) {
            return null;
        } catch (LinkageError absent) {
            return null;
        }
    }

    /**
     * Minimal parser for the flat contract fixture. It keeps the gate
     * independent of production JSON libraries and supports only the syntax
     * emitted by the checked-in fixture.
     */
    private static final class JsonContractParser {
        private int pos;

        private JsonContractParser(String json) {
            this.value = json;
        }

        private final String value;

        static Object parse(String json) throws IOException {
            JsonContractParser parser = new JsonContractParser(json);
            parser.whitespace();
            Object result = parser.value();
            parser.whitespace();
            if (parser.pos != parser.value.length()) {
                throw new IOException("trailing JSON input");
            }
            return result;
        }

        private Object value() throws IOException {
            whitespace();
            char current = at(pos);
            if (current == '{') {
                return object();
            }
            if (current == '[') {
                return array();
            }
            if (current == '"') {
                return string();
            }
            int end = pos;
            while (end < value.length() && "-+.eE0123456789truefalsn".indexOf(value.charAt(end)) >= 0) {
                end++;
            }
            String token = value.substring(pos, end);
            pos = end;
            if (token.equals("true")) return Boolean.TRUE;
            if (token.equals("false")) return Boolean.FALSE;
            if (token.equals("null") || token.isEmpty()) throw new IOException("invalid JSON value");
            return token;
        }

        private java.util.Map<String, Object> object() throws IOException {
            java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
            expect('{');
            whitespace();
            if (at(pos) == '}') {
                pos++;
                return result;
            }
            while (true) {
                whitespace();
                String key = string();
                whitespace();
                expect(':');
                result.put(key, value());
                whitespace();
                char separator = at(pos++);
                if (separator == '}') return result;
                if (separator != ',') throw new IOException("expected , or } in object");
            }
        }

        private List<Object> array() throws IOException {
            List<Object> result = new ArrayList<>();
            expect('[');
            whitespace();
            if (at(pos) == ']') {
                pos++;
                return result;
            }
            while (true) {
                result.add(value());
                whitespace();
                char separator = at(pos++);
                if (separator == ']') return result;
                if (separator != ',') throw new IOException("expected , or ] in array");
            }
        }

        private String string() throws IOException {
            expect('"');
            StringBuilder result = new StringBuilder();
            while (pos < value.length()) {
                char current = value.charAt(pos++);
                if (current == '"') return result.toString();
                if (current == '\\') {
                    char escaped = value.charAt(pos++);
                    if (escaped == 'n') result.append('\n');
                    else if (escaped == 'u') result.append(unicodeEscape());
                    else result.append(escaped);
                } else {
                    result.append(current);
                }
            }
            throw new IOException("unterminated JSON string");
        }

        private char unicodeEscape() throws IOException {
            if (pos + 4 > value.length()) throw new IOException("truncated unicode escape");
            String digits = value.substring(pos, pos + 4);
            try {
                char decoded = (char) Integer.parseInt(digits, 16);
                pos += 4;
                return decoded;
            } catch (NumberFormatException ex) {
                throw new IOException("invalid unicode escape: \\u" + digits);
            }
        }

        private void whitespace() {
            while (pos < value.length() && Character.isWhitespace(value.charAt(pos))) pos++;
        }

        private char at(int index) throws IOException {
            if (index >= value.length()) throw new IOException("unexpected end of JSON");
            return value.charAt(index);
        }

        private void expect(char expected) throws IOException {
            char actual = at(pos++);
            if (actual != expected) throw new IOException("expected " + expected + " but found " + actual);
        }
    }
}
