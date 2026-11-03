package io.rowweave.build;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protects semantic formats while public class names move. Reflection is
 * deliberate: this gate runs before and after the ADR 0001 rename.
 */
class ApiRenamePersistenceGuardTest {

    private static Class<?> renamed(String oldName, String newName) throws ClassNotFoundException {
        try {
            return Class.forName(oldName);
        } catch (ClassNotFoundException preRename) {
            return Class.forName(newName);
        }
    }

    private static Object schemaInstance() throws Exception {
        Class<?> schemaType = renamed("io.rowweave.Row\u004deta", "io.rowweave.DataSchema");
        Constructor<?> constructor = schemaType.getConstructor(Field[].class);
        Object fields = new Field[]{
                new Field("adaptor", DataType.StringType),
                new Field("amount", DataType.BigDecimalType)};
        return constructor.newInstance(fields);
    }

    private static Object rowInstance() throws Exception {
        Class<?> rowType = renamed("io.rowweave.Row", "io.rowweave.DataRow");
        Constructor<?> constructor = rowType.getConstructor(int.class);
        Object row = constructor.newInstance(2);
        rowType.getMethod("set", int.class, Object.class).invoke(row, 0, "A");
        rowType.getMethod("set", int.class, Object.class).invoke(row, 1, new BigDecimal("100"));
        return row;
    }

    private static Object invoke(Object target, String method, Object... arguments) throws Exception {
        Class<?>[] types = new Class<?>[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getClass();
        }
        Method actual = target.getClass().getMethod(method, types);
        return actual.invoke(target, arguments);
    }

    @Test
    void schemaFingerprintRemainsFormatVersionOne() throws Exception {
        Class<?> schemaType = renamed("io.rowweave.Row\u004deta", "io.rowweave.DataSchema");
        Object schema = schemaInstance();
        int count = (Integer) schemaType.getMethod("getFieldCount").invoke(schema);
        assertEquals(2, count);

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (DataOutputStream output = new DataOutputStream(
                new java.io.ByteArrayOutputStream() {
                    @Override
                    public void write(byte[] buffer, int offset, int length) {
                        digest.update(buffer, offset, length);
                    }

                    @Override
                    public void write(int value) {
                        digest.update((byte) value);
                    }
                })) {
            output.writeInt(1);
            output.writeInt(count);
            writeField(output, schemaType, schema, 0);
            writeField(output, schemaType, schema, 1);
        }
        String expected = hex(digest.digest());

        Class<?> fingerprintType = Class.forName("io.rowweave.commit.SchemaFingerprint");
        Method of = fingerprintType.getMethod("of", schemaType);
        assertEquals(expected, of.invoke(null, schema));
    }

    private static void writeField(
            DataOutputStream output, Class<?> schemaType, Object schema, int index)
            throws Exception {
        Object field = schemaType.getMethod("getField", int.class).invoke(schema, index);
        writeText(output, (String) Field.class.getMethod("getName").invoke(field));
        writeText(output, (String) Field.class.getMethod("getAlias").invoke(field));
        Object dataType = Field.class.getMethod("getDataType").invoke(field);
        writeText(output, dataType.getClass().getMethod("name").invoke(dataType).toString());
    }

    private static void writeText(DataOutputStream output, String value) throws Exception {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    @Test
    void renamedValueAndSchemaTypesSerializeWithinOneVersion() throws Exception {
        Object row = rowInstance();
        assertEquals("A", renamed(row, "io.rowweave.Row", "io.rowweave.DataRow", "get", int.class, 0));
        assertEquals(new BigDecimal("100"),
                renamed(row, "io.rowweave.Row", "io.rowweave.DataRow", "get", int.class, 1));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(row);
            output.writeObject(schemaInstance());
        }
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            Object restoredRow = input.readObject();
            Object restoredSchema = input.readObject();
            assertEquals(row.getClass(), restoredRow.getClass());
            assertEquals(schemaInstance().getClass(), restoredSchema.getClass());
            assertEquals("A", restoredRow.getClass().getMethod("get", int.class)
                    .invoke(restoredRow, 0));
            assertEquals(new BigDecimal("100"), restoredRow.getClass()
                    .getMethod("get", int.class).invoke(restoredRow, 1));
        }
    }

    private static Object renamed(Object value, String oldName, String newName,
                                  String method, Class<?> parameter, Object argument) throws Exception {
        assertEquals(renamed(oldName, newName), value.getClass());
        return value.getClass().getMethod(method, parameter).invoke(value, argument);
    }

    @Test
    void portableSchemaFingerprintKeepsCanonicalV1Identity() throws Exception {
        List<PortableField> fields = Collections.unmodifiableList(Arrays.asList(
                new PortableField("adaptor", "adaptor", "adaptor",
                        PortableDataType.STRING, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)));
        PortableRowSchema schema = new PortableRowSchema(fields);
        PortableRowSchema duplicate = new PortableRowSchema(fields);

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update("RWXXSC01".getBytes(StandardCharsets.US_ASCII));
        digest.update(writeInt(1));
        digest.update(writeInt(fields.size()));
        for (PortableField field : fields) {
            writePortableText(digest, field.fieldId());
            writePortableText(digest, field.name());
            writePortableText(digest, field.alias());
            digest.update(writeInt(field.dataType().wireId()));
            digest.update(new byte[] {field.nullable() ? (byte) 1 : (byte) 0});
        }
        String expected = hex(digest.digest());

        assertEquals(expected, schema.fingerprint());
        assertEquals(schema.fingerprint(), duplicate.fingerprint());
    }

    private static byte[] writeInt(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16),
            (byte) (value >>> 8), (byte) value,
        };
    }

    private static void writePortableText(MessageDigest digest, String value) {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        digest.update(writeInt(encoded.length));
        digest.update(encoded);
    }

    @Test
    void compiledExpressionEnginePreservesSemanticsAcrossDescriptors() throws Exception {
        Class<?> parser = Class.forName("io.rowweave.expr.ExprParser");
        Class<?> engineFactory = Class.forName("io.rowweave.expr.ExprEngineFactory");
        Object expression = parser.getMethod("parse", String.class)
                .invoke(null, "amount >= 60");
        Object engine = engineFactory.getMethod("getDefault").invoke(null);
        assertNotNull(engine);
        assertEquals(Class.forName("io.rowweave.expr.CompiledExprEngine"), engine.getClass());

        Class<?> schemaType = renamed("io.rowweave.Row\u004deta", "io.rowweave.DataSchema");
        Object schema = schemaInstance();
        Method compile = null;
        for (Method candidate : engine.getClass().getMethods()) {
            if (!candidate.getName().equals("compile") || candidate.getParameterCount() != 2) continue;
            Class<?>[] types = candidate.getParameterTypes();
            if (types[0].isAssignableFrom(schemaType)
                    && types[1].isAssignableFrom(expression.getClass())) {
                compile = candidate;
                break;
            }
        }
        assertNotNull(compile, "compiled expression engine compile(Row\u004deta/DataSchema, Expr) is absent");
        Object compiled = compile.invoke(engine, schema, expression);

        Method test = compiled.getClass().getMethod("test", renamed(
                "io.rowweave.Row", "io.rowweave.DataRow"));
        assertTrue((Boolean) test.invoke(compiled, rowWithAmount("100")));
        assertFalse((Boolean) test.invoke(compiled, rowWithAmount("59.9999999999")));
        assertFalse((Boolean) test.invoke(compiled, rowWithAmount(null)));
    }

    private static Object rowWithAmount(String amount) throws Exception {
        Class<?> rowType = renamed("io.rowweave.Row", "io.rowweave.DataRow");
        Constructor<?> constructor = rowType.getConstructor(int.class);
        Object row = constructor.newInstance(2);
        rowType.getMethod("set", int.class, Object.class).invoke(row, 0, "A");
        rowType.getMethod("set", int.class, Object.class).invoke(row, 1,
                amount == null ? null : new BigDecimal(amount));
        return row;
    }
}
