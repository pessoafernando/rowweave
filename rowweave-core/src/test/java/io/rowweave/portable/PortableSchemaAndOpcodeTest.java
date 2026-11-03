package io.rowweave.portable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortableSchemaAndOpcodeTest {

    @Test
    void schemaFingerprintBindsFieldIdentityOrderAliasTypeAndNullability() {
        PortableRowSchema first = schema(
                field("customer-id", "customer_id", "customerId",
                        PortableDataType.STRING, false),
                field("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true));
        PortableRowSchema same = schema(
                field("customer-id", "customer_id", "customerId",
                        PortableDataType.STRING, false),
                field("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true));

        assertEquals(first, same);
        assertEquals(first.fingerprint(), same.fingerprint());
        assertEquals("e1be26eb784cf606344bbf714ee6ced69a409cc55cb5985cd8d197a952979c64",
                first.fingerprint());
        assertNotEquals(first.fingerprint(), schema(
                field("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true),
                field("customer-id", "customer_id", "customerId",
                        PortableDataType.STRING, false)).fingerprint());
        assertNotEquals(first.fingerprint(), schema(
                field("customer-id", "customer_id", "customerId",
                        PortableDataType.STRING, true),
                field("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)).fingerprint());
    }

    @Test
    void schemaRejectsDuplicateOrUnboundedFieldsWithStableCodes() {
        PortableField duplicate = field("id", "id", "id",
                PortableDataType.LONG, false);
        assertCode("DUPLICATE_FIELD_ID",
                () -> schema(duplicate, duplicate));
        assertCode("INVALID_FIELD",
                () -> field("id", "", "id", PortableDataType.LONG, false));
        assertCode("LIMIT_EXCEEDED",
                () -> field(repeat('x', 257), "id", "id",
                        PortableDataType.LONG, false));
    }

    @Test
    void dataTypesAndOperatorCodesHaveStableNumericIdsNotOrdinals() {
        assertEquals(1, PortableDataType.BOOLEAN.wireId());
        assertEquals(5, PortableDataType.BIG_DECIMAL.wireId());
        assertEquals(8, PortableDataType.NULL.wireId());
        assertEquals(PortableDataType.TIMESTAMP, PortableDataType.fromWireId(7));

        assertEquals(1, PortableOperatorCode.SOURCE.wireId());
        assertEquals(2, PortableOperatorCode.SINK.wireId());
        assertEquals(30, PortableOperatorCode.GROUP_SUM.wireId());
        assertEquals(51, PortableOperatorCode.JOIN.wireId());
        assertEquals(61, PortableOperatorCode.FUNCTION_MAP.wireId());
        assertEquals(PortableOperatorCode.BindingKind.FUNCTION,
                PortableOperatorCode.FUNCTION_MAP.bindingKind());
        assertEquals(2, PortableOperatorCode.JOIN.inputCount());
        assertEquals(PortableOperatorCode.BindingKind.NONE,
                PortableOperatorCode.JOIN.bindingKind());

        Set<Integer> typeIds = new HashSet<>();
        for (PortableDataType value : PortableDataType.values()) {
            assertTrue(typeIds.add(value.wireId()));
        }
        Set<Integer> operatorIds = new HashSet<>();
        for (PortableOperatorCode value : PortableOperatorCode.values()) {
            assertTrue(operatorIds.add(value.wireId()), value.name());
        }
    }

    @Test
    void unknownNumericIdsFailClosedWithNoOrdinalFallback() {
        assertCode("UNKNOWN_DATA_TYPE",
                () -> PortableDataType.fromWireId(0));
        assertCode("UNKNOWN_OPCODE",
                () -> PortableOperatorCode.fromWireId(3));
        assertCode("UNKNOWN_OPCODE",
                () -> PortableOperatorCode.fromWireId(999));
    }

    private static PortableRowSchema schema(PortableField... fields) {
        return new PortableRowSchema(Arrays.asList(fields));
    }

    private static PortableField field(
            String fieldId, String name, String alias,
            PortableDataType type, boolean nullable) {
        return new PortableField(fieldId, name, alias, type, nullable);
    }

    private static void assertCode(String code, Runnable action) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
