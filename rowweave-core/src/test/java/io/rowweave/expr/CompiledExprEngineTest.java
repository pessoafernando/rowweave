package io.rowweave.expr;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CompiledExprEngineTest {

    private static final DataSchema META = new DataSchema(
            new Field("a", DataType.IntegerType),
            new Field("b", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType),
            new Field("name", DataType.StringType));

    private static DataRow row(Integer a, Integer b, BigDecimal amt, String name) {
        DataRow r = new DataRow(4);
        r.set(0, a); r.set(1, b); r.set(2, amt); r.set(3, name);
        return r;
    }

    private static final ExprEngine INTERP =
            ExprEngineFactory.create(ExprEngineFactory.EngineType.INTERPRETED);
    private static final ExprEngine COMPILED =
            ExprEngineFactory.create(ExprEngineFactory.EngineType.COMPILED);

    private static List<DataRow> sampleRows() {
        return Arrays.asList(
                row(10, 3, new BigDecimal("5.5"), "hello"),
                row(1, 3, null, "world"),
                row(null, 0, new BigDecimal("100.00"), null),
                row(-5, -5, new BigDecimal("0"), "x"),
                row(0, 7, new BigDecimal("12.3456789012"), "abc"));
    }

    /** Core case: the same expression on the same input gives bit-identical compiled and interpreted results. */
    private void assertBitEqual(String expr) {
        CompiledExpr ci = INTERP.compile(META, expr);
        CompiledExpr cc = COMPILED.compile(META, expr);
        for (DataRow r : sampleRows()) {
            Object vi = ci.eval(r);
            Object vc = cc.eval(r);
            assertEquals(vi, vc, "eval mismatch for [" + expr + "] on row " + render(r)
                    + ": interp=" + vi + " compiled=" + vc);
            assertEquals(ci.test(r), cc.test(r), "test mismatch for [" + expr + "] on row " + render(r));
        }
    }

    private static String render(DataRow r) {
        return "(" + r.get(0) + "," + r.get(1) + "," + r.get(2) + "," + r.get(3) + ")";
    }

    @Test
    void arithmeticBitEqual() {
        assertBitEqual("a + b");
        assertBitEqual("a - b");
        assertBitEqual("a * b");
        assertBitEqual("amt + 1");
        assertBitEqual("amt * a");
    }

    @Test
    void comparisonBitEqual() {
        assertBitEqual("a > b");
        assertBitEqual("a = b");
        assertBitEqual("a <> b");
        assertBitEqual("amt > 5");  // null under three-valued logic
        assertBitEqual("amt = amt");
    }

    @Test
    void logicalBitEqual() {
        assertBitEqual("a > b and a > 0");
        assertBitEqual("a < b or b > 0");
        assertBitEqual("not (a > b)");
        assertBitEqual("amt > 5 and a < 0");  // includes null
    }

    @Test
    void isNullAndInAndLikeBitEqual() {
        assertBitEqual("a is null");
        assertBitEqual("amt is not null");
        assertBitEqual("b in (0, 3, 7)");
        assertBitEqual("name like 'h%'");
    }

    @Test
    void caseWhenBitEqual() {
        assertBitEqual("case when a > b then 1 else 0 end");
        assertBitEqual("case when amt > 10 then amt else 0 end");
    }

    @Test
    void compiledReusableAcrossRows() {
        CompiledExpr cc = COMPILED.compile(META, "a + b");
        assertEquals(new BigDecimal("5"), cc.eval(row(2, 3, null, null)));
        assertEquals(new BigDecimal("11"), cc.eval(row(7, 4, null, null)));
    }

    @Test
    void namedParamsGracefullyDegradeToInterpreter() {
        // the JIT does not support named parameters yet, so CompiledExprEngine falls back to the interpreter and the result stays correct
        java.util.Map<String, Object> params = new java.util.HashMap<>();
        params.put("threshold", new BigDecimal("5"));
        CompiledExpr cc = COMPILED.compile(META, "amt > threshold", params);
        DataRow r = row(1, 1, new BigDecimal("10"), "x");
        assertTrue(cc.test(r)); // 10 > 5
    }

    @Test
    void factoryDefaultIsCompiledAndDowngradeable() {
        assertTrue(ExprEngineFactory.getDefault() instanceof CompiledExprEngine);
        String prev = System.getProperty(ExprEngineFactory.CODEGEN_PROPERTY);
        try {
            System.setProperty(ExprEngineFactory.CODEGEN_PROPERTY, "false");
            assertTrue(ExprEngineFactory.getDefault() instanceof InterpretedExprEngine);
        } finally {
            if (prev == null) {
                System.clearProperty(ExprEngineFactory.CODEGEN_PROPERTY);
            } else {
                System.setProperty(ExprEngineFactory.CODEGEN_PROPERTY, prev);
            }
        }
    }
}
