package io.rowweave.expr;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class ExprInterpreterTest {

    private static final DataSchema META = new DataSchema(
            new Field("a", DataType.IntegerType),
            new Field("b", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType),
            new Field("name", DataType.StringType));

    private static DataRow row(Integer a, Integer b, BigDecimal amt, String name) {
        DataRow r = new DataRow(4);
        r.set(0, a);
        r.set(1, b);
        r.set(2, amt);
        r.set(3, name);
        return r;
    }

    private static Object eval(String expr, DataRow row) {
        return ExprEngineFactory.getDefault().compile(META, expr).eval(row);
    }

    private static boolean test(String expr, DataRow row) {
        return ExprEngineFactory.getDefault().compile(META, expr).test(row);
    }

    @Test
    void interpretedEngineExplicit() {
        // the interpreter tested directly: the default engine starts as COMPILED, so INTERPRETED is taken explicitly here
        ExprEngine eng = ExprEngineFactory.create(ExprEngineFactory.EngineType.INTERPRETED);
        assertTrue(eng instanceof InterpretedExprEngine);
        DataRow r = row(10, 3, null, "x");
        assertEquals(new BigDecimal("13"), eng.compile(META, "a + b").eval(r));
    }

    @Test
    void arithmetic() {
        DataRow r = row(10, 3, new BigDecimal("5.5"), "x");
        assertEquals(new BigDecimal("13"), eval("a + b", r));
        assertEquals(new BigDecimal("7"), eval("a - b", r));
        assertEquals(new BigDecimal("30"), eval("a * b", r));
    }

    @Test
    void arithmeticWithNullYieldsNull() {
        DataRow r = row(null, 3, null, null);
        assertNull(eval("a + b", r));   // null + 3 = null (SQL)
        assertNull(eval("amt * b", r)); // null * 3 = null
    }

    @Test
    void comparisonAndNullThreeValued() {
        DataRow r = row(10, 3, null, "x");
        assertTrue(test("a > b", r));
        assertFalse(test("a < b", r));
        // comparison with null -> unknown -> test() false (filter semantics)
        assertFalse(test("amt > 5", r));   // null > 5 = unknown -> false
        assertFalse(test("amt = amt", r)); // null = null = unknown -> false (NOT true)
    }

    @Test
    void logicalThreeValued() {
        DataRow r = row(10, 3, null, "x");
        assertTrue(test("a > b and a > 0", r));
        assertFalse(test("a > b and a < 0", r));
        assertTrue(test("a < b or a > 0", r));
        // null AND false = false; null AND true = unknown->false in test
        assertFalse(test("amt > 5 and a < 0", r)); // unknown and false = false
    }

    @Test
    void isNullIsNotNull() {
        DataRow r = row(10, null, null, "x");
        assertTrue(test("b is null", r));
        assertFalse(test("a is null", r));
        assertTrue(test("a is not null", r));
    }

    @Test
    void inList() {
        // ExprParser supports no BETWEEN ... AND ... syntax, so only IN is exercised here.
        DataRow r = row(5, 3, null, "x");
        assertTrue(test("a in (1, 3, 5)", r));
        assertFalse(test("a in (1, 2)", r));
    }

    @Test
    void likeString() {
        DataRow r = row(1, 1, null, "hello");
        assertTrue(test("name like 'hel%'", r));
        assertTrue(test("name like '%llo'", r));
        assertFalse(test("name like 'xyz%'", r));
    }

    @Test
    void caseWhen() {
        DataRow r = row(10, 3, null, "x");
        assertEquals(new BigDecimal("1"), eval("case when a > b then 1 else 0 end", r));
        DataRow r2 = row(1, 3, null, "x");
        assertEquals(new BigDecimal("0"), eval("case when a > b then 1 else 0 end", r2));
    }

    @Test
    void parseErrorThrows() {
        assertThrows(ExprParseException.class, () -> ExprEngineFactory.getDefault().compile(META, "a + + "));
    }

    @Test
    void compiledExprReusableAcrossRows() {
        CompiledExpr ce = ExprEngineFactory.getDefault().compile(META, "a + b");
        assertEquals(new BigDecimal("5"), ce.eval(row(2, 3, null, null)));
        assertEquals(new BigDecimal("11"), ce.eval(row(7, 4, null, null))); // reuse
    }
}
