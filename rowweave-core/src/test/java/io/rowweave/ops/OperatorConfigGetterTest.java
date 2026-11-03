package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Operator-config getters round-trip: each returned value matches its constructor argument,
 * including the reverse lookup from index to field name, so DataSet can extract describable
 * PlanNode params. This is a hard precondition for the describable refactor.
 */
class OperatorConfigGetterTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("grp", DataType.StringType),
            new Field("amt", DataType.BigDecimalType));

    /** groupBy: round-trips the key field names, the single-aggregate type id and the measure name. */
    @Test
    void groupByConfigExtraction() {
        ParallelGroupByAgg sum = ParallelGroupByAgg.of(META, "grp").sum("amt");
        assertEquals(Arrays.asList("grp"), sum.keyFieldNames());
        assertEquals(1, sum.aggCount());
        assertEquals("groupBySum", sum.singleAggTypeId());
        assertEquals("amt", sum.singleAggMeasureField());

        ParallelGroupByAgg cnt = ParallelGroupByAgg.of(META, "grp", "id").count();
        assertEquals(Arrays.asList("grp", "id"), cnt.keyFieldNames());
        assertEquals("groupByCount", cnt.singleAggTypeId());
        assertNull(cnt.singleAggMeasureField(), "count(*) measure = null");

        ParallelGroupByAgg max = ParallelGroupByAgg.of(META, "grp").max("amt");
        assertEquals("groupByMax", max.singleAggTypeId());
        assertEquals("amt", max.singleAggMeasureField());

        ParallelGroupByAgg min = ParallelGroupByAgg.of(META, "id").min("amt");
        assertEquals("groupByMin", min.singleAggTypeId());
        assertEquals(Arrays.asList("id"), min.keyFieldNames());
    }

    /** groupBy with several aggregates: the single-aggregate describable extraction is refused by design. */
    @Test
    void groupByMultiAggRejected() {
        ParallelGroupByAgg multi = ParallelGroupByAgg.of(META, "grp").sum("amt").count();
        assertEquals(2, multi.aggCount());
        assertThrows(IllegalStateException.class, multi::singleAggTypeId);
    }

    /** orderBy: round-trips sortFields, excluding the __seq field that applyStream adds internally. */
    @Test
    void orderByConfigExtraction() {
        SortField sf0 = new SortField(2, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST);
        SortField sf1 = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_FIRST);
        OrderBy ob = OrderBy.of(META, sf0, sf1);
        SortField[] got = ob.sortFields();
        assertEquals(2, got.length);
        assertEquals(2, got[0].getFieldIndex());
        assertEquals(SortField.Direction.DESC, got[0].getDirection());
        assertEquals(SortField.NullOrder.NULLS_LAST, got[0].getNullOrder());
        assertEquals(0, got[1].getFieldIndex());
        assertEquals(SortField.NullOrder.NULLS_FIRST, got[1].getNullOrder());
        // round-trip through PlanOps.encodeSortField
        assertEquals("2:DESC:NULLS_LAST", io.rowweave.plan.PlanOps.encodeSortField(got[0]));
    }

    /** top: round-trips n and sortFields. */
    @Test
    void topConfigExtraction() {
        SortField sf = new SortField(2, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST);
        Top top = Top.of(META, 5, sf);
        assertEquals(5, top.n());
        assertEquals(1, top.sortFields().length);
        assertEquals(2, top.sortFields()[0].getFieldIndex());
    }

    /** distinct: separates allColumns from byColumns and round-trips the key field names. */
    @Test
    void distinctConfigExtraction() {
        Distinct all = Distinct.allColumns(META);
        assertTrue(all.isAllColumns());
        assertEquals(Arrays.asList("id", "grp", "amt"), all.keyFieldNames());

        Distinct by = Distinct.byColumns(META, "grp", "amt");
        assertFalse(by.isAllColumns());
        assertEquals(Arrays.asList("grp", "amt"), by.keyFieldNames());
    }

    /** SMJ: round-trips joinType and the left and right key field names resolved from indexes. */
    @Test
    void sortMergeJoinConfigExtraction() {
        DataSchema lm = new DataSchema(new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
        SortMergeJoin smj = SortMergeJoin.of(lm, rm, new String[]{"lk"}, new String[]{"rk"}, JoinType.LEFT);
        assertEquals(JoinType.LEFT, smj.joinType());
        assertEquals(Arrays.asList("lk"), smj.leftKeyFieldNames());
        assertEquals(Arrays.asList("rk"), smj.rightKeyFieldNames());
    }

    /** SMJ with several keys keeps their order. */
    @Test
    void sortMergeJoinMultiKey() {
        DataSchema lm = new DataSchema(new Field("a", DataType.IntegerType), new Field("b", DataType.StringType));
        DataSchema rm = new DataSchema(new Field("c", DataType.IntegerType), new Field("d", DataType.StringType));
        SortMergeJoin smj = SortMergeJoin.of(lm, rm, new String[]{"a", "b"}, new String[]{"c", "d"}, JoinType.INNER);
        assertEquals(Arrays.asList("a", "b"), smj.leftKeyFieldNames());
        assertEquals(Arrays.asList("c", "d"), smj.rightKeyFieldNames());
    }
}
