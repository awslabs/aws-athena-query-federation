/*-
 * #%L
 * athena-tpcds
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.tpcds;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link TPCDSSubstraitFilter} row evaluation, built against the real TPC-DS
 * {@code income_band} table (columns: ib_income_band_sk BIGINT, ib_lower_bound INT, ib_upper_bound INT).
 */
public class TPCDSSubstraitFilterTest
{
    private static final Table INCOME_BAND = TPCDSUtils.validateTable(
            new com.amazonaws.athena.connector.lambda.domain.TableName("tpcds1", "income_band"));

    private static final String SK = "ib_income_band_sk";
    private static final String LOWER = "ib_lower_bound";
    private static final String UPPER = "ib_upper_bound";

    // income_band row layout by column position: [ib_income_band_sk, ib_lower_bound, ib_upper_bound]
    private static List<String> row(String sk, String lower, String upper)
    {
        return Arrays.asList(sk, lower, upper);
    }

    private static LogicalExpression leaf(String col, SubstraitOperator op, Object value)
    {
        return new LogicalExpression(new ColumnPredicate(col, op, value, null));
    }

    private static boolean matches(LogicalExpression expr, List<String> row)
    {
        return TPCDSSubstraitFilter.forExpression(expr, INCOME_BAND).matches(row);
    }

    @Test
    public void from_WhenNoQueryPlan_ReturnsNull()
    {
        Constraints noPlan = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        assertNull(TPCDSSubstraitFilter.from(noPlan, INCOME_BAND));
    }

    @Test
    public void equalOnBigintColumn()
    {
        LogicalExpression expr = leaf(SK, SubstraitOperator.EQUAL, 7L);
        assertTrue(matches(expr, row("7", "0", "10000")));
        assertFalse(matches(expr, row("1", "0", "10000")));
    }

    @Test
    public void notEqualOnBigintColumn()
    {
        LogicalExpression expr = leaf(SK, SubstraitOperator.NOT_EQUAL, 7L);
        assertFalse(matches(expr, row("7", "0", "10000")));
        assertTrue(matches(expr, row("8", "0", "10000")));
    }

    @Test
    public void greaterThanExcludesZeroLowerBound()
    {
        LogicalExpression expr = leaf(LOWER, SubstraitOperator.GREATER_THAN, 0L);
        assertFalse(matches(expr, row("1", "0", "10000")));
        assertTrue(matches(expr, row("2", "10001", "20000")));
    }

    @Test
    public void rangeComparisonsAreNumeric()
    {
        assertTrue(matches(leaf(UPPER, SubstraitOperator.GREATER_THAN_OR_EQUAL_TO, 10000L), row("1", "0", "10000")));
        assertTrue(matches(leaf(UPPER, SubstraitOperator.LESS_THAN_OR_EQUAL_TO, 10000L), row("1", "0", "10000")));
        assertTrue(matches(leaf(UPPER, SubstraitOperator.LESS_THAN, 20000L), row("1", "0", "10000")));
        assertFalse(matches(leaf(UPPER, SubstraitOperator.LESS_THAN, 10000L), row("1", "0", "10000")));
    }

    @Test
    public void isNullAndIsNotNull()
    {
        assertTrue(matches(leaf(LOWER, SubstraitOperator.IS_NOT_NULL, null), row("1", "0", "10000")));
        assertFalse(matches(leaf(LOWER, SubstraitOperator.IS_NULL, null), row("1", "0", "10000")));
        assertTrue(matches(leaf(LOWER, SubstraitOperator.IS_NULL, null), row("1", null, "10000")));
        assertFalse(matches(leaf(LOWER, SubstraitOperator.IS_NOT_NULL, null), row("1", null, "10000")));
    }

    @Test
    public void nullValueFailsComparison()
    {
        assertFalse(matches(leaf(LOWER, SubstraitOperator.GREATER_THAN, 0L), row("1", null, "10000")));
    }

    @Test
    public void andRequiresAllChildren()
    {
        LogicalExpression and = new LogicalExpression(SubstraitOperator.AND, Arrays.asList(
                leaf(LOWER, SubstraitOperator.GREATER_THAN, 0L),
                leaf(UPPER, SubstraitOperator.IS_NOT_NULL, null)));
        assertTrue(matches(and, row("2", "10001", "20000")));
        assertFalse(matches(and, row("1", "0", "10000")));
    }

    @Test
    public void orModelsInList()
    {
        // ib_income_band_sk IN (1,2,3) is rendered as OR(=1, =2, =3)
        LogicalExpression in = new LogicalExpression(SubstraitOperator.OR, Arrays.asList(
                leaf(SK, SubstraitOperator.EQUAL, 1L),
                leaf(SK, SubstraitOperator.EQUAL, 2L),
                leaf(SK, SubstraitOperator.EQUAL, 3L)));
        assertTrue(matches(in, row("2", "10001", "20000")));
        assertFalse(matches(in, row("5", "40001", "50000")));
    }

    @Test
    public void notInExcludesListedValues()
    {
        ColumnPredicate notIn = new ColumnPredicate(SK, SubstraitOperator.NOT_IN,
                Arrays.<Object>asList(1L, 2L, 3L), null);
        assertFalse(matches(new LogicalExpression(notIn), row("2", "10001", "20000")));
        assertTrue(matches(new LogicalExpression(notIn), row("5", "40001", "50000")));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unknownColumnThrows()
    {
        matches(leaf("does_not_exist", SubstraitOperator.EQUAL, 1L), row("1", "0", "10000"));
    }

    @Test
    public void validateSchemaLayoutAssumption()
    {
        // Guard against the income_band column order the tests depend on.
        assertEquals(SK, INCOME_BAND.getColumns()[0].getName());
        assertEquals(LOWER, INCOME_BAND.getColumns()[1].getName());
        assertEquals(UPPER, INCOME_BAND.getColumns()[2].getName());
    }

    @Test
    public void from_NullConstraints_ReturnsNull()
    {
        assertNull(TPCDSSubstraitFilter.from(null, INCOME_BAND));
    }

    @Test
    public void from_EmptySubstraitPlan_ReturnsNull()
    {
        Constraints emptyPlan = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                DEFAULT_NO_LIMIT, Collections.emptyMap(), new QueryPlan("0", ""));
        assertNull(TPCDSSubstraitFilter.from(emptyPlan, INCOME_BAND));
    }

    @Test
    public void from_SubstraitPlanWithFilter_BuildsFilter()
    {
        // A captured Athena-generated Substrait plan for a "col_5 >= ? AND col_5 <= ?" predicate.
        // This drives from() through plan deserialization and predicate parsing; the plan's own column
        // names are irrelevant to this assertion because from() only parses the plan (per-row evaluation
        // is covered by the tests above).
        String planBase64 = "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb2"
                + "4ueWFtbBIOGgwIARoIYW5kOmJvb2wSExoRCAIQARoLZ3RlOmFueV9hbnkSExoRCAIQAhoLbHRlOmFueV9hbnkalwQSlAQKywM6yAMK"
                + "DhIMCgoKCwwNDg8QERITEr8CErwCCgIKABLGAQrDAQoCCgASrgEKBWNvbF8wCgVjb2xfMQoFY29sXzIKBWNvbF8zCgVjb2xfNAoFY2"
                + "9sXzUKBWNvbF82CgVjb2xfNwoFY29sXzgKBWNvbF85EmYKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBCgi"
                + "yAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBGAE6DAoKVEVTVF9UQUJMRRpt"
                + "GmsaBAoCEAEiMBouGiwIARoECgIQASIYGhZaFAoEKgIQARIKEggKBBICCAUiABgCIggaBgoEKMDEByIxGi8aLQgCGgQKAhABIhga"
                + "FloUCgQqAhABEgoSCAoEEgIIBSIAGAIiCRoHCgUom4zbKRoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQS"
                + "AggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJ"
                + "IgASBWNvbF8wEgVjb2xfMRIFY29sXzISBWNvbF8zEgVjb2xfNBIFY29sXzUSBWNvbF82EgVjb2xfNxIFY29sXzgSBWNvbF85";
        Constraints withPlan = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                DEFAULT_NO_LIMIT, Collections.emptyMap(), new QueryPlan("", planBase64));
        assertNotNull(TPCDSSubstraitFilter.from(withPlan, INCOME_BAND));
    }

    @Test
    public void from_SubstraitPlanWithoutFilter_ReturnsNull()
    {
        // A captured plan for "SELECT * FROM test_table LIMIT 100" — no WHERE predicate. LIMIT is applied
        // by the engine, so the connector has no filter to apply and from() returns null.
        String planBase64 = "GqwDEqkDCuACGt0CCgIKABLSAjrPAgoOEgwKCgoLDA0ODxAREhMSxgEKwwEKAgoAEq4BCgVjb2x"
                + "fMAoFY29sXzEKBWNvbF8yCgVjb2xfMwoFY29sXzQKBWNvbF81CgVjb2xfNgoFY29sXzcKBWNvbF84CgVjb2xfORJmCgiyAQUI6AcYA"
                + "QoIsgEFCOgHGAEKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIB"
                + "BQjoBxgBCgiyAQUI6AcYARgBOgwKClRFU1RfVEFCTEUaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIA"
                + "yIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGA"
                + "AgZBIFY29sXzASBWNvbF8xEgVjb2xfMhIFY29sXzMSBWNvbF80EgVjb2xfNRIFY29sXzYSBWNvbF83EgVjb2xfOBIFY29sXzk=";
        Constraints limitOnly = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                DEFAULT_NO_LIMIT, Collections.emptyMap(), new QueryPlan("", planBase64));
        assertNull(TPCDSSubstraitFilter.from(limitOnly, INCOME_BAND));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void evaluate_NonAndOrLogicalOperator_Throws()
    {
        LogicalExpression bad = new LogicalExpression(SubstraitOperator.NOT,
                Arrays.asList(leaf(SK, SubstraitOperator.EQUAL, 1L)));
        matches(bad, row("1", "0", "10000"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void evaluateLeaf_UnsupportedComparisonOperator_Throws()
    {
        matches(leaf(SK, SubstraitOperator.NOT, 1L), row("1", "0", "10000"));
    }

    @Test
    public void notIn_NullRaw_ReturnsFalse()
    {
        ColumnPredicate notIn = new ColumnPredicate(LOWER, SubstraitOperator.NOT_IN,
                Arrays.<Object>asList(1L, 2L), null);
        assertFalse(matches(new LogicalExpression(notIn), row("1", null, "10000")));
    }

    @Test
    public void nand_NegatesConjunctionOfChildren()
    {
        // NAND(children) is true unless every child is true.
        List<ColumnPredicate> children = Arrays.asList(
                new ColumnPredicate(LOWER, SubstraitOperator.GREATER_THAN, 0L, null),
                new ColumnPredicate(UPPER, SubstraitOperator.IS_NOT_NULL, null, null));
        LogicalExpression nand = new LogicalExpression(
                new ColumnPredicate(SK, SubstraitOperator.NAND, children, null));
        assertFalse(matches(nand, row("2", "10001", "20000")));
        assertTrue(matches(nand, row("1", "0", "10000")));
    }

    @Test
    public void nor_NegatesDisjunctionOfChildren()
    {
        // NOR(children) is true only when no child is true.
        List<ColumnPredicate> children = Arrays.asList(
                new ColumnPredicate(SK, SubstraitOperator.EQUAL, 1L, null),
                new ColumnPredicate(SK, SubstraitOperator.EQUAL, 2L, null));
        LogicalExpression nor = new LogicalExpression(
                new ColumnPredicate(SK, SubstraitOperator.NOR, children, null));
        assertTrue(matches(nor, row("5", "40001", "50000")));
        assertFalse(matches(nor, row("1", "0", "10000")));
    }

    @Test
    public void dateColumnComparesChronologically()
    {
        Object[] found = firstColumnOfBase(ColumnType.Base.DATE);
        assertNotNull("expected a TPC-DS table with a DATE column", found);
        Table table = (Table) found[0];
        Column col = (Column) found[1];
        int pos = col.getPosition();

        // Literal supplied as ISO text.
        LogicalExpression gtText = leaf(col.getName(), SubstraitOperator.GREATER_THAN, "2000-01-01");
        assertTrue(matchesOn(table, gtText, rowFor(table, pos, "2000-06-15")));
        assertFalse(matchesOn(table, gtText, rowFor(table, pos, "1999-12-31")));

        // Literal supplied as a Substrait epoch-day number.
        long epochDay = LocalDate.parse("2000-01-01").toEpochDay();
        LogicalExpression gtEpoch = leaf(col.getName(), SubstraitOperator.GREATER_THAN, epochDay);
        assertTrue(matchesOn(table, gtEpoch, rowFor(table, pos, "2000-06-15")));
        assertFalse(matchesOn(table, gtEpoch, rowFor(table, pos, "1999-12-31")));
    }

    @Test
    public void characterColumnComparesLexicographically()
    {
        Object[] found = firstColumnOfBase(ColumnType.Base.CHAR, ColumnType.Base.VARCHAR);
        assertNotNull("expected a TPC-DS table with a CHAR/VARCHAR column", found);
        Table table = (Table) found[0];
        Column col = (Column) found[1];
        int pos = col.getPosition();

        LogicalExpression equal = leaf(col.getName(), SubstraitOperator.EQUAL, "MMM");
        assertTrue(matchesOn(table, equal, rowFor(table, pos, "MMM")));
        assertFalse(matchesOn(table, equal, rowFor(table, pos, "NNN")));

        LogicalExpression greater = leaf(col.getName(), SubstraitOperator.GREATER_THAN, "MMM");
        assertTrue(matchesOn(table, greater, rowFor(table, pos, "NNN")));
        assertFalse(matchesOn(table, greater, rowFor(table, pos, "AAA")));
    }

    private static Object[] firstColumnOfBase(ColumnType.Base... bases)
    {
        for (ColumnType.Base base : bases) {
            for (Table table : Table.getBaseTables()) {
                for (Column column : table.getColumns()) {
                    if (column.getType().getBase() == base) {
                        return new Object[] {table, column};
                    }
                }
            }
        }
        return null;
    }

    private static List<String> rowFor(Table table, int position, String value)
    {
        List<String> cells = new ArrayList<>(Collections.nCopies(table.getColumns().length, (String) null));
        cells.set(position, value);
        return cells;
    }

    private static boolean matchesOn(Table table, LogicalExpression expr, List<String> row)
    {
        return TPCDSSubstraitFilter.forExpression(expr, table).matches(row);
    }
}
