/*-
 * #%L
 * athena-db2-as400
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.db2as400;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Db2As400QueryStringBuilderTest {
    private static final String QUOTE_CHARACTER = "\"";

    @Mock
    Split split;

    @Test
    public void testQueryBuilder()
    {
        Split split = Mockito.mock(Split.class);
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder(QUOTE_CHARACTER);
        Assert.assertEquals(" FROM \"default\".table ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM \"default\".schema.table ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder(QUOTE_CHARACTER);
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(Mockito.eq("partition_number"))).thenReturn("0");
        Mockito.when(split.getProperty(Mockito.eq("PARTITIONING_COLUMN"))).thenReturn("PC");
        Assert.assertEquals(List.of(" DATAPARTITIONNUM(\"PC\") = 0"), builder.getPartitionWhereClauses(split));
    }

    @Test
    public void getPartitionWhereClauses_partitioningColumnWithSpecialCharacters_returnsQuotedIdentifier()
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder(QUOTE_CHARACTER);
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(Mockito.eq("partition_number"))).thenReturn("0");
        Mockito.when(split.getProperty(Mockito.eq("PARTITIONING_COLUMN"))).thenReturn("\"X\") = 0 OR 1=1 --");
        Assert.assertEquals(
                List.of(" DATAPARTITIONNUM(\"\"\"X\"\") = 0 OR 1=1 --\") = 0"),
                builder.getPartitionWhereClauses(split));
    }

    @Test
    public void getSqlDialect_returnsDb2Dialect()
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder("\"");
        SqlDialect dialect = builder.getSqlDialect();
        Assert.assertTrue(dialect instanceof Db2SqlDialect);
    }

    @Test
    public void getSqlDialectWithCasingFilter_returnsDb2Dialect()
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder("\"");
        SqlDialect dialect = builder.getSqlDialect(true);
        Assert.assertTrue(dialect instanceof Db2As400Dialect);
    }

    @Test
    public void extractOrderByClause_emulatesNullsWithoutNullsKeyword()
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder("\"");
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getOrderByClause()).thenReturn(Arrays.asList(
                new OrderByField("ID", OrderByField.Direction.DESC_NULLS_LAST),
                new OrderByField("NAME", OrderByField.Direction.ASC_NULLS_FIRST)));
        String orderBy = builder.extractOrderByClause(constraints);
        // Db2 for i rejects the NULLS keyword (SQL0199); it must not appear in the generated clause.
        Assert.assertFalse(orderBy.toUpperCase().contains("NULLS"));
        Assert.assertEquals(
                "ORDER BY CASE WHEN \"ID\" IS NULL THEN 1 ELSE 0 END, \"ID\" DESC, "
                        + "CASE WHEN \"NAME\" IS NULL THEN 0 ELSE 1 END, \"NAME\" ASC",
                orderBy);
    }

    @Test
    public void extractOrderByClause_emptyWhenNoOrderBy()
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder("\"");
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getOrderByClause()).thenReturn(Collections.emptyList());
        Assert.assertEquals("", builder.extractOrderByClause(constraints));
    }

    @Test
    public void extractOrderByClause_coversRemainingNullDirections()
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder("\"");
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getOrderByClause()).thenReturn(Arrays.asList(
                new OrderByField("A", OrderByField.Direction.ASC_NULLS_LAST),
                new OrderByField("B", OrderByField.Direction.DESC_NULLS_FIRST)));
        String orderBy = builder.extractOrderByClause(constraints);
        Assert.assertFalse(orderBy.toUpperCase().contains("NULLS"));
        Assert.assertEquals(
                "ORDER BY CASE WHEN \"A\" IS NULL THEN 1 ELSE 0 END, \"A\" ASC, "
                        + "CASE WHEN \"B\" IS NULL THEN 0 ELSE 1 END, \"B\" DESC",
                orderBy);
    }
}
