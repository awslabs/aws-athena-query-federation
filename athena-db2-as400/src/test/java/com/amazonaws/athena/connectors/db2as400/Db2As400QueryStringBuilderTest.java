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

import com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
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

    // Substrait plan for: SELECT MOD(int_col, 2) AS m FROM test_table
    // (generated with the SDK EncodedSubstraitPlanStringGenerator test table; Calcite normalizes
    //  both MOD(a, b) and the a % b operator to the MODULUS call).
    private static final String MODULUS_PROJECTION_PLAN =
            "Ch4IARIaL2Z1bmN0aW9uc19hcml0aG1ldGljLnlhbWwSFxoVCAEQARoPbW9kdWx1czppMzJfaTMyGtsEEtgECtIEOs8ECgUSAwoBFxKjBAqgBAoCCgAS/gMKCGJvb2xfY29sCgt0aW55aW50X2NvbAoMc21hbGxpbnRfY29sCgdpbnRfY29sCgpiaWdpbnRfY29sCglmbG9hdF9jb2wKCmRvdWJsZV9jb2wKCHJlYWxfY29sCgt2YXJjaGFyX2NvbAoIY2hhcl9jb2wKCmJpbmFyeV9jb2wKDXZhcmJpbmFyeV9jb2wKCGRhdGVfY29sCgh0aW1lX2NvbAoNdGltZXN0YW1wX2NvbAoLZGVjaW1hbF9jb2wKDGRlY2ltYWxfY29sMgoMZGVjaW1hbF9jb2wzCglhcnJheV9jb2wKDWludF9hcnJheV9jb2wKB21hcF9jb2wKEG1hcF93aXRoX2RlY2ltYWwKDG5lc3RlZF9hcnJheRLfAQoECgIQAgoEEgIQAgoEGgIQAgoEKgIQAgoEOgIQAgoEWgIQAgoEWgIQAgoEUgIQAgoEYgIQAgoHqgEECAEYAgoHugEECAEYAgoEagIQAgoFggECEAIKBYoBAhACCgWKAgIYAgoJwgEGCAQQEyACCgnCAQYIAhAKIAIKCcIBBggKEBMgAgoL2gEICgRiAhACGAIKC9oBCAoEKgIQAhgCChHiAQ4KBGICEAISBCoCEAIgAgoW4gETCgRiAhACEgnCAQYIAhAKIAIgAgoS2gEPCgvaAQgKBCoCEAIYAhgCGAI6GQoLdGVzdF9zY2hlbWEKCnRlc3RfdGFibGUaIBoeCAEaBCoCEAIiDBoKEggKBBICCAMiACIGGgQKAigCEgFNMgsQSioHaXN0aG11cw==";

    // Substrait plan for: SELECT int_col FROM test_table WHERE MOD(int_col, 2) = 0
    private static final String MODULUS_FILTER_PLAN =
            "Ch4IAhIaL2Z1bmN0aW9uc19hcml0aG1ldGljLnlhbWwKHggBEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIVGhMIARABGg1lcXVhbDphbnlfYW55EhcaFQgCEAIaD21vZHVsdXM6aTMyX2kzMhqNBRKKBQr+BDr7BAoFEgMKARcS5QQS4gQKAgoAEqMECqAECgIKABL+AwoIYm9vbF9jb2wKC3RpbnlpbnRfY29sCgxzbWFsbGludF9jb2wKB2ludF9jb2wKCmJpZ2ludF9jb2wKCWZsb2F0X2NvbAoKZG91YmxlX2NvbAoIcmVhbF9jb2wKC3ZhcmNoYXJfY29sCghjaGFyX2NvbAoKYmluYXJ5X2NvbAoNdmFyYmluYXJ5X2NvbAoIZGF0ZV9jb2wKCHRpbWVfY29sCg10aW1lc3RhbXBfY29sCgtkZWNpbWFsX2NvbAoMZGVjaW1hbF9jb2wyCgxkZWNpbWFsX2NvbDMKCWFycmF5X2NvbAoNaW50X2FycmF5X2NvbAoHbWFwX2NvbAoQbWFwX3dpdGhfZGVjaW1hbAoMbmVzdGVkX2FycmF5Et8BCgQKAhACCgQSAhACCgQaAhACCgQqAhACCgQ6AhACCgRaAhACCgRaAhACCgRSAhACCgRiAhACCgeqAQQIARgCCge6AQQIARgCCgRqAhACCgWCAQIQAgoFigECEAIKBYoCAhgCCgnCAQYIBBATIAIKCcIBBggCEAogAgoJwgEGCAoQEyACCgvaAQgKBGICEAIYAgoL2gEICgQqAhACGAIKEeIBDgoEYgIQAhIEKgIQAiACChbiARMKBGICEAISCcIBBggCEAogAiACChLaAQ8KC9oBCAoEKgIQAhgCGAIYAjoZCgt0ZXN0X3NjaGVtYQoKdGVzdF90YWJsZRo2GjQIARoECgIQAiIiGiAaHggCGgQqAhACIgwaChIICgQSAggDIgAiBhoECgIoAiIGGgQKAigAGgoSCAoEEgIIAyIAEgdJTlRfQ09MMgsQSioHaXN0aG11cw==";

    private static Schema oneColumnSchema()
    {
        return SchemaBuilder.newBuilder().addIntField("int_col").build();
    }

    /**
     * Db2 for i does not support the ANSI {@code %} modulo operator; the modulus must render as the
     * {@code MOD(x, y)} scalar function. The capability test only asserts that modulus is advertised,
     * so this drives a Substrait plan carrying a modulus expression through {@code buildSql} and
     * checks the generated SQL is valid Db2-for-i SQL.
     */
    @Test
    public void buildSql_modulusProjectionQueryPlan_rendersModFunction() throws Exception
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder(QUOTE_CHARACTER);
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getQueryPlan()).thenReturn(new QueryPlan("v1", MODULUS_PROJECTION_PLAN));
        Split split = Mockito.mock(Split.class);

        PreparedStatement result = builder.buildSql(connection, "", "test_schema", "test_table", oneColumnSchema(), constraints, split);

        Assert.assertNotNull(result);
        String generatedSql = sqlCaptor.getValue();
        Assert.assertTrue(generatedSql.toUpperCase().contains("MOD("), "Expected MOD() function, got: " + generatedSql);
        Assert.assertFalse(generatedSql.contains(" % "), "Db2 for i rejects the % modulo operator: " + generatedSql);
    }

    /**
     * Same as above for a modulus in a WHERE predicate, driven through the {@code Db2As400Dialect}
     * casing-filter path (the dialect returned by {@code getSqlDialect(true)}).
     */
    @Test
    public void buildSql_modulusFilterQueryPlanWithCasingFilter_rendersModFunction() throws Exception
    {
        Db2As400QueryStringBuilder builder = new Db2As400QueryStringBuilder(QUOTE_CHARACTER);
        Connection connection = Mockito.mock(Connection.class);
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getQueryPlan()).thenReturn(new QueryPlan("v1", MODULUS_FILTER_PLAN));
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(EnvironmentConstants.CATALOG_CASING_FILTER)).thenReturn(EnvironmentConstants.UPPERCASE_ONLY);

        PreparedStatement result = builder.buildSql(connection, "", "test_schema", "test_table", oneColumnSchema(), constraints, split);

        Assert.assertNotNull(result);
        String generatedSql = sqlCaptor.getValue();
        Assert.assertTrue(generatedSql.toUpperCase().contains("MOD("), "Expected MOD() function, got: " + generatedSql);
        Assert.assertFalse(generatedSql.contains(" % "), "Db2 for i rejects the % modulo operator: " + generatedSql);
    }
}
