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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.nullable;

public class Db2As400RecordHandlerTest {
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_COL3 = "testCol3";
    private static final String TEST_COL4 = "testCol4";
    private static final String TEST_VARCHAR_VALUE = "varcharTest";
    private static final String QPT_SCHEMA_FUNCTION_NAME = "schemaFunctionName";
    private static final String QPT_ENABLE_QUERY_PASSTHROUGH = "enableQueryPassthrough";
    private static final String QPT_NAME = "name";
    private static final String QPT_SCHEMA = "schema";
    private static final String QPT_SYSTEM_QUERY_SIGNATURE = "system.query";
    private static final String QPT_SYSTEM_SCHEMA = "system";
    private static final String QPT_QUERY_NAME = "query";
    private static final String QPT_ENABLE_TRUE = "true";

    private Db2As400RecordHandler db2As400RecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup() throws Exception {
        System.setProperty("aws.region", "us-east-1");
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new Db2As400QueryStringBuilder("\"");
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", Db2As400Constants.NAME,
                "db2as400://jdbc:as400://testhost;user=dummy;password=dummy;");
        this.db2As400RecordHandler = new Db2As400RecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void buildSplitSql_withConstraints_returnsPreparedStatement()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(Db2As400MetadataHandler.PARTITION_NUMBER)).thenReturn("0");

        ValueSet valueSet = getSingleValueSet(TEST_VARCHAR_VALUE);
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put(TEST_COL4, valueSet)
                .build());

        String expectedSql = String.format("SELECT \"%s\", \"%s\", \"%s\", \"%s\" FROM %s.%s  WHERE (\"%s\" = ?)",
                TEST_COL1, TEST_COL2, TEST_COL3, TEST_COL4, TEST_SCHEMA, TEST_TABLE, TEST_COL4);
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.db2As400RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, TEST_VARCHAR_VALUE);
    }

    @Test
    public void buildSplitSql_withQueryPassthrough_returnsPassthroughQuery()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                .build();
        Split split = Mockito.mock(Split.class);

        String passthroughQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", TEST_SCHEMA, TEST_TABLE, TEST_COL1);
        Map<String, String> queryPassthroughArgs = ImmutableMap.of(
                JdbcQueryPassthrough.QUERY, passthroughQuery,
                QPT_SCHEMA_FUNCTION_NAME, QPT_SYSTEM_QUERY_SIGNATURE,
                QPT_ENABLE_QUERY_PASSTHROUGH, QPT_ENABLE_TRUE,
                QPT_NAME, QPT_QUERY_NAME,
                QPT_SCHEMA, QPT_SYSTEM_SCHEMA);
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(passthroughQuery))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.db2As400RecordHandler.buildSplitSql(
                this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(this.connection).prepareStatement(passthroughQuery);
    }

    @Test
    public void buildSplitSql_withPassthroughEnabledAndMissingQuery_throwsException()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                .build();
        Split split = Mockito.mock(Split.class);

        Map<String, String> queryPassthroughArgs = ImmutableMap.of(
                QPT_SCHEMA_FUNCTION_NAME, QPT_SYSTEM_QUERY_SIGNATURE,
                QPT_ENABLE_QUERY_PASSTHROUGH, QPT_ENABLE_TRUE,
                QPT_NAME, QPT_QUERY_NAME,
                QPT_SCHEMA, QPT_SYSTEM_SCHEMA);
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        Assert.assertTrue(constraints.isQueryPassThrough());

        try {
            this.db2As400RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
            Assert.fail("Expected AthenaConnectorException to be thrown");
        }
        catch (AthenaConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Missing Query Passthrough Argument: QUERY"));
        }
    }

    @Test
    public void buildSplitSql_withPassthroughAndWrongSchemaFunctionName_throwsException()
            throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build())
                .build();
        Split split = Mockito.mock(Split.class);

        String passthroughQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", TEST_SCHEMA, TEST_TABLE, TEST_COL1);
        Map<String, String> queryPassthroughArgs = ImmutableMap.of(
                JdbcQueryPassthrough.QUERY, passthroughQuery,
                QPT_SCHEMA_FUNCTION_NAME, "wrong.function",
                QPT_ENABLE_QUERY_PASSTHROUGH, QPT_ENABLE_TRUE,
                QPT_NAME, QPT_QUERY_NAME,
                QPT_SCHEMA, QPT_SYSTEM_SCHEMA);
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        Assert.assertTrue(constraints.isQueryPassThrough());

        try {
            this.db2As400RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
            Assert.fail("Expected AthenaConnectorException to be thrown");
        }
        catch (AthenaConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Function Signature doesn't match implementation's"));
        }
    }
}
