/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;

import static com.amazonaws.athena.connectors.synapse.SynapseConstants.QUOTE_CHARACTER;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SynapseRecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_QUERY_ID = "testQueryId";
    private static final String TEST_DB_NAME = "fakedatabase";
    private static final String TEST_HOSTNAME = "hostname";
    private static final String TEST_JDBC_URL = String.format("synapse://jdbc:sqlserver://%s;databaseName=%s", TEST_HOSTNAME, TEST_DB_NAME);
    private static final TableName TEST_TABLE_NAME = new TableName(TEST_SCHEMA, TEST_TABLE);
    
    // Test column names
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_COL2 = "testCol2";
    private static final String TEST_COL3 = "testCol3";
    private static final String TEST_COL4 = "testCol4";
    private static final String TEST_ID_COL = "id";
    private static final String TEST_NAME_COL = "name";
    private static final String TEST_CREATED_AT_COL = "created_at";
    
    // Test values
    private static final String TEST_VARCHAR_VALUE = "varcharTest";
    private static final String TEST_PARTITION_FROM = "100000";
    private static final String TEST_PARTITION_TO = "300000";
    private static final int TEST_ID_1 = 123;
    private static final int TEST_ID_2 = 124;
    private static final String TEST_NAME_1 = "test1";
    private static final String TEST_NAME_2 = "test2";

    private SynapseRecordHandler synapseRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private FederatedIdentity federatedIdentity;

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.federatedIdentity = mock(FederatedIdentity.class);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new SynapseQueryStringBuilder(QUOTE_CHARACTER, new SynapseFederationExpressionParser(QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, SynapseConstants.NAME,
                TEST_JDBC_URL);

        this.synapseRecordHandler = new SynapseRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    private Schema buildTestSchema()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.VARCHAR.getType()).build());
        return schemaBuilder.build();
    }

    private Schema buildTestSchemaWithPartition()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL2, Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL3, Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL4, Types.MinorType.VARBINARY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition", Types.MinorType.VARCHAR.getType()).build());
        return schemaBuilder.build();
    }

    private Schema buildSimpleTestSchema()
    {
        return SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder(TEST_ID_COL, Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_NAME_COL, Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder(TEST_CREATED_AT_COL, Types.MinorType.DATEMILLI.getType()).build())
                .build();
    }

    private Split mockSplitWithPartitionProperties(String from, String to, String partitionNumber)
    {
        Split split = Mockito.mock(Split.class);
        ImmutableMap<String, String> properties = ImmutableMap.of(
                SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, from,
                SynapseMetadataHandler.PARTITION_NUMBER, partitionNumber,
                SynapseMetadataHandler.PARTITION_COLUMN, TEST_COL1,
                SynapseMetadataHandler.PARTITION_BOUNDARY_TO, to
        );
        when(split.getProperties()).thenReturn(properties);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM))).thenReturn(from);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_NUMBER))).thenReturn(partitionNumber);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_COLUMN))).thenReturn(TEST_COL1);
        when(split.getProperty(eq(SynapseMetadataHandler.PARTITION_BOUNDARY_TO))).thenReturn(to);
        return split;
    }

    private ValueSet getSingleValueSet()
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        when(range.isSingleValue()).thenReturn(true);
        when(range.getLow().getValue()).thenReturn(TEST_VARCHAR_VALUE);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        Schema schema = buildTestSchema();

        Split split = Mockito.mock(Split.class);
        when(split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn(TEST_ID_COL);
        when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn(TEST_PARTITION_FROM);
        when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn(TEST_PARTITION_TO);

        ValueSet valueSet = getSingleValueSet();
        Constraints constraints = Mockito.mock(Constraints.class);
        when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put(TEST_COL4, valueSet)
                .build());

        when(constraints.getLimit()).thenReturn(5L);

        String expectedSql = "SELECT \"" + TEST_COL1 + "\", \"" + TEST_COL2 + "\", \"" + TEST_COL3 + "\", \"" + TEST_COL4 + "\" FROM \"" + TEST_SCHEMA + "\".\"" + TEST_TABLE + "\"  WHERE (\"" + TEST_COL4 + "\" = ?) AND " + TEST_ID_COL + " > " + TEST_PARTITION_FROM + " and " + TEST_ID_COL + " <= " + TEST_PARTITION_TO;
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        when(this.connection.prepareStatement(eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verify(preparedStatement, Mockito.times(1)).setString(1, TEST_VARCHAR_VALUE);
    }

    @Test
    public void buildSplitSqlWithPartition()
            throws SQLException
    {
        Schema schema = buildTestSchemaWithPartition();

        // Test case 1: Normal partition boundaries
        Split split = mockSplitWithPartitionProperties("0", "100000", "1");
        Constraints constraints = Mockito.mock(Constraints.class);
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        // Test case 2: Empty from boundary
        split = mockSplitWithPartitionProperties(" ", "100000", "1");
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        // Test case 3: Empty to boundary
        split = mockSplitWithPartitionProperties("300000", " ", "2");
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);

        // Test case 4: Both boundaries empty
        split = mockSplitWithPartitionProperties(" ", " ", "2");
        this.synapseRecordHandler.buildSplitSql(this.connection, TEST_CATALOG, TEST_TABLE_NAME, schema, constraints, split);
    }

    @Test
    public void testReadWithConstraint() throws Exception {
            Schema schema = buildSimpleTestSchema();

            Split split = Mockito.mock(Split.class);
            when(split.getProperties()).thenReturn(Collections.emptyMap());

            // Setup mock result set with actual test data
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            when(resultSet.next()).thenReturn(true, true, false); // Return true twice for two rows, then false
            when(resultSet.getInt(TEST_ID_COL)).thenReturn(TEST_ID_1, TEST_ID_2);
            when(resultSet.getString(TEST_NAME_COL)).thenReturn(TEST_NAME_1, TEST_NAME_2);
            when(resultSet.getTimestamp(TEST_CREATED_AT_COL)).thenReturn(new Timestamp(System.currentTimeMillis()));

            PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
            when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
            when(preparedStatement.executeQuery()).thenReturn(resultSet);

            DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
            when(connection.getMetaData()).thenReturn(metaData);
            when(metaData.getURL()).thenReturn("jdbc:sqlserver://test.sql.azuresynapse.net:1433;databaseName=testdb;");

            ReadRecordsRequest request = new ReadRecordsRequest(
                    federatedIdentity,
                    TEST_CATALOG,
                    TEST_QUERY_ID,
                    TEST_TABLE_NAME,
                    schema,
                    split,
                    new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 1000, Collections.emptyMap(),null),
                    0,
                    0
            );

            BlockSpiller spiller = Mockito.mock(BlockSpiller.class);
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            // Execute the test
            synapseRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);

            // Verify that writeRows was called twice (once for each row)
            verify(spiller, Mockito.times(2)).writeRows(Mockito.any());
            verify(resultSet, Mockito.times(3)).next(); // Called 3 times (2 true, 1 false)

    }
}
