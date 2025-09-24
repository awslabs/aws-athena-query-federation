/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import java.sql.SQLException;
import java.util.Collections;

import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2Constants.QUOTE_CHARACTER;
import static org.mockito.ArgumentMatchers.nullable;

public class DataLakeRecordHandlerTest
{
    private DataLakeGen2RecordHandler dataLakeGen2RecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
            throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new DataLakeGen2QueryStringBuilder(QUOTE_CHARACTER, new DataLakeGen2FederationExpressionParser(QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", DataLakeGen2Constants.NAME,
                "datalakegentwo://jdbc:sqlserver://hostname;databaseName=fakedatabase");

        this.dataLakeGen2RecordHandler = new DataLakeGen2RecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
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
    public void buildSplitSqlNew()
            throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(DataLakeGen2MetadataHandler.PARTITION_NUMBER)).thenReturn("0");

        ValueSet valueSet = getSingleValueSet("varcharTest");
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol4", valueSet)
                .build());
        Mockito.when(constraints.getLimit()).thenReturn(5L);

        String expectedSql = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\" FROM \"testSchema\".\"testTable\"  WHERE (\"testCol4\" = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.dataLakeGen2RecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "varcharTest");
    }

    @Test
    public void testReadWithConstraintWithAzureServerlessEnvironment()
            throws Exception
    {
        // Mock Azure serverless URL
        DatabaseMetaData mockDatabaseMetaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(connection.getMetaData()).thenReturn(mockDatabaseMetaData);
        Mockito.when(mockDatabaseMetaData.getURL()).thenReturn("datalakegentwo://jdbc:sqlserver://myworkspace-ondemand.sql.azuresynapse.net:1433;database=mydatabase;");

        // Mock the prepared statement and result set
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        java.sql.ResultSet mockResultSet = Mockito.mock(java.sql.ResultSet.class);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(mockPreparedStatement);
        Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        Mockito.when(mockResultSet.next()).thenReturn(false); // No rows

        // Mock the query status checker
        com.amazonaws.athena.connector.lambda.QueryStatusChecker mockQueryStatusChecker = 
            Mockito.mock(com.amazonaws.athena.connector.lambda.QueryStatusChecker.class);
        Mockito.when(mockQueryStatusChecker.isQueryRunning()).thenReturn(true);

        // Mock the block spiller
        com.amazonaws.athena.connector.lambda.data.BlockSpiller mockBlockSpiller = 
            Mockito.mock(com.amazonaws.athena.connector.lambda.data.BlockSpiller.class);

        // Create the read records request
        com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest mockReadRecordsRequest = 
            Mockito.mock(com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest.class);
        Mockito.when(mockReadRecordsRequest.getQueryId()).thenReturn("testQueryId");
        Mockito.when(mockReadRecordsRequest.getCatalogName()).thenReturn("testCatalog");
        Mockito.when(mockReadRecordsRequest.getTableName()).thenReturn(new TableName("testSchema", "testTable"));
        Mockito.when(mockReadRecordsRequest.getSchema()).thenReturn(SchemaBuilder.newBuilder().build());
        Mockito.when(mockReadRecordsRequest.getConstraints()).thenReturn(Mockito.mock(Constraints.class));
        Mockito.when(mockReadRecordsRequest.getSplit()).thenReturn(Mockito.mock(Split.class));

        // Execute the method
        dataLakeGen2RecordHandler.readWithConstraint(mockBlockSpiller, mockReadRecordsRequest, mockQueryStatusChecker);

        // Verify that setAutoCommit(false) was NOT called for Azure serverless environment
        Mockito.verify(connection, Mockito.never()).setAutoCommit(false);
        
        // Verify that commit() was NOT called for Azure serverless environment
        Mockito.verify(connection, Mockito.never()).commit();
    }

    @Test
    public void testReadWithConstraintWithStandardEnvironment()
            throws Exception
    {
        // Mock standard SQL Server URL (non-serverless)
        DatabaseMetaData mockDatabaseMetaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(connection.getMetaData()).thenReturn(mockDatabaseMetaData);
        Mockito.when(mockDatabaseMetaData.getURL()).thenReturn("datalakegentwo://jdbc:sqlserver://myserver.database.windows.net:1433;database=mydatabase;");

        // Mock the prepared statement and result set
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        java.sql.ResultSet mockResultSet = Mockito.mock(java.sql.ResultSet.class);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(mockPreparedStatement);
        Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        Mockito.when(mockResultSet.next()).thenReturn(false); // No rows

        // Mock the query status checker
        com.amazonaws.athena.connector.lambda.QueryStatusChecker mockQueryStatusChecker = 
            Mockito.mock(com.amazonaws.athena.connector.lambda.QueryStatusChecker.class);
        Mockito.when(mockQueryStatusChecker.isQueryRunning()).thenReturn(true);

        // Mock the block spiller
        com.amazonaws.athena.connector.lambda.data.BlockSpiller mockBlockSpiller = 
            Mockito.mock(com.amazonaws.athena.connector.lambda.data.BlockSpiller.class);

        // Create the read records request
        com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest mockReadRecordsRequest = 
            Mockito.mock(com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest.class);
        Mockito.when(mockReadRecordsRequest.getQueryId()).thenReturn("testQueryId");
        Mockito.when(mockReadRecordsRequest.getCatalogName()).thenReturn("testCatalog");
        Mockito.when(mockReadRecordsRequest.getTableName()).thenReturn(new TableName("testSchema", "testTable"));
        Mockito.when(mockReadRecordsRequest.getSchema()).thenReturn(SchemaBuilder.newBuilder().build());
        Mockito.when(mockReadRecordsRequest.getConstraints()).thenReturn(Mockito.mock(Constraints.class));
        Mockito.when(mockReadRecordsRequest.getSplit()).thenReturn(Mockito.mock(Split.class));

        // Execute the method
        dataLakeGen2RecordHandler.readWithConstraint(mockBlockSpiller, mockReadRecordsRequest, mockQueryStatusChecker);

        // Verify that setAutoCommit(false) WAS called for standard environment
        Mockito.verify(connection, Mockito.times(1)).setAutoCommit(false);
        
        // Verify that commit() WAS called for standard environment
        Mockito.verify(connection, Mockito.times(1)).commit();
    }
}
