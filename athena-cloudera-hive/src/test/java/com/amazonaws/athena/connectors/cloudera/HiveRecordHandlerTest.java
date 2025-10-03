/*-
 * #%L
 * athena-cloudera-hive
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

package com.amazonaws.athena.connectors.cloudera;

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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_QUOTE_CHARACTER;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HiveRecordHandlerTest
{
    private static final String CATALOG_NAME = "testCatalog";
    private static final String QUERY_ID = "queryId";
    private static final String BASE_CONNECTION_STRING = "hive://jdbc:hive2://testHost:21050/default;AuthMech=3;";
    private static final String SECRET_NAME = "testSecret";
    
    private HiveRecordHandler hiveRecordHandler;
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
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId(SECRET_NAME).build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new HiveQueryStringBuilder(HIVE_QUOTE_CHARACTER, new HiveFederationExpressionParser(HIVE_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(CATALOG_NAME, HiveConstants.HIVE_NAME,
                BASE_CONNECTION_STRING);

        this.hiveRecordHandler = new HiveRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

  
    private ValueSet getSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.VARBINARY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.isSingleValue()).thenReturn(true);
        Mockito.when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1b.isSingleValue()).thenReturn(true);
        Mockito.when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));
        final long dateDays = TimeUnit.MILLISECONDS.toDays(Date.valueOf("2020-01-05").getTime());
        ValueSet valueSet2 = getSingleValueSet(dateDays);
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol2", valueSet2)
                .build());
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.hiveRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Date expectedDate = new Date(120, 0, 5);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1))
                .setDate(1, expectedDate);
    }

    @Test
    public void buildSplitSqlTimestamp()
            throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime timestamp = LocalDateTime.parse("2024-10-03 12:34:56", formatter);
        ValueSet valueSet2 = getSingleValueSet(timestamp);
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet2)
                .build());
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.hiveRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        LocalDateTime timestampExp = LocalDateTime.parse("2024-10-03 12:34:56", formatter);
        Timestamp expectedTimestamp = new Timestamp(timestamp.toInstant(ZoneOffset.UTC).toEpochMilli());
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1))
                .setTimestamp(1, expectedTimestamp);
    }

    @Test
    public void getCredentialProvider_withSecret_returnsHiveCredentialsProvider() throws Exception
    {
        DatabaseConnectionConfig configWithSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, HiveConstants.HIVE_NAME,
                BASE_CONNECTION_STRING + "${" + SECRET_NAME + "}", SECRET_NAME);

        mockSecretManagerResponse();
        CredentialsProvider provider = captureCredentialsProvider(configWithSecret, true);

        assertNotNull("Expected non-null CredentialsProvider when secret configured", provider);
        assertTrue("Expected HiveCredentialsProvider type", provider instanceof HiveCredentialsProvider);
    }

    @Test
    public void getCredentialProvider_withoutSecret_returnsNull() throws Exception
    {
        DatabaseConnectionConfig configWithoutSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, HiveConstants.HIVE_NAME,
                BASE_CONNECTION_STRING);

        CredentialsProvider provider = captureCredentialsProvider(configWithoutSecret, false);

        assertNull("Expected null CredentialsProvider when no secret configured", provider);
    }

    /**
     * Captures the CredentialsProvider used by the JDBC connection factory
     */
    private CredentialsProvider captureCredentialsProvider(DatabaseConnectionConfig config, boolean hasSecret) throws Exception
    {
        Connection mockConn = mockConnectionAndMetadata();
        if (hasSecret) {
            when(jdbcConnectionFactory.getConnection(Mockito.any(CredentialsProvider.class)))
                    .thenReturn(mockConn);
        }
        else {
            when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class)))
                    .thenReturn(mockConn);
        }

        HiveRecordHandler handler = new HiveRecordHandler(
                config, amazonS3, secretsManager, athena, jdbcConnectionFactory,
                jdbcSplitQueryBuilder, ImmutableMap.of());

        handler.readWithConstraint(
                mock(BlockSpiller.class),
                mockReadRecordsRequest(),
                mock(QueryStatusChecker.class));

        ArgumentCaptor<CredentialsProvider> captor = ArgumentCaptor.forClass(CredentialsProvider.class);
        verify(jdbcConnectionFactory).getConnection(captor.capture());
        return captor.getValue();
    }

    private void mockSecretManagerResponse()
    {
        GetSecretValueResponse mockResponse =
                GetSecretValueResponse.builder()
                        .secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}")
                        .build();

        when(secretsManager.getSecretValue(
                        Mockito.any(GetSecretValueRequest.class)))
                .thenReturn(mockResponse);
    }

    private Connection mockConnectionAndMetadata() throws Exception
    {
        Connection mockConn = mock(Connection.class);
        java.sql.DatabaseMetaData metaData = mock(java.sql.DatabaseMetaData.class);
        when(metaData.getDatabaseProductName()).thenReturn("Hive");
        when(mockConn.getMetaData()).thenReturn(metaData);

        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockConn.prepareStatement(Mockito.anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        return mockConn;
    }

    private ReadRecordsRequest mockReadRecordsRequest()
    {
        ReadRecordsRequest req = mock(ReadRecordsRequest.class);
        Split split = mock(Split.class);
        SchemaBuilder sb = SchemaBuilder.newBuilder();
        sb.addField(FieldBuilder.newBuilder("testCol", Types.MinorType.VARCHAR.getType()).build());

        when(req.getQueryId()).thenReturn(QUERY_ID);
        when(req.getCatalogName()).thenReturn(CATALOG_NAME);
        when(req.getTableName()).thenReturn(new TableName("testSchema", "testTable"));
        when(req.getSplit()).thenReturn(split);
        when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "*"));
        when(split.getProperty("partition")).thenReturn("*");
        when(req.getSchema()).thenReturn(sb.build());
        when(req.getConstraints()).thenReturn(mock(Constraints.class));
        return req;
    }
}
