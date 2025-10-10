/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
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
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collections;

import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OracleRecordHandlerTest
{
    private static final String CATALOG_NAME = "testCatalog";
    private static final String QUERY_ID = "queryId";
    private static final String BASE_CONNECTION_STRING = "oracle://jdbc:oracle:thin:@//testHost:1521/orcl";
    private static final String SECRET_NAME = "testSecret";
    
    private OracleRecordHandler oracleRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    private static final String ORACLE_QUOTE_CHARACTER = "\"";

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = mock(S3Client.class);
        this.secretsManager = mock(SecretsManagerClient.class);
        this.athena = mock(AthenaClient.class);
        this.connection = mock(Connection.class);
        this.jdbcConnectionFactory = mock(JdbcConnectionFactory.class);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new OracleQueryStringBuilder(ORACLE_QUOTE_CHARACTER, new OracleFederationExpressionParser(ORACLE_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(CATALOG_NAME, ORACLE_NAME,
                "oracle://jdbc:oracle:thin:username/password@//127.0.0.1:1521/orcl");

        this.oracleRecordHandler = new OracleRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol9", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol10", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "p0"));
        when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("p0");

        Range range1a = mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        when(range1a.isSingleValue()).thenReturn(true);
        when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        when(range1b.isSingleValue()).thenReturn(true);
        when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));

        ValueSet valueSet2 = getRangeSet(Marker.Bound.EXACTLY, "1", Marker.Bound.BELOW, "10");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.1F);
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);
        final long epochDaysPrior1970 = LocalDate.parse("1967-07-27").toEpochDay();
        ValueSet valueSet9 = getSingleValueSet(epochDaysPrior1970);
        final long epochDaysPost1970 = LocalDate.parse("1971-01-01").toEpochDay();
        ValueSet valueSet10 = getSingleValueSet(epochDaysPost1970);

        Constraints constraints = mock(Constraints.class);
        when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("testCol4", valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .put("testCol9", valueSet9)
                .put("testCol10", valueSet10)
                .build());

        when(constraints.getLimit()).thenReturn(5L);

        String expectedSql = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\", \"testCol5\", \"testCol6\", \"testCol7\", \"testCol8\", \"testCol9\", \"testCol10\" FROM \"testSchema\".\"testTable\" PARTITION (p0)  WHERE (\"testCol1\" IN (?,?)) AND ((\"testCol2\" >= ? AND \"testCol2\" < ?)) AND ((\"testCol3\" > ? AND \"testCol3\" <= ?)) AND (\"testCol4\" = ?) AND (\"testCol5\" = ?) AND (\"testCol6\" = ?) AND (\"testCol7\" = ?) AND (\"testCol8\" = ?) AND (\"testCol9\" = ?) AND (\"testCol10\" = ?) FETCH FIRST 5 ROWS ONLY ";
        PreparedStatement expectedPreparedStatement = mock(PreparedStatement.class);
        when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.oracleRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        assertEquals(expectedPreparedStatement, preparedStatement);
        verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        verify(preparedStatement, Mockito.times(1)).setString(3, "1");
        verify(preparedStatement, Mockito.times(1)).setString(4, "10");
        verify(preparedStatement, Mockito.times(1)).setLong(5, 2L);
        verify(preparedStatement, Mockito.times(1)).setLong(6, 20L);
        verify(preparedStatement, Mockito.times(1)).setFloat(7, 1.1F);
        verify(preparedStatement, Mockito.times(1)).setShort(8, (short) 1);
        verify(preparedStatement, Mockito.times(1)).setByte(9, (byte) 0);
        verify(preparedStatement, Mockito.times(1)).setDouble(10, 1.2d);
        verify(preparedStatement, Mockito.times(1)).setBoolean(11, true);
        //year – the year minus 1900; must be 0 to 8099. (Note that 8099 is 9999 minus 1900.) month – 0 to 11 day – 1 to 31
        Date expectedDatePrior1970 = new Date(67, 6, 27); //Date: 1967-07-27
        verify(preparedStatement, Mockito.times(1)).setDate(12, expectedDatePrior1970);
        Date expectedDatePost1970 = new Date(71, 0, 1); //Date: 1971-01-01
        verify(preparedStatement, Mockito.times(1)).setDate(13, expectedDatePost1970);
    }

    private ValueSet getSingleValueSet(Object value)
    {
        Range range = mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        when(range.isSingleValue()).thenReturn(true);
        when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue)
    {
        Range range = mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        when(range.isSingleValue()).thenReturn(false);
        when(range.getLow().getBound()).thenReturn(lowerBound);
        when(range.getLow().getValue()).thenReturn(lowerValue);
        when(range.getHigh().getBound()).thenReturn(upperBound);
        when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void testGetCredentialProvider_withSecret() throws Exception
    {
        DatabaseConnectionConfig configWithSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, ORACLE_NAME,
                BASE_CONNECTION_STRING.replace("@//", "${" + SECRET_NAME + "}@//"), SECRET_NAME);

        mockSecretManagerResponse();
        CredentialsProvider provider = captureCredentialsProvider(configWithSecret, true);

        assertNotNull("Expected non-null CredentialsProvider when secret configured", provider);
        assertTrue("Expected OracleCredentialsProvider type", provider instanceof OracleCredentialsProvider);
    }

    @Test
    public void testGetCredentialProvider_withoutSecret() throws Exception
    {
        DatabaseConnectionConfig configWithoutSecret = new DatabaseConnectionConfig(
                CATALOG_NAME, ORACLE_NAME,
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
            Mockito.when(jdbcConnectionFactory.getConnection(Mockito.any(CredentialsProvider.class)))
                    .thenReturn(mockConn);
        }
        else {
            Mockito.when(jdbcConnectionFactory.getConnection(Mockito.nullable(CredentialsProvider.class)))
                    .thenReturn(mockConn);
        }

        OracleRecordHandler handler = new OracleRecordHandler(
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
        DatabaseMetaData metaData = mock(java.sql.DatabaseMetaData.class);
        when(metaData.getDatabaseProductName()).thenReturn("Oracle");
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
        when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "ALL_PARTITIONS"));
        when(split.getProperty("partition_name")).thenReturn("ALL_PARTITIONS");
        when(req.getSchema()).thenReturn(sb.build());
        when(req.getConstraints()).thenReturn(mock(Constraints.class));
        return req;
    }
}
