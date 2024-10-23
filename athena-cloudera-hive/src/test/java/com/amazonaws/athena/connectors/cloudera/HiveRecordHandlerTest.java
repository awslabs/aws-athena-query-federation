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
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
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
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_QUOTE_CHARACTER;
import static org.mockito.ArgumentMatchers.nullable;

public class HiveRecordHandlerTest
{
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
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(GetSecretValueRequest.builder().secretId("testSecret").build()))).thenReturn(GetSecretValueResponse.builder().secretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}").build());
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new HiveQueryStringBuilder(HIVE_QUOTE_CHARACTER, new HiveFederationExpressionParser(HIVE_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", HiveConstants.HIVE_NAME,
                "hive2://jdbc:hive2://54.89.6.2:10000/authena;AuthMech=3;UID=hive;PWD=hive");

        this.hiveRecordHandler = new HiveRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
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
}
