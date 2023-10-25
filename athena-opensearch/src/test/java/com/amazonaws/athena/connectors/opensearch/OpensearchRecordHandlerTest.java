/*-
 * #%L
 * athena-opensearch
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
package com.amazonaws.athena.connectors.opensearch;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Struct;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connectors.opensearch.OpensearchConstants.OPENSEARCH_NAME;
import static com.amazonaws.athena.connectors.opensearch.OpensearchConstants.OPENSEARCH_QUOTE_CHARACTER;;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

public class OpensearchRecordHandlerTest extends TestBase
{
    private OpensearchRecordHandler opensearchRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private AmazonS3 amazonS3;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private FederatedIdentity federatedIdentity;
    private QueryStatusChecker queryStatusChecker;

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = Mockito.mock(AmazonS3.class);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new OpensearchQueryStringBuilder(OPENSEARCH_QUOTE_CHARACTER, new OpensearchFederationExpressionParser(OPENSEARCH_QUOTE_CHARACTER) );
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", OPENSEARCH_NAME,
                "opensearch://jdbc:opensearch://hostname/user=A&password=B");
        this.opensearchRecordHandler = new OpensearchRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
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
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "*"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("*");

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.isSingleValue()).thenReturn(true);
        Mockito.when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1b.isSingleValue()).thenReturn(true);
        Mockito.when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));

        ValueSet valueSet2 = getRangeSet(Marker.Bound.EXACTLY, "1", Marker.Bound.BELOW, "10");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.1F);
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("testCol4", valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .build());

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4`, `testCol5`, `testCol6`, `testCol7`, `testCol8` FROM `testTable`  WHERE (`testCol1` IN (?,?)) AND ((`testCol2` >= ? AND `testCol2` < ?)) AND ((`testCol3` > ? AND `testCol3` <= ?)) AND (`testCol4` = ?) AND (`testCol5` = ?) AND (`testCol6` = ?) AND (`testCol7` = ?) AND (`testCol8` = ?)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.opensearchRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(3, "1");
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(4, "10");
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(5, 2L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setLong(6, 20L);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(7, 1.1F);
        Mockito.verify(preparedStatement, Mockito.times(1)).setShort(8, (short) 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setByte(9, (byte) 0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(10, 1.2d);
        Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(11, true);
    }

    private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void readRecordsTest() throws Exception
    {
        ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
        Mockito.when(constraintEvaluator.apply(nullable(String.class), any())).thenReturn(true);

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(any())).thenReturn(preparedStatement);

        TableName inputTableName = new TableName("testSchema", "testTable");

        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField("int_column", org.apache.arrow.vector.types.Types.MinorType.INT.getType());
        expectedSchemaBuilder.addField("string_column", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType());
        expectedSchemaBuilder.addStructField("struct_column");
        expectedSchemaBuilder.addChildField("struct_column", FieldBuilder.newBuilder("stringAttribute1", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addChildField("struct_column", FieldBuilder.newBuilder("intAttribute2", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField("partition_name", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType());
        Schema fieldSchema = expectedSchemaBuilder.build();

        BlockAllocator allocator = new BlockAllocatorImpl();
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();

        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add("partition_name", String.valueOf("*"));

        Constraints constraints = Mockito.mock(Constraints.class, Mockito.RETURNS_DEEP_STUBS);
        List<FederationExpression> fedExpList = Collections.<FederationExpression>emptyList();
        Mockito.when(constraints.getExpression()).thenReturn(fedExpList);

        String[] schema = {"int_column", "string_column", "struct_value", "partition_name"};
        int[] columnTypes = {java.sql.Types.INTEGER, java.sql.Types.VARCHAR, java.sql.Types.STRUCT};
        Map.Entry<String, Object>[] struct1Attributes = new Map.Entry[3];
        struct1Attributes[0]  = new HashMap.SimpleEntry<>("stringAttribute1", "random_string");
        struct1Attributes[1]  = new HashMap.SimpleEntry<>("intAttribute2", 2);
        Map.Entry<String, Object>[] struct2Attributes = new Map.Entry[3];
        struct2Attributes[0]  = new HashMap.SimpleEntry<>("stringAttribute1", "some_string");
        struct2Attributes[1]  = new HashMap.SimpleEntry<>("intAttribute2", 5);
        Map.Entry<String, Object>[] struct3Attributes = new Map.Entry[3];
        struct3Attributes[0]  = new HashMap.SimpleEntry<>("stringAttribute1", "generated_string");
        struct3Attributes[1]  = new HashMap.SimpleEntry<>("intAttribute2", 10);
        Object[][] values = {
            {99, "Athena", struct1Attributes, "*"},
            {25, "Federation", struct2Attributes, "*"},
            {172, "OpenSearch", struct2Attributes, "*"},
        };
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, values, rowNumber);
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
        Mockito.when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);
        BlockSpiller s3Spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema, constraintEvaluator, com.google.common.collect.ImmutableMap.of());
        ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(this.federatedIdentity, "testCatalog", "testQueryId", inputTableName, fieldSchema, splitBuilder.build(), constraints, 1024, 1024);

        Mockito.when(amazonS3.putObject(any())).thenAnswer((Answer<PutObjectResult>) invocation -> {
            ByteArrayInputStream byteArrayInputStream = (ByteArrayInputStream) ((PutObjectRequest) invocation.getArguments()[0]).getInputStream();
            int n = byteArrayInputStream.available();
            byte[] bytes = new byte[n];
            byteArrayInputStream.read(bytes, 0, n);
            String data = new String(bytes, StandardCharsets.UTF_8);
            System.out.println("Data: " + data);
            Assert.assertTrue(data.contains("int_column") || data.contains("string_column") || data.contains("struct_value") || data.contains("partition_name"));
            return new PutObjectResult();
        });

        this.opensearchRecordHandler.readWithConstraint(s3Spiller, readRecordsRequest, queryStatusChecker);
    }
}
