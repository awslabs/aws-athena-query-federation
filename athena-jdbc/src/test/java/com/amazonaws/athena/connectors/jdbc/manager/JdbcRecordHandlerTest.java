/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JdbcRecordHandlerTest
        extends TestBase
{

    private JdbcRecordHandler jdbcRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private AmazonS3 amazonS3;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private QueryStatusChecker queryStatusChecker;
    private FederatedIdentity federatedIdentity;
    private PreparedStatement preparedStatement;

    @Before
    public void setup()
            throws Exception
    {
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(this.connection);
        this.amazonS3 = Mockito.mock(AmazonS3.class);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        this.preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement("someSql")).thenReturn(this.preparedStatement);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "fakedatabase",
                "fakedatabase://jdbc:fakedatabase://hostname/${testSecret}", "testSecret");
        this.jdbcRecordHandler = new JdbcRecordHandler(this.amazonS3, this.secretsManager, this.athena, databaseConnectionConfig, this.jdbcConnectionFactory)
        {
            @Override
            public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
                    throws SQLException
            {
                return jdbcConnection.prepareStatement("someSql");
            }
        };
        this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @Test
    public void readWithConstraint()
            throws Exception
    {
        ConstraintEvaluator constraintEvaluator = Mockito.mock(ConstraintEvaluator.class);
        Mockito.when(constraintEvaluator.apply(Mockito.anyString(), Mockito.any())).thenReturn(true);

        TableName inputTableName = new TableName("testSchema", "testTable");
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testPartitionCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema fieldSchema = expectedSchemaBuilder.build();

        BlockAllocator allocator = new BlockAllocatorImpl();
        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();

        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add("testPartitionCol", String.valueOf("testPartitionValue"));

        Constraints constraints = Mockito.mock(Constraints.class, Mockito.RETURNS_DEEP_STUBS);

        String[] schema = {"testCol1", "testCol2"};
        int[] columnTypes = {Types.INTEGER, Types.VARCHAR};
        Object[][] values = {{1, "testVal1"}, {2, "testVal2"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);
        ResultSet resultSet = mockResultSet(schema, columnTypes, values, rowNumber);
        Mockito.when(this.preparedStatement.executeQuery()).thenReturn(resultSet);

        SpillConfig spillConfig = Mockito.mock(SpillConfig.class);
        Mockito.when(spillConfig.getSpillLocation()).thenReturn(s3SpillLocation);
        BlockSpiller s3Spiller = new S3BlockSpiller(this.amazonS3, spillConfig, allocator, fieldSchema, constraintEvaluator);
        ReadRecordsRequest readRecordsRequest = new ReadRecordsRequest(this.federatedIdentity, "testCatalog", "testQueryId", inputTableName, fieldSchema, splitBuilder.build(), constraints, 1024, 1024);

        Mockito.when(amazonS3.putObject(Mockito.any())).thenAnswer((Answer<PutObjectResult>) invocation -> {
            ByteArrayInputStream byteArrayInputStream = (ByteArrayInputStream) ((PutObjectRequest) invocation.getArguments()[0]).getInputStream();
            int n = byteArrayInputStream.available();
            byte[] bytes = new byte[n];
            byteArrayInputStream.read(bytes, 0, n);
            String data = new String(bytes, StandardCharsets.UTF_8);
            Assert.assertTrue(data.contains("testVal1") || data.contains("testVal2") || data.contains("testPartitionValue"));
            return new PutObjectResult();
        });

        this.jdbcRecordHandler.readWithConstraint(s3Spiller, readRecordsRequest, queryStatusChecker);
    }
    @Test
    public void makeExtractor()
            throws SQLException
    {
        String[] schema = {"testCol1", "testCol2"};
        int[] columnTypes = {Types.INTEGER, Types.VARCHAR};
        Object[][] values = {{1, "testVal1"}, {2, "testVal2"}};
        AtomicInteger rowNumber = new AtomicInteger(-1);

        ResultSet resultSet = mockResultSet(schema, columnTypes, values, rowNumber);
        Mockito.when(this.preparedStatement.executeQuery()).thenReturn(resultSet);
        Map<String,String> partitionMap = Collections.singletonMap("testPartitionCol","testPartitionValue");

        Extractor actualInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build(),resultSet,partitionMap);
        Extractor actualVarchar = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),resultSet,partitionMap);
        Extractor actualBit = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.BIT.getType()).build(),resultSet,partitionMap);
        Extractor actualTinyInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.TINYINT.getType()).build(),resultSet,partitionMap);
        Extractor actualSmallInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol5", org.apache.arrow.vector.types.Types.MinorType.SMALLINT.getType()).build(),resultSet,partitionMap);
        Extractor actualVarbinary = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol6", org.apache.arrow.vector.types.Types.MinorType.VARBINARY.getType()).build(),resultSet,partitionMap);
        Extractor actualBigInt = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol8", org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType()).build(),resultSet,partitionMap);
        Extractor actualFloat4 = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol9", org.apache.arrow.vector.types.Types.MinorType.FLOAT4.getType()).build(),resultSet,partitionMap);
        Extractor actualFloat8 = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol10", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build(),resultSet,partitionMap);
        Extractor actualDateDay = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol11", org.apache.arrow.vector.types.Types.MinorType.DATEDAY.getType()).build(),resultSet,partitionMap);
        Extractor actualDateMilli = this.jdbcRecordHandler.makeExtractor(FieldBuilder.newBuilder("testCol12", org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()).build(),resultSet,partitionMap);

        Assert.assertTrue(actualInt instanceof IntExtractor);
        Assert.assertTrue(actualVarchar instanceof VarCharExtractor);
        Assert.assertTrue(actualBit instanceof BitExtractor);
        Assert.assertTrue(actualTinyInt instanceof TinyIntExtractor);
        Assert.assertTrue(actualSmallInt instanceof SmallIntExtractor);
        Assert.assertTrue(actualVarbinary instanceof VarBinaryExtractor);
        Assert.assertTrue(actualBigInt instanceof BigIntExtractor);
        Assert.assertTrue(actualFloat4 instanceof Float4Extractor);
        Assert.assertTrue(actualFloat8 instanceof Float8Extractor);
        Assert.assertTrue(actualDateDay instanceof DateDayExtractor);
        Assert.assertTrue(actualDateMilli instanceof DateMilliExtractor);

    }
}
