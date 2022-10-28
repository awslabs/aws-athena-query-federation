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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

public class SynapseRecordHandlerTest
{
    private SynapseRecordHandler synapseRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private AmazonS3 amazonS3;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = Mockito.mock(AmazonS3.class);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(Mockito.mock(JdbcCredentialProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new SynapseQueryStringBuilder("`");
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SynapseConstants.NAME,
                "synapse://jdbc:sqlserver://hostname;databaseName=fakedatabase");

        this.synapseRecordHandler = new SynapseRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder);
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
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn("id");
        Mockito.when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("100000");
        Mockito.when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("300000");

        ValueSet valueSet = getSingleValueSet("varcharTest");
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol4", valueSet)
                .build());

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4` FROM `testSchema`.`testTable`  WHERE (`testCol4` = ?) AND id > 100000 and id <= 300000";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "varcharTest");
    }

    @Test
    public void buildSplitSqlWithPartition()
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
        Mockito.when(split.getProperties()).thenReturn(Map.of("PARTITION_BOUNDARY_FROM", "0", "PARTITION_NUMBER", "1", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", "100000"));
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_FROM"))).thenReturn("0");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_NUMBER"))).thenReturn("1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_TO"))).thenReturn("100000");

        Constraints constraints = Mockito.mock(Constraints.class);
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.anyString())).thenReturn(expectedPreparedStatement);
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Mockito.when(split.getProperties()).thenReturn(Map.of("PARTITION_BOUNDARY_FROM", " ", "PARTITION_NUMBER", "1", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", "100000"));
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_FROM"))).thenReturn(" ");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_NUMBER"))).thenReturn("1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_TO"))).thenReturn("100000");
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Mockito.when(split.getProperties()).thenReturn(Map.of("PARTITION_BOUNDARY_FROM", "300000", "PARTITION_NUMBER", "2", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", " "));
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_FROM"))).thenReturn("300000");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_NUMBER"))).thenReturn("1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_TO"))).thenReturn(" ");
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Mockito.when(split.getProperties()).thenReturn(Map.of("PARTITION_BOUNDARY_FROM", " ", "PARTITION_NUMBER", "2", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", " "));
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_FROM"))).thenReturn(" ");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_NUMBER"))).thenReturn("1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_BOUNDARY_TO"))).thenReturn(" ");
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
    }
}
