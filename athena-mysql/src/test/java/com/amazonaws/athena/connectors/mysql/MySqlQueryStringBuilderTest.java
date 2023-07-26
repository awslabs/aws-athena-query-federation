/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.core.config.plugins.validation.Constraint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.DefaultJdbcFederationExpressionParser;

public class MySqlQueryStringBuilderTest
{
    private String catalogName = "testCatalog";
    private String schemaName = "testSchema";
    private String tableName = "testTable";
    private BlockAllocator allocator = new BlockAllocatorImpl("test-allocator-id");
    private Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
    private S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();
    Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null).add("partition_name", String.valueOf("p0"));
    private MySqlQueryStringBuilder mySqlQueryStringBuilder = new MySqlQueryStringBuilder("`", new DefaultJdbcFederationExpressionParser());
    Schema schema = SchemaBuilder.newBuilder()
        .addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build())
        .addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build())
        .addField(FieldBuilder.newBuilder("testCol3", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build())
        .addField(FieldBuilder.newBuilder("testCol4", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType()).build())
        .addField(FieldBuilder.newBuilder("partition_name", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build())
        .build();

    @Test
    public void generateSqlIsNotNull() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of("testCol2", SortedRangeSet.of(false, Range.all(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType())));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE (`testCol2` IS NOT NULL)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = mySqlQueryStringBuilder.buildSql(connection, catalogName, tableName, schemaName, schema, constraints, splitBuilder.build());

        assertEquals(expectedPreparedStatement, preparedStatement);

    }

    @Test
    public void generateSqlIsNotEqual() throws Exception
    {
        Map<String, ValueSet> constraintsMap = ImmutableMap.of("testCol2", SortedRangeSet.of(false, Range.lessThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 138), Range.greaterThan(allocator, org.apache.arrow.vector.types.Types.MinorType.INT.getType(), 138)));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT);

        String expectedSql = "SELECT `testCol1`, `testCol2`, `testCol3`, `testCol4` FROM `testCatalog`.`testTable`.`testSchema` PARTITION(p0)  WHERE ((`testCol2` < ?) OR (`testCol2` > ?))";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = mySqlQueryStringBuilder.buildSql(connection, catalogName, tableName, schemaName, schema, constraints, splitBuilder.build());

        assertEquals(expectedPreparedStatement, preparedStatement);

    }
}
