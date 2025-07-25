/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v6;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDeTest;
import com.fasterxml.jackson.core.JsonEncoding;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;

public class ReadRecordsRequestSerDeV6Test extends TypedSerDeTest<FederationRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(ReadRecordsRequestSerDeV6Test.class);

    @Before
    public void beforeTest()
            throws IOException
    {
        String yearCol = "year";
        String monthCol = "month";
        String dayCol = "day";

        Schema schema = SchemaBuilder.newBuilder()
                .addField(yearCol, new ArrowType.Int(32, true))
                .addField(monthCol, new ArrowType.Int(32, true))
                .addField(dayCol, new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .addField("col3", Types.MinorType.FLOAT8.getType())
                .addField("col4", Types.MinorType.FLOAT8.getType())
                .addField("col5", Types.MinorType.FLOAT8.getType())
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), -10000D)), false));
        constraintsMap.put("col4", EquatableValueSet.newBuilder(allocator, Types.MinorType.FLOAT8.getType(), false, true).add(1.1D).build());
        constraintsMap.put("col5", new AllOrNoneValueSet(Types.MinorType.FLOAT8.getType(), false, true));

        HashMap<String, String> queryPassthroughArguments = new HashMap<>(1);
        queryPassthroughArguments.put("query", "SELECT * FROM DUMMY_TABLE;");

        QueryPlan queryPlan = new QueryPlan("1.0.0", "TestPlan", "TestConformance");

        Block partitions = allocator.createBlock(schema);

        FederationExpression federationExpression = new FunctionCallExpression(
                ArrowType.Bool.INSTANCE,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME.getFunctionName(),
                ImmutableList.of(new FunctionCallExpression(
                        Types.MinorType.FLOAT8.getType(),
                        StandardFunctions.ADD_FUNCTION_NAME.getFunctionName(),
                        ImmutableList.of(new VariableExpression("col3", Types.MinorType.FLOAT8.getType()),
                                new ConstantExpression(
                                        BlockUtils.newBlock(allocator, "col1", new ArrowType.Int(32, true), ImmutableList.of(10)),
                                        new ArrowType.Int(32, true)))),
                        new VariableExpression("col2", Types.MinorType.FLOAT8.getType())));

        List<OrderByField> orderByClause = ImmutableList.of(
            new OrderByField("col3", OrderByField.Direction.ASC_NULLS_FIRST),
            new OrderByField("col2", OrderByField.Direction.DESC_NULLS_FIRST)
        );

        Constraints constraints = new Constraints(constraintsMap, ImmutableList.of(federationExpression), orderByClause,
                DEFAULT_NO_LIMIT, queryPassthroughArguments, queryPlan);

        int num_partitions = 10;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(yearCol), i, 2016 + i);
            BlockUtils.setValue(partitions.getFieldVector(monthCol), i, (i % 12) + 1);
            BlockUtils.setValue(partitions.getFieldVector(dayCol), i, (i % 28) + 1);
        }
        partitions.setRowCount(num_partitions);

        SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket("athena-virtuoso-test")
                .withPrefix("lambda-spill")
                .withQueryId("test-query-id")
                .withSplitId("test-split-id")
                .withIsDirectory(true)
                .build();
        EncryptionKey encryptionKey = new EncryptionKey("test-key".getBytes(), "test-nonce".getBytes());
        Split split = Split.newBuilder(spillLocation, encryptionKey)
                .add("year", "2017")
                .add("month", "11")
                .add("day", "1")
                .build();

        expected = new ReadRecordsRequest(federatedIdentity,
                "test-query-id",
                "test-catalog",
                new TableName("test-schema", "test-table"),
                schema,
                split,
                constraints,
                100_000_000_000L,
                100_000_000_000L);

        federatedIdentity = new FederatedIdentity("testArn", "0123456789",
                Collections.emptyMap(), Collections.emptyList(), Map.of("test", "test"));

        String expectedSerDeFile = utils.getResourceOrFail("serde/v6", "ReadRecordsRequest.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    @Test
    public void serialize()
            throws Exception
    {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        mapperV6.writeValue(outputStream, expected);

        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("serialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);
        expected.close();

        logger.info("serialize: exit");
    }

    @Test
    public void deserialize()
            throws IOException
    {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        ReadRecordsRequest actual = (ReadRecordsRequest) mapperV6.readValue(input, FederationRequest.class);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }
}
