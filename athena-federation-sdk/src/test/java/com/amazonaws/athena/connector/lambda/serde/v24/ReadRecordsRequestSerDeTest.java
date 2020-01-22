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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.utils.TestUtils;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ReadRecordsRequestSerDeTest
{
    private static final Logger logger = LoggerFactory.getLogger(ReadRecordsRequestSerDeTest.class);

    private TestUtils utils = new TestUtils();
    private JsonFactory jsonFactory = new JsonFactory();

    private V24SerDeProvider v24SerDeProvider = new V24SerDeProvider();
    private ReadRecordsRequestSerDe serde;

    private BlockAllocator allocator;

    private ReadRecordsRequest expected;
    private String expectedSerDeText;

    @Before
    public void before()
            throws IOException
    {
        allocator = new BlockAllocatorImpl("test-allocator-id");

        SchemaSerDe schemaSerDe = new SchemaSerDe();
        BlockSerDe blockSerDe = new BlockSerDe(allocator, jsonFactory, schemaSerDe);
        ArrowTypeSerDe arrowTypeSerDe = new ArrowTypeSerDe();
        MarkerSerDe markerSerDe = new MarkerSerDe(blockSerDe);
        RangeSerDe rangeSerDe = new RangeSerDe(markerSerDe);
        EquatableValueSetSerDe equatableValueSetSerDe = new EquatableValueSetSerDe(blockSerDe);
        SortedRangeSetSerDe sortedRangeSetSerDe = new SortedRangeSetSerDe(arrowTypeSerDe, rangeSerDe);
        AllOrNoneValueSetSerDe allOrNoneValueSetSerDe = new AllOrNoneValueSetSerDe(arrowTypeSerDe);
        ValueSetSerDe valueSetSerDe = new ValueSetSerDe(equatableValueSetSerDe, sortedRangeSetSerDe, allOrNoneValueSetSerDe);
        FederatedIdentitySerDe federatedIdentitySerDe = new FederatedIdentitySerDe();
        TableNameSerDe tableNameSerDe = new TableNameSerDe();
        ConstraintsSerDe constraintsSerDe = new ConstraintsSerDe(valueSetSerDe);
        S3SpillLocationSerDe s3SpillLocationSerDe = new S3SpillLocationSerDe();
        SpillLocationSerDe spillLocationSerDe = new SpillLocationSerDe(s3SpillLocationSerDe);
        EncryptionKeySerDe encryptionKeySerDe = new EncryptionKeySerDe();
        SplitSerDe splitSerDe = new SplitSerDe(spillLocationSerDe, encryptionKeySerDe);
        serde = new ReadRecordsRequestSerDe(federatedIdentitySerDe, tableNameSerDe, constraintsSerDe, schemaSerDe, splitSerDe);

        FederatedIdentity federatedIdentity = new FederatedIdentity("test-id", "test-principal", "0123456789");

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
        Constraints constraints = new Constraints(constraintsMap);

        Block partitions = allocator.createBlock(schema);
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


        String expectedSerDeFile = utils.getResourceOrFail("serde/v24", "ReadRecordsRequest.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void serialize()
            throws Exception
    {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonGenerator jgen = jsonFactory.createGenerator(outputStream);
        jgen.useDefaultPrettyPrinter();

        serde.serialize(jgen, expected);

        jgen.close();
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
        JsonParser jparser = jsonFactory.createParser(input);

        ReadRecordsRequest actual = (ReadRecordsRequest) serde.deserialize(jparser);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }

    @Test
    public void delegateSerialize()
            throws IOException
    {
        logger.info("delegateSerialize: enter");
        FederationRequestSerDe federationRequestSerDe = v24SerDeProvider.getFederationRequestSerDe(allocator);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        JsonGenerator jgen = jsonFactory.createGenerator(outputStream);
        jgen.useDefaultPrettyPrinter();

        federationRequestSerDe.serialize(jgen, expected);

        jgen.close();
        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("delegateSerialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);

        logger.info("delegateSerialize: exit");
    }

    @Test
    public void delegateDeserialize()
            throws IOException
    {
        logger.info("delegateDeserialize: enter");
        FederationRequestSerDe federationRequestSerDe = v24SerDeProvider.getFederationRequestSerDe(allocator);
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());
        JsonParser jparser = jsonFactory.createParser(input);

        ReadRecordsRequest actual = (ReadRecordsRequest) federationRequestSerDe.deserialize(jparser);

        logger.info("delegateDeserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("delegateDeserialize: exit");
    }
}
