/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class DDBTypeUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(DDBTypeUtilsTest.class);

    private String col1 = "col_1";
    private String col2 = "col_2";
    private Schema mapping;

    @Mock
    private DDBRecordMetadata ddbRecordMetadata;

    @Before
    public void setUp()
            throws IOException
    {
        ddbRecordMetadata = mock(DDBRecordMetadata.class);
    }


    @Test
    public void makeDecimalExtractorTest()
            throws Exception
    {
        logger.info("makeDecimalExtractorTest - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField(col1, ArrowType.Decimal.createDecimal(38, 18, 128))
                .addField(col2, ArrowType.Decimal.createDecimal(38, 18, 128))
                .build();

        String literalValue = "12345";
        String literalValue2 = "789.1234";

        AttributeValue myValue = new AttributeValue();
        myValue.setN(literalValue);
        AttributeValue myValue2 = new AttributeValue();
        myValue2.setN(literalValue2);

        Map<String, AttributeValue> testValue = ImmutableMap.of(col1, myValue, col2, myValue2);

        Map<String, Object> expectedResults = ImmutableMap.of(
                col1, new BigDecimal(literalValue),
                col2, new BigDecimal(literalValue2));
        Map<String, Object> extractedResults = testField(mapping, testValue);
        logger.info("makeDecimalExtractorTest - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);
        logger.info("makeDecimalExtractorTest - exit");
    }

    @Test
    public void makeVarBinaryExtractorTest()
            throws Exception
    {
        logger.info("makeVarBinaryExtractorTest - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField(col1, Types.MinorType.VARBINARY.getType())
                .addField(col2, Types.MinorType.VARBINARY.getType())
                .build();

        byte[] byteValue1 = "Hello".getBytes();
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(byteValue1);
        byte[] byteValue2 = "World!".getBytes();
        ByteBuffer byteBuffer2 = ByteBuffer.wrap(byteValue2);

        AttributeValue myValue = new AttributeValue();
        myValue.setB(byteBuffer1);
        AttributeValue myValue2 = new AttributeValue();
        myValue2.setB(byteBuffer2);

        Map<String, AttributeValue> testValue = ImmutableMap.of(col1, myValue, col2, myValue2);

        Map<String, Object> extractedResults = testField(mapping, testValue);
        assertEquals("Extracted results are not as expected!",
                new String(byteValue1),
                new String((byte[]) extractedResults.get(col1)));
        assertEquals("Extracted results are not as expected!",
                new String(byteValue2),
                new String((byte[]) extractedResults.get(col2)));
        logger.info("makeVarBinaryExtractorTest - exit");
    }

    @Test
    public void makeBitExtractorTest()
            throws Exception
    {
        logger.info("makeBitExtractorTest - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField(col1, Types.MinorType.BIT.getType())
                .addField(col2, Types.MinorType.BIT.getType())
                .build();

        AttributeValue myValue = new AttributeValue();
        myValue.setBOOL(true);
        AttributeValue myValue2 = new AttributeValue();
        myValue2.setBOOL(false);

        Map<String, AttributeValue> testValue = ImmutableMap.of(col1, myValue, col2, myValue2);

        Map<String, Object> expectedResults = ImmutableMap.of(
                col1, 1,
                col2, 0);
        Map<String, Object> extractedResults = testField(mapping, testValue);
        logger.info("makeBitExtractorTest - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);
        logger.info("makeBitExtractorTest - exit");
    }


    private Map<String, Object> testField(Schema mapping, Map<String, AttributeValue> values)
            throws Exception
    {
        Map<String, Object> results = new HashMap<>();
        for (Field field : mapping.getFields()) {
            Optional<Extractor> optionalExtractor = DDBTypeUtils.makeExtractor(field, ddbRecordMetadata, false);

            if (optionalExtractor.isPresent()) {
                Extractor extractor = optionalExtractor.get();
                if (extractor instanceof VarCharExtractor) {
                    NullableVarCharHolder holder = new NullableVarCharHolder();
                    ((VarCharExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
                else if (extractor instanceof VarBinaryExtractor) {
                    NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
                    ((VarBinaryExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
                else if (extractor instanceof DecimalExtractor) {
                    NullableDecimalHolder holder = new NullableDecimalHolder();
                    ((DecimalExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
                else if (extractor instanceof BitExtractor) {
                    NullableBitHolder holder = new NullableBitHolder();
                    ((BitExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
            }
            else {
                //generate field writer factor for complex data types.
                fail(String.format("Extractor not found for Type {}", field.getType()));
            }
        }
        return results;
    }
}
