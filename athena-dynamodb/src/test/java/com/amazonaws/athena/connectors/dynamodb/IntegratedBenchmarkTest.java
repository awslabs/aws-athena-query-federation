/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connectors.dynamodb.util.DirectDDBExtractors;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IntegratedBenchmarkTest
{
    private static final int RECORD_COUNT = 10000;
    
    @Test
    public void benchmarkEndToEndPerformance()
    {
        Schema schema = createTestSchema();
        
        // Test old approach (JSON + GeneratedRowWriter)
        long oldStart = System.nanoTime();
        processWithOldApproach(schema);
        long oldTime = System.nanoTime() - oldStart;
        
        // Test new approach (Direct Extractors)
        long newStart = System.nanoTime();
        processWithNewApproach(schema);
        long newTime = System.nanoTime() - newStart;
        
        System.out.println("Old approach (JSON): " + (oldTime / 1_000_000) + "ms");
        System.out.println("New approach (Direct Extractors): " + (newTime / 1_000_000) + "ms");
        System.out.println("Performance improvement: " + ((double) oldTime / newTime) + "x");
    }
    
    private Schema createTestSchema()
    {
        return new Schema(Arrays.asList(
            new Field("id", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("amount", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("active", FieldType.nullable(Types.MinorType.BIT.getType()), null)
        ));
    }
    
    private void processWithOldApproach(Schema schema)
    {
        try (RootAllocator allocator = new RootAllocator();
             BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl(allocator)) {
            
            try (Block block = blockAllocator.createBlock(schema)) {
                for (int i = 0; i < RECORD_COUNT; i++) {
                    Map<String, AttributeValue> item = createTestItem(i);
                    
                    // Simulate old JSON conversion approach
                    for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                        String json = DDBTypeUtils.attributeToJson(entry.getValue(), entry.getKey());
                        DDBTypeUtils.jsonToAttributeValue(json, entry.getKey());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private void processWithNewApproach(Schema schema)
    {
        try (RootAllocator allocator = new RootAllocator();
             BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl(allocator)) {
            
            try (Block block = blockAllocator.createBlock(schema)) {
                for (int i = 0; i < RECORD_COUNT; i++) {
                    Map<String, AttributeValue> item = createTestItem(i);
                    
                    // Use new direct extractor approach
                    DirectDDBExtractors.DirectVarCharExtractor varCharExtractor = new DirectDDBExtractors.DirectVarCharExtractor("id");
                    DirectDDBExtractors.DirectDecimalExtractor decimalExtractor = new DirectDDBExtractors.DirectDecimalExtractor("amount");
                    DirectDDBExtractors.DirectBitExtractor bitExtractor = new DirectDDBExtractors.DirectBitExtractor("active");
                    
                    try {
                        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder varCharHolder = new com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder();
                        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder decimalHolder = new com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder();
                        org.apache.arrow.vector.holders.NullableBitHolder bitHolder = new org.apache.arrow.vector.holders.NullableBitHolder();
                        
                        varCharExtractor.extract(item, varCharHolder);
                        decimalExtractor.extract(item, decimalHolder);
                        bitExtractor.extract(item, bitHolder);
                    }
                    catch (Exception e) {
                        // Ignore for benchmark
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private Map<String, AttributeValue> createTestItem(int index)
    {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("record_" + index).build());
        item.put("amount", AttributeValue.builder().n(String.valueOf(index * 10.5)).build());
        item.put("active", AttributeValue.builder().bool(index % 2 == 0).build());
        return item;
    }
    
    private ArrowType getArrowType(AttributeValue value)
    {
        if (value.s() != null) return Types.MinorType.VARCHAR.getType();
        if (value.n() != null) return new ArrowType.Decimal(38, 9);
        if (value.bool() != null) return Types.MinorType.BIT.getType();
        return Types.MinorType.VARCHAR.getType();
    }
}