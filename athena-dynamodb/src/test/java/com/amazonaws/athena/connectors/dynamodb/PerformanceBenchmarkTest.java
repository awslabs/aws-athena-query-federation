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

import com.amazonaws.athena.connectors.dynamodb.util.DirectDDBExtractors;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.Types;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

public class PerformanceBenchmarkTest
{
    private static final int ITERATIONS = 100000;
    
    @Test
    public void benchmarkDirectArrowVsJson()
    {
        // Setup test data
        Map<String, AttributeValue> testItem = createTestItem();
        
        // Benchmark JSON approach
        long jsonStart = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            benchmarkJsonConversion(testItem);
        }
        long jsonTime = System.nanoTime() - jsonStart;
        
        // Benchmark direct extractor approach
        long directStart = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            benchmarkDirectExtractorConversion(testItem);
        }
        long directTime = System.nanoTime() - directStart;
        
        System.out.println("JSON approach: " + (jsonTime / 1_000_000) + "ms");
        System.out.println("Direct Extractor approach: " + (directTime / 1_000_000) + "ms");
        System.out.println("CPU Performance Improvement: " + ((double) jsonTime / directTime) + "x");
    }
    
    @Test
    public void benchmarkMemoryUsage()
    {
        Runtime runtime = Runtime.getRuntime();
        
        // Test JSON memory usage
        runtime.gc();
        long memBefore = runtime.totalMemory() - runtime.freeMemory();
        
        for (int i = 0; i < ITERATIONS; i++) {
            Map<String, AttributeValue> item = createTestItem();
            String json = DDBTypeUtils.attributeToJson(item.get("stringField"), "stringField");
        }
        
        long jsonMemory = runtime.totalMemory() - runtime.freeMemory() - memBefore;
        
        // Test direct Arrow memory usage
        runtime.gc();
        memBefore = runtime.totalMemory() - runtime.freeMemory();
        
        try (RootAllocator allocator = new RootAllocator();
             VarCharVector vector = new VarCharVector("test", allocator)) {
            vector.allocateNew();
            
            for (int i = 0; i < ITERATIONS; i++) {
                Map<String, AttributeValue> item = createTestItem();
                AttributeValue value = item.get("stringField");
                if (value.s() != null) {
                    vector.setSafe(i % 1000, value.s().getBytes());
                }
            }
        }
        
        long directMemory = runtime.totalMemory() - runtime.freeMemory() - memBefore;
        
        System.out.println("JSON memory usage: " + (jsonMemory / 1024) + "KB");
        System.out.println("Direct Extractor memory usage: " + (directMemory / 1024) + "KB");
        System.out.println("Memory reduction: " + ((double) jsonMemory / directMemory) + "x");
    }
    
    private Map<String, AttributeValue> createTestItem()
    {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("stringField", AttributeValue.builder().s("test_string_value").build());
        item.put("numberField", AttributeValue.builder().n("123.45").build());
        item.put("boolField", AttributeValue.builder().bool(true).build());
        return item;
    }
    
    private void benchmarkJsonConversion(Map<String, AttributeValue> item)
    {
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            String json = DDBTypeUtils.attributeToJson(entry.getValue(), entry.getKey());
            DDBTypeUtils.jsonToAttributeValue(json, entry.getKey());
        }
    }
    
    private void benchmarkDirectExtractorConversion(Map<String, AttributeValue> item)
    {
        // Test direct extractors
        DirectDDBExtractors.DirectVarCharExtractor varCharExtractor = new DirectDDBExtractors.DirectVarCharExtractor("stringField");
        DirectDDBExtractors.DirectDecimalExtractor decimalExtractor = new DirectDDBExtractors.DirectDecimalExtractor("numberField");
        DirectDDBExtractors.DirectBitExtractor bitExtractor = new DirectDDBExtractors.DirectBitExtractor("boolField");
        
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
    
    private ArrowType getArrowType(AttributeValue value)
    {
        if (value.s() != null) return Types.MinorType.VARCHAR.getType();
        if (value.n() != null) return new ArrowType.Decimal(38, 9);
        if (value.bool() != null) return Types.MinorType.BIT.getType();
        return Types.MinorType.VARCHAR.getType();
    }
}