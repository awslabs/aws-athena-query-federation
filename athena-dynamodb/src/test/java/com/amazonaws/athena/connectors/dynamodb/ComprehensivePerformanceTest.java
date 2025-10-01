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

import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DirectDDBExtractors;
import com.amazonaws.athena.connectors.dynamodb.util.OptimizedDecimalExtractor;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.Types;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ComprehensivePerformanceTest
{
    private static final int RECORD_COUNT = 50000;
    
    @Test
    public void compareOptimalApproaches()
    {
        Map<String, AttributeValue>[] testData = createTestData();
        
        System.out.println("=== Decimal Performance Comparison: " + RECORD_COUNT + " records ===\n");
        
        // 1. Original JSON approach (baseline)
        long jsonTime = benchmarkJsonApproach(testData);
        
        // 2. Direct Extractors approach
        long extractorTime = benchmarkDirectExtractors(testData);
        
        // 3. Optimized Decimal Extractors approach
        long optimizedTime = benchmarkOptimizedDecimalExtractors(testData);
        
        // Results
        System.out.println("\n=== RESULTS ===");
        System.out.println("JSON Serialization (baseline):     " + (jsonTime / 1_000_000) + "ms");
        System.out.println("Direct Extractors:                 " + (extractorTime / 1_000_000) + "ms");
        System.out.println("Optimized Decimal Extractors:      " + (optimizedTime / 1_000_000) + "ms");
        
        System.out.println("\n=== PERFORMANCE IMPROVEMENTS ===");
        System.out.println("Direct Extractors vs JSON:         " + String.format("%.2fx faster", (double) jsonTime / extractorTime));
        System.out.println("Optimized Decimals vs JSON:        " + String.format("%.2fx faster", (double) jsonTime / optimizedTime));
        System.out.println("Optimized vs Direct Extractors:    " + String.format("%.2fx faster", (double) extractorTime / optimizedTime));
        
        System.out.println("\n=== DECIMAL OPTIMIZATION IMPACT ===");
        System.out.println("Additional speedup from decimal optimization: " + String.format("%.2fx", (double) extractorTime / optimizedTime));
        System.out.println("Time saved by decimal optimization: " + ((extractorTime - optimizedTime) / 1_000_000) + "ms");
    }
    
    private long benchmarkJsonApproach(Map<String, AttributeValue>[] testData)
    {
        System.out.println("Benchmarking JSON Serialization approach...");
        long start = System.nanoTime();
        
        for (Map<String, AttributeValue> item : testData) {
            for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                String json = DDBTypeUtils.attributeToJson(entry.getValue(), entry.getKey());
                DDBTypeUtils.jsonToAttributeValue(json, entry.getKey());
            }
        }
        
        return System.nanoTime() - start;
    }
    
    private long benchmarkDirectExtractors(Map<String, AttributeValue>[] testData)
    {
        System.out.println("Benchmarking Direct Extractors approach...");
        
        // Pre-create extractors
        DirectDDBExtractors.DirectDecimalExtractor priceExtractor = new DirectDDBExtractors.DirectDecimalExtractor("price");
        DirectDDBExtractors.DirectDecimalExtractor costExtractor = new DirectDDBExtractors.DirectDecimalExtractor("cost");
        DirectDDBExtractors.DirectDecimalExtractor marginExtractor = new DirectDDBExtractors.DirectDecimalExtractor("margin");
        DirectDDBExtractors.DirectDecimalExtractor discountExtractor = new DirectDDBExtractors.DirectDecimalExtractor("discount");
        DirectDDBExtractors.DirectDecimalExtractor totalExtractor = new DirectDDBExtractors.DirectDecimalExtractor("total");
        
        // Pre-create holders
        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder decimalHolder = new com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder();
        
        long start = System.nanoTime();
        
        for (Map<String, AttributeValue> item : testData) {
            try {
                // Test 5 decimal fields to show cumulative impact
                priceExtractor.extract(item, decimalHolder);
                costExtractor.extract(item, decimalHolder);
                marginExtractor.extract(item, decimalHolder);
                discountExtractor.extract(item, decimalHolder);
                totalExtractor.extract(item, decimalHolder);
            }
            catch (Exception e) {
                // Ignore for benchmark
            }
        }
        
        return System.nanoTime() - start;
    }
    
    private long benchmarkOptimizedDecimalExtractors(Map<String, AttributeValue>[] testData)
    {
        System.out.println("Benchmarking Optimized Decimal Extractors approach...");
        
        // Pre-create optimized extractors
        OptimizedDecimalExtractor priceExtractor = new OptimizedDecimalExtractor("price", 9);
        OptimizedDecimalExtractor costExtractor = new OptimizedDecimalExtractor("cost", 9);
        OptimizedDecimalExtractor marginExtractor = new OptimizedDecimalExtractor("margin", 9);
        OptimizedDecimalExtractor discountExtractor = new OptimizedDecimalExtractor("discount", 9);
        OptimizedDecimalExtractor totalExtractor = new OptimizedDecimalExtractor("total", 9);
        
        // Pre-create holders
        com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder decimalHolder = new com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder();
        
        long start = System.nanoTime();
        
        for (Map<String, AttributeValue> item : testData) {
            try {
                // Test same 5 decimal fields with optimized extractors
                priceExtractor.extract(item, decimalHolder);
                costExtractor.extract(item, decimalHolder);
                marginExtractor.extract(item, decimalHolder);
                discountExtractor.extract(item, decimalHolder);
                totalExtractor.extract(item, decimalHolder);
            }
            catch (Exception e) {
                // Ignore for benchmark
            }
        }
        
        return System.nanoTime() - start;
    }
    

    
    @SuppressWarnings("unchecked")
    private Map<String, AttributeValue>[] createTestData()
    {
        Map<String, AttributeValue>[] data = new Map[RECORD_COUNT];
        
        for (int i = 0; i < RECORD_COUNT; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            
            // 10 String fields
            item.put("id", AttributeValue.builder().s("order_" + i).build());
            item.put("name", AttributeValue.builder().s("Product Name " + (i % 1000)).build());
            item.put("email", AttributeValue.builder().s("user" + (i % 5000) + "@example.com").build());
            item.put("category", AttributeValue.builder().s("Category_" + (i % 50)).build());
            item.put("description", AttributeValue.builder().s("Description for product " + (i % 100)).build());
            item.put("brand", AttributeValue.builder().s("Brand_" + (i % 20)).build());
            item.put("model", AttributeValue.builder().s("Model_" + (i % 500)).build());
            item.put("sku", AttributeValue.builder().s("SKU_" + i).build());
            item.put("status", AttributeValue.builder().s("Status_" + (i % 5)).build());
            item.put("region", AttributeValue.builder().s("Region_" + (i % 10)).build());
            
            // 20 Decimal fields (heavy JSON parsing - this is where performance matters)
            item.put("price", AttributeValue.builder().n(String.valueOf(99.99 + (i * 0.01))).build());
            item.put("cost", AttributeValue.builder().n(String.valueOf(50.00 + (i * 0.005))).build());
            item.put("margin", AttributeValue.builder().n(String.valueOf(49.99 + (i * 0.005))).build());
            item.put("discount", AttributeValue.builder().n(String.valueOf((i % 20) * 0.05)).build());
            item.put("tax_rate", AttributeValue.builder().n(String.valueOf(0.08 + (i % 10) * 0.001)).build());
            item.put("shipping_cost", AttributeValue.builder().n(String.valueOf(5.99 + (i % 30) * 0.1)).build());
            item.put("handling_fee", AttributeValue.builder().n(String.valueOf(2.50 + (i % 10) * 0.05)).build());
            item.put("insurance", AttributeValue.builder().n(String.valueOf(1.99 + (i % 5) * 0.1)).build());
            item.put("total", AttributeValue.builder().n(String.valueOf(150.00 + (i * 0.02))).build());
            item.put("subtotal", AttributeValue.builder().n(String.valueOf(140.00 + (i * 0.02))).build());
            item.put("weight", AttributeValue.builder().n(String.valueOf(1.5 + (i % 100) * 0.01)).build());
            item.put("length", AttributeValue.builder().n(String.valueOf(10.0 + (i % 50) * 0.1)).build());
            item.put("width", AttributeValue.builder().n(String.valueOf(8.0 + (i % 40) * 0.1)).build());
            item.put("height", AttributeValue.builder().n(String.valueOf(6.0 + (i % 30) * 0.1)).build());
            item.put("volume", AttributeValue.builder().n(String.valueOf(480.0 + (i % 200) * 0.5)).build());
            item.put("rating", AttributeValue.builder().n(String.valueOf(1.0 + (i % 50) * 0.08)).build());
            item.put("commission", AttributeValue.builder().n(String.valueOf(5.0 + (i % 20) * 0.25)).build());
            item.put("bonus", AttributeValue.builder().n(String.valueOf((i % 100) * 0.1)).build());
            item.put("penalty", AttributeValue.builder().n(String.valueOf((i % 50) * 0.05)).build());
            item.put("adjustment", AttributeValue.builder().n(String.valueOf(-5.0 + (i % 100) * 0.1)).build());
            
            data[i] = item;
        }
        
        return data;
    }
    

}