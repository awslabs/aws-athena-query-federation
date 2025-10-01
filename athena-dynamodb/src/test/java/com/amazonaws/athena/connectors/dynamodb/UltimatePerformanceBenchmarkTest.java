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
import com.amazonaws.athena.connectors.dynamodb.util.Java17OptimizedDecimalExtractor;
import com.amazonaws.athena.connectors.dynamodb.util.UltraOptimizedDecimalExtractor;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;

public class UltimatePerformanceBenchmarkTest
{
    private static final int RECORD_COUNT = 1000000;
    private static final int WARMUP_ITERATIONS = 5;
    private static final int BENCHMARK_ITERATIONS = 10;
    
    @Test
    public void ultimateDecimalPerformanceTest()
    {
        Map<String, AttributeValue>[] testData = createRealisticTestData();
        
        System.out.println("=== ULTIMATE DECIMAL PERFORMANCE BENCHMARK ===");
        System.out.println("Records: " + RECORD_COUNT);
        System.out.println("Warmup iterations: " + WARMUP_ITERATIONS);
        System.out.println("Benchmark iterations: " + BENCHMARK_ITERATIONS);
        System.out.println();
        
        // Warmup JVM
        System.out.println("Warming up JVM...");
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            benchmarkAllApproaches(testData, false);
        }
        
        System.out.println("Running benchmarks...");
        
        // Run actual benchmarks
        long[] jsonTimes = new long[BENCHMARK_ITERATIONS];
        long[] basicDirectTimes = new long[BENCHMARK_ITERATIONS];
        long[] optimizedTimes = new long[BENCHMARK_ITERATIONS];
        long[] java17Times = new long[BENCHMARK_ITERATIONS];
        long[] ultraTimes = new long[BENCHMARK_ITERATIONS];
        
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            System.out.println("Iteration " + (i + 1) + "/" + BENCHMARK_ITERATIONS);
            
            jsonTimes[i] = benchmarkJsonApproach(testData);
            basicDirectTimes[i] = benchmarkBasicDirectExtractors(testData);
            optimizedTimes[i] = benchmarkOptimizedExtractors(testData);
            java17Times[i] = benchmarkJava17Extractors(testData);
            ultraTimes[i] = benchmarkUltraOptimizedExtractors(testData);
            
            // Force GC between iterations
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Calculate statistics
        printResults("JSON Serialization", jsonTimes);
        printResults("Basic Direct Extractors", basicDirectTimes);
        printResults("Optimized Extractors", optimizedTimes);
        printResults("Java 17 Extractors", java17Times);
        printResults("Ultra Optimized Extractors", ultraTimes);
        
        // Performance comparisons
        double avgJson = average(jsonTimes);
        double avgBasic = average(basicDirectTimes);
        double avgOptimized = average(optimizedTimes);
        double avgJava17 = average(java17Times);
        double avgUltra = average(ultraTimes);
        
        System.out.println("\n=== PERFORMANCE IMPROVEMENTS ===");
        System.out.printf("Basic Direct vs JSON:      %.2fx faster%n", avgJson / avgBasic);
        System.out.printf("Optimized vs JSON:         %.2fx faster%n", avgJson / avgOptimized);
        System.out.printf("Java 17 vs JSON:           %.2fx faster%n", avgJson / avgJava17);
        System.out.printf("Ultra Optimized vs JSON:   %.2fx faster%n", avgJson / avgUltra);
        
        System.out.println("\n=== INCREMENTAL IMPROVEMENTS ===");
        System.out.printf("Optimized vs Basic:        %.2fx faster%n", avgBasic / avgOptimized);
        System.out.printf("Java 17 vs Optimized:      %.2fx faster%n", avgOptimized / avgJava17);
        System.out.printf("Ultra vs Java 17:          %.2fx faster%n", avgJava17 / avgUltra);
        
        System.out.println("\n=== WINNER ===");
        double bestTime = Math.min(Math.min(Math.min(avgJson, avgBasic), Math.min(avgOptimized, avgJava17)), avgUltra);
        String winner = bestTime == avgUltra ? "Ultra Optimized" :
                       bestTime == avgJava17 ? "Java 17" :
                       bestTime == avgOptimized ? "Optimized" :
                       bestTime == avgBasic ? "Basic Direct" : "JSON";
        System.out.printf("Best approach: %s (%.2f ms average)%n", winner, bestTime / 1_000_000);
    }
    
    private void benchmarkAllApproaches(Map<String, AttributeValue>[] testData, boolean print)
    {
        benchmarkJsonApproach(testData);
        benchmarkBasicDirectExtractors(testData);
        benchmarkOptimizedExtractors(testData);
        benchmarkJava17Extractors(testData);
        benchmarkUltraOptimizedExtractors(testData);
    }
    
    private long benchmarkJsonApproach(Map<String, AttributeValue>[] testData)
    {
        Schema schema = createRealisticSchema();
        return profileWithExtractorFramework(schema, testData, false); // false = use JSON extractors
    }
    
    private long benchmarkBasicDirectExtractors(Map<String, AttributeValue>[] testData)
    {
        Schema schema = createRealisticSchema();
        return profileWithDirectExtractors(schema, testData);
    }
    
    private long benchmarkOptimizedExtractors(Map<String, AttributeValue>[] testData)
    {
        Schema schema = createRealisticSchema();
        return profileWithExtractorFramework(schema, testData, true); // true = use optimized extractors
    }
    
    private long benchmarkJava17Extractors(Map<String, AttributeValue>[] testData)
    {
        Schema schema = createRealisticSchema();

        long totalTime = 0;
        int batchSize = 1000;

        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator();
             com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl blockAllocator = new com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl(allocator)) {

            for (int batch = 0; batch < testData.length; batch += batchSize) {
                int batchEnd = Math.min(batch + batchSize, testData.length);

                long batchStart = System.nanoTime();

                try (com.amazonaws.athena.connector.lambda.data.Block block = blockAllocator.createBlock(schema)) {
                    com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata recordMetadata = new com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata(schema);
                    com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver resolver = new com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver(recordMetadata);
                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder builder = com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.newBuilder(new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(new HashMap<>(), java.util.Collections.emptyList(), java.util.Collections.emptyList(), com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT, java.util.Collections.emptyMap(), null));

                    for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
                        org.apache.arrow.vector.types.Types.MinorType fieldType = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(field.getType());
                        String fieldName = field.getName();

                        switch (fieldType) {
                            case VARCHAR:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectVarCharExtractor(fieldName));
                                break;
                            case DECIMAL:
                                builder.withExtractor(fieldName, new Java17OptimizedDecimalExtractor(fieldName, 38));
                                break;
                            case BIT:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectBitExtractor(fieldName));
                                break;
                            default:
                                builder.withFieldWriterFactory(fieldName,
                                        com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils.makeFactory(field, recordMetadata, resolver, false));
                                break;
                        }
                    }

                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter rowWriter = builder.build();

                    for (int i = batch; i < batchEnd; i++) {
                        try {
                            rowWriter.writeRow(block, i - batch, testData[i]);
                        } catch (Exception e) {
                            // Continue
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                totalTime += (System.nanoTime() - batchStart);
            }
        }

        return totalTime;
    }
    
    private long benchmarkUltraOptimizedExtractors(Map<String, AttributeValue>[] testData)
    {
        Schema schema = createRealisticSchema();

        long totalTime = 0;
        int batchSize = 1000;

        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator();
             com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl blockAllocator = new com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl(allocator)) {

            for (int batch = 0; batch < testData.length; batch += batchSize) {
                int batchEnd = Math.min(batch + batchSize, testData.length);

                long batchStart = System.nanoTime();

                try (com.amazonaws.athena.connector.lambda.data.Block block = blockAllocator.createBlock(schema)) {
                    com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata recordMetadata = new com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata(schema);
                    com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver resolver = new com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver(recordMetadata);
                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder builder = com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.newBuilder(new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(new HashMap<>(), java.util.Collections.emptyList(), java.util.Collections.emptyList(), com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT, java.util.Collections.emptyMap(), null));

                    for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
                        org.apache.arrow.vector.types.Types.MinorType fieldType = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(field.getType());
                        String fieldName = field.getName();

                        switch (fieldType) {
                            case VARCHAR:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectVarCharExtractor(fieldName));
                                break;
                            case DECIMAL:
                                builder.withExtractor(fieldName, new UltraOptimizedDecimalExtractor(fieldName, 38));
                                break;
                            case BIT:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectBitExtractor(fieldName));
                                break;
                            default:
                                builder.withFieldWriterFactory(fieldName,
                                        com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils.makeFactory(field, recordMetadata, resolver, false));
                                break;
                        }
                    }

                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter rowWriter = builder.build();

                    for (int i = batch; i < batchEnd; i++) {
                        try {
                            rowWriter.writeRow(block, i - batch, testData[i]);
                        } catch (Exception e) {
                            // Continue
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                totalTime += (System.nanoTime() - batchStart);
            }
        }

        return totalTime;
    }
    
    // Realistic profiling methods from RealisticProfilingTest
    private long profileWithExtractorFramework(Schema schema, Map<String, AttributeValue>[] testData, boolean useOptimized)
    {
        long totalTime = 0;
        int batchSize = 1000;
        
        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator();
             com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl blockAllocator = new com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl(allocator)) {
            
            for (int batch = 0; batch < testData.length; batch += batchSize) {
                int batchEnd = Math.min(batch + batchSize, testData.length);
                
                long batchStart = System.nanoTime();
                
                try (com.amazonaws.athena.connector.lambda.data.Block block = blockAllocator.createBlock(schema)) {
                    com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata recordMetadata = new com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata(schema);
                    com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver resolver = new com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver(recordMetadata);
                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder builder = com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.newBuilder(new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(new HashMap<>(), java.util.Collections.emptyList(), java.util.Collections.emptyList(), com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT, java.util.Collections.emptyMap(), null));
                    
                    for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
                        java.util.Optional<com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor> extractor = 
                            useOptimized ? com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils.makeDirectExtractor(field, recordMetadata, false) :
                                         com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils.makeExtractor(field, recordMetadata, false);
                        if (extractor.isPresent()) {
                            builder.withExtractor(field.getName(), extractor.get());
                        } else {
                            builder.withFieldWriterFactory(field.getName(), 
                                com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils.makeFactory(field, recordMetadata, resolver, false));
                        }
                    }
                    
                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter rowWriter = builder.build();
                    
                    for (int i = batch; i < batchEnd; i++) {
                        try {
                            rowWriter.writeRow(block, i - batch, testData[i]);
                        } catch (Exception e) {
                            // Continue
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                
                totalTime += (System.nanoTime() - batchStart);
            }
        }
        
        return totalTime;
    }
    
    private long profileWithDirectExtractors(Schema schema, Map<String, AttributeValue>[] testData)
    {
        long totalTime = 0;
        int batchSize = 1000;
        
        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator();
             com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl blockAllocator = new com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl(allocator)) {
            
            for (int batch = 0; batch < testData.length; batch += batchSize) {
                int batchEnd = Math.min(batch + batchSize, testData.length);
                
                long batchStart = System.nanoTime();
                
                try (com.amazonaws.athena.connector.lambda.data.Block block = blockAllocator.createBlock(schema)) {
                    com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata recordMetadata = new com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata(schema);
                    com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver resolver = new com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver(recordMetadata);
                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder builder = com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.newBuilder(new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(new HashMap<>(), java.util.Collections.emptyList(), java.util.Collections.emptyList(), com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT, java.util.Collections.emptyMap(), null));
                    
                    for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
                        org.apache.arrow.vector.types.Types.MinorType fieldType = org.apache.arrow.vector.types.Types.getMinorTypeForArrowType(field.getType());
                        String fieldName = field.getName();
                        
                        switch (fieldType) {
                            case VARCHAR:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectVarCharExtractor(fieldName));
                                break;
                            case DECIMAL:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectDecimalExtractor(fieldName));
                                break;
                            case BIT:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectBitExtractor(fieldName));
                                break;
                            default:
                                builder.withFieldWriterFactory(fieldName, 
                                    com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils.makeFactory(field, recordMetadata, resolver, false));
                                break;
                        }
                    }
                    
                    com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter rowWriter = builder.build();
                    
                    for (int i = batch; i < batchEnd; i++) {
                        try {
                            rowWriter.writeRow(block, i - batch, testData[i]);
                        } catch (Exception e) {
                            // Continue
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                
                totalTime += (System.nanoTime() - batchStart);
            }
        }
        
        return totalTime;
    }
    
    private org.apache.arrow.vector.types.pojo.Schema createRealisticSchema()
    {
        return new org.apache.arrow.vector.types.pojo.Schema(java.util.Arrays.asList(
            new org.apache.arrow.vector.types.pojo.Field("id", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("name", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("email", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("category", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("description", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("brand", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("model", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("sku", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("status", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("region", org.apache.arrow.vector.types.pojo.FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()), null),
            new org.apache.arrow.vector.types.pojo.Field("price", org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(38, 9)), null),
            new org.apache.arrow.vector.types.pojo.Field("cost", org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(38, 9)), null),
            new org.apache.arrow.vector.types.pojo.Field("margin", org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(38, 9)), null),
            new org.apache.arrow.vector.types.pojo.Field("discount", org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(38, 9)), null),
            new org.apache.arrow.vector.types.pojo.Field("total", org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(38, 9)), null)
        ));
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, AttributeValue>[] createRealisticTestData()
    {
        Map<String, AttributeValue>[] data = new Map[RECORD_COUNT];
        
        for (int i = 0; i < RECORD_COUNT; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            
            // 10 String fields
            item.put("id", AttributeValue.builder().s("order_" + i).build());
            item.put("name", AttributeValue.builder().s("Product_" + (i % 1000)).build());
            item.put("email", AttributeValue.builder().s("user" + (i % 5000) + "@test.com").build());
            item.put("category", AttributeValue.builder().s("Cat_" + (i % 50)).build());
            item.put("description", AttributeValue.builder().s("Desc_" + (i % 100)).build());
            item.put("brand", AttributeValue.builder().s("Brand_" + (i % 20)).build());
            item.put("model", AttributeValue.builder().s("Model_" + (i % 500)).build());
            item.put("sku", AttributeValue.builder().s("SKU_" + i).build());
            item.put("status", AttributeValue.builder().s("Status_" + (i % 5)).build());
            item.put("region", AttributeValue.builder().s("Region_" + (i % 10)).build());
            
            // 20 Decimal fields
            item.put("price", AttributeValue.builder().n(String.valueOf(99 + (i % 100))).build());
            item.put("cost", AttributeValue.builder().n(String.valueOf(50.99 + (i % 50) * 0.01)).build());
            item.put("margin", AttributeValue.builder().n(String.valueOf(i % 10)).build());
            item.put("discount", AttributeValue.builder().n(String.valueOf((i % 20) * 0.05)).build());
            item.put("total", AttributeValue.builder().n(String.valueOf(1000.123456 + i * 0.001)).build());
            item.put("subtotal", AttributeValue.builder().n(String.valueOf(900 + (i % 200))).build());
            item.put("tax_rate", AttributeValue.builder().n(String.valueOf(0.08 + (i % 10) * 0.001)).build());
            item.put("shipping", AttributeValue.builder().n(String.valueOf(5.99 + (i % 30) * 0.1)).build());
            item.put("handling", AttributeValue.builder().n(String.valueOf(2.50 + (i % 10) * 0.05)).build());
            item.put("insurance", AttributeValue.builder().n(String.valueOf(1.99 + (i % 5) * 0.1)).build());
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
    
    private void printResults(String name, long[] times)
    {
        double avg = average(times);
        double min = minimum(times);
        double max = maximum(times);
        double stddev = standardDeviation(times);
        
        System.out.printf("%-25s: avg=%.2fms, min=%.2fms, max=%.2fms, stddev=%.2fms%n", 
            name, avg / 1_000_000, min / 1_000_000, max / 1_000_000, stddev / 1_000_000);
    }
    
    private double average(long[] values)
    {
        return java.util.Arrays.stream(values).average().orElse(0.0);
    }
    
    private double minimum(long[] values)
    {
        return java.util.Arrays.stream(values).min().orElse(0);
    }
    
    private double maximum(long[] values)
    {
        return java.util.Arrays.stream(values).max().orElse(0);
    }
    
    private double standardDeviation(long[] values)
    {
        double avg = average(values);
        double variance = java.util.Arrays.stream(values)
            .mapToDouble(x -> Math.pow(x - avg, 2))
            .average()
            .orElse(0.0);
        return Math.sqrt(variance);
    }
}