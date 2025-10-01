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
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DirectDDBExtractors;
import com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;

public class RealisticProfilingTest
{
    private static final int RECORD_COUNT = 1000000;
    private static final int BATCH_SIZE = 1000;
    
    @Test
    public void profileRealWorldUsage()
    {
        Schema schema = createSchema();
        Map<String, AttributeValue>[] testData = createTestData();
        
        System.out.println("=== Realistic DynamoDB Connector Profiling ===");
        System.out.println("Records: " + RECORD_COUNT + ", Batch size: " + BATCH_SIZE + "\n");
        
        // Test current approach (JSON + GeneratedRowWriter)
        long currentTime = profileCurrentApproach(schema, testData);
        
        // Test direct extractors approach
        long directTime = profileDirectExtractorsApproach(schema, testData);
        
        // Test optimized decimal extractors approach
        long optimizedTime = profileOptimizedDecimalApproach(schema, testData);
        
        System.out.println("\n=== PROFILING RESULTS ===");
        System.out.println("Current approach (JSON):           " + (currentTime / 1_000_000) + "ms");
        System.out.println("Direct Extractors:                 " + (directTime / 1_000_000) + "ms");
        System.out.println("Optimized Decimal Extractors:      " + (optimizedTime / 1_000_000) + "ms");
        
        System.out.println("\n=== PERFORMANCE IMPROVEMENTS ===");
        System.out.println("Direct Extractors vs JSON:         " + String.format("%.2fx", (double) currentTime / directTime));
        System.out.println("Optimized Decimals vs JSON:        " + String.format("%.2fx", (double) currentTime / optimizedTime));
        System.out.println("Optimized vs Direct Extractors:    " + String.format("%.2fx", (double) directTime / optimizedTime));
        
        System.out.println("\n=== DECIMAL OPTIMIZATION IMPACT ===");
        System.out.println("Additional speedup from decimal optimization: " + String.format("%.2fx", (double) directTime / optimizedTime));
        System.out.println("Time saved by decimal optimization: " + ((directTime - optimizedTime) / 1_000_000) + "ms");
    }
    
    private long profileCurrentApproach(Schema schema, Map<String, AttributeValue>[] testData)
    {
        System.out.println("Profiling current approach (JSON serialization)...");
        
        long totalTime = 0;
        
        try (RootAllocator allocator = new RootAllocator();
             BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl(allocator)) {
            
            // Process in batches like real connector
            for (int batch = 0; batch < RECORD_COUNT; batch += BATCH_SIZE) {
                int batchEnd = Math.min(batch + BATCH_SIZE, RECORD_COUNT);
                
                long batchStart = System.nanoTime();
                
                try (Block block = blockAllocator.createBlock(schema)) {
                    // Build row writer with JSON approach
                    DDBRecordMetadata recordMetadata = new DDBRecordMetadata(schema);
                    DynamoDBFieldResolver resolver = new DynamoDBFieldResolver(recordMetadata);
                    GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));
                    
                    // Use existing makeExtractor (JSON-based)
                    for (Field field : schema.getFields()) {
                        Optional<com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor> extractor = 
                            DDBTypeUtils.makeExtractor(field, recordMetadata, false);
                        if (extractor.isPresent()) {
                            builder.withExtractor(field.getName(), extractor.get());
                        } else {
                            builder.withFieldWriterFactory(field.getName(), 
                                DDBTypeUtils.makeFactory(field, recordMetadata, resolver, false));
                        }
                    }
                    
                    GeneratedRowWriter rowWriter = builder.build();
                    
                    // Write rows
                    for (int i = batch; i < batchEnd; i++) {
                        try {
                            rowWriter.writeRow(block, i - batch, testData[i]);
                        } catch (Exception e) {
                            // Continue for profiling
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
    
    private long profileDirectExtractorsApproach(Schema schema, Map<String, AttributeValue>[] testData)
    {
        System.out.println("Profiling direct extractors approach...");
        
        long totalTime = 0;
        
        try (RootAllocator allocator = new RootAllocator();
             BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl(allocator)) {
            
            // Process in batches like real connector
            for (int batch = 0; batch < RECORD_COUNT; batch += BATCH_SIZE) {
                int batchEnd = Math.min(batch + BATCH_SIZE, RECORD_COUNT);
                
                long batchStart = System.nanoTime();
                
                try (Block block = blockAllocator.createBlock(schema)) {
                    // Build row writer with direct extractors
                    DDBRecordMetadata recordMetadata = new DDBRecordMetadata(schema);
                    DynamoDBFieldResolver resolver = new DynamoDBFieldResolver(recordMetadata);
                    GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));
                    
                    // Use basic direct extractors (without decimal optimization)
                    for (Field field : schema.getFields()) {
                        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
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
                            case VARBINARY:
                                builder.withExtractor(fieldName, new DirectDDBExtractors.DirectVarBinaryExtractor(fieldName));
                                break;
                            default:
                                builder.withFieldWriterFactory(fieldName, 
                                    DDBTypeUtils.makeFactory(field, recordMetadata, resolver, false));
                                break;
                        }
                    }
                    
                    GeneratedRowWriter rowWriter = builder.build();
                    
                    // Write rows
                    for (int i = batch; i < batchEnd; i++) {
                        try {
                            rowWriter.writeRow(block, i - batch, testData[i]);
                        } catch (Exception e) {
                            // Continue for profiling
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
    
    private long profileOptimizedDecimalApproach(Schema schema, Map<String, AttributeValue>[] testData)
    {
        System.out.println("Profiling optimized decimal extractors approach...");
        
        long totalTime = 0;
        
        try (RootAllocator allocator = new RootAllocator();
             BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl(allocator)) {
            
            // Process in batches like real connector
            for (int batch = 0; batch < RECORD_COUNT; batch += BATCH_SIZE) {
                int batchEnd = Math.min(batch + BATCH_SIZE, RECORD_COUNT);
                
                long batchStart = System.nanoTime();
                
                try (Block block = blockAllocator.createBlock(schema)) {
                    // Build row writer with optimized decimal extractors
                    DDBRecordMetadata recordMetadata = new DDBRecordMetadata(schema);
                    DynamoDBFieldResolver resolver = new DynamoDBFieldResolver(recordMetadata);
                    GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null));
                    
                    // Use optimized direct extractors
                    for (Field field : schema.getFields()) {
                        Optional<com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor> directExtractor = 
                            DDBTypeUtils.makeDirectExtractor(field, recordMetadata, false);
                        if (directExtractor.isPresent()) {
                            builder.withExtractor(field.getName(), directExtractor.get());
                        } else {
                            builder.withFieldWriterFactory(field.getName(), 
                                DDBTypeUtils.makeFactory(field, recordMetadata, resolver, false));
                        }
                    }
                    
                    GeneratedRowWriter rowWriter = builder.build();
                    
                    // Write rows
                    for (int i = batch; i < batchEnd; i++) {
                        try {
                            rowWriter.writeRow(block, i - batch, testData[i]);
                        } catch (Exception e) {
                            // Continue for profiling
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
    
    private Schema createSchema()
    {
        return new Schema(Arrays.asList(
            // 10 String fields
            new Field("id", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("email", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("category", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("description", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("brand", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("model", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("sku", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("status", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            new Field("region", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
            
            // 20 Decimal fields (heavy JSON parsing)
            new Field("price", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("cost", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("margin", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("discount", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("tax_rate", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("shipping_cost", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("handling_fee", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("insurance", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("total", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("subtotal", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("weight", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("length", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("width", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("height", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("volume", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("rating", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("commission", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("bonus", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("penalty", FieldType.nullable(new ArrowType.Decimal(38, 9)), null),
            new Field("adjustment", FieldType.nullable(new ArrowType.Decimal(38, 9)), null)
        ));
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