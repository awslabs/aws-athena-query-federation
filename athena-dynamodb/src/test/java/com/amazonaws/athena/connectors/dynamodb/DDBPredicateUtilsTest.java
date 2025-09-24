/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBIndex;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.IncrementingValueNameProducer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.pojo.ArrowType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.Optional;

import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests DynamoDB utility methods relating to predicate handling.
 */
@RunWith(MockitoJUnitRunner.class)
public class DDBPredicateUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(DDBPredicateUtilsTest.class);

    @Test
    public void testAliasColumn()
    {
        logger.info("testAliasColumn - enter");

        assertEquals("Unexpected alias column value!", "#column_1",
                DDBPredicateUtils.aliasColumn("column_1"));
        assertEquals("Unexpected alias column value!", "#column__1",
                DDBPredicateUtils.aliasColumn("column__1"));
        assertEquals("Unexpected alias column value!", "#column_1",
                DDBPredicateUtils.aliasColumn("column-1"));
        assertEquals("Unexpected alias column value!", "#column_1_f3F",
                DDBPredicateUtils.aliasColumn("column-$1`~!@#$%^&*()-=+[]{}\\|;:'\",.<>/?f3F"));

        logger.info("testAliasColumn - exit");
    }

    @Test
    public void testGetBestIndexForPredicatesWithGSIProjectionTypeInclude()
    {
        // global secondary index with INCLUDE projection type
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
              ImmutableList.of(
                      AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                      AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                      AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
              ImmutableList.of(
                    new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.INCLUDE, ImmutableList.of("col1"))
              ), 1000, 10, 5);
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1", "col2"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1", "col2"), ImmutableMap.of("sortKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1", "col2"), ImmutableMap.of("col3", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1", "col2"), ImmutableMap.of("col0", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("col0", singleValueSet)).getName());
    }

    @Test
    public void testGetBestIndexForPredicatesWithGSIProjectionTypeKeysOnly()
    {
        // global secondary index with KEYS_ONLY projection type
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
              ImmutableList.of(
                    AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                     AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
              ImmutableList.of(
                    new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.KEYS_ONLY, ImmutableList.of())
              ), 1000, 10, 5);
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("col0", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0"), ImmutableMap.of("col0", singleValueSet)).getName());
    }

    @Test
    public void testGetBestIndexForPredicatesWithNonEqualityPredicate()
    {
        // non-equality conditions for the hash key
        ValueSet rangeValueSet = SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), VARCHAR.getType(), "aaa", true, "bbb", false));
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
              ImmutableList.of(
                    AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                     AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
              ImmutableList.of(
                    new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.KEYS_ONLY, ImmutableList.of())
              ), 1000, 10, 5);
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0"), ImmutableMap.of("col0", rangeValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0"), ImmutableMap.of("col0", singleValueSet)).getName());
    }

    @Test
    public void testGetBestIndexForPredicatesWithGSIProjectionTypeAll()
    {
        // global secondary index with ALL projection type
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
              ImmutableList.of(
                    AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                     AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
              ImmutableList.of(
                    new DynamoDBIndex("col0-gsi", "col0", Optional.of("col1"), ProjectionType.ALL, ImmutableList.of())
              ), 1000, 10, 5);
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1", "col2"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("col0", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1", "col2"), ImmutableMap.of("col0", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0"), ImmutableMap.of("col0", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0"), ImmutableMap.of("col0", singleValueSet, "col1", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0"), ImmutableMap.of("col0", singleValueSet, "sortKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0"), ImmutableMap.of("col1", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0"), ImmutableMap.of("col1", singleValueSet, "hashKey", singleValueSet)).getName());

    }

    @Test
    public void testGetBestIndexForPredicatesWithLSI()
    {
        // local secondary index
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
              ImmutableList.of(
                    AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                     AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
              ImmutableList.of(
                    new DynamoDBIndex("col0-lsi", "hashKey", Optional.of("col0"), ProjectionType.ALL, ImmutableList.of())
              ), 1000, 10, 5);
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("hashKey", singleValueSet, "sortKey", singleValueSet)).getName());
        assertEquals("col0-lsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("hashKey", singleValueSet, "col0", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "sortKey", "col0", "col1"), ImmutableMap.of("col0", singleValueSet)).getName());

    }

    @Test
    public void testGetBestIndexForPredicatesWithMultipleIndices()
    {
        // multiple indices
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
              ImmutableList.of(
                    AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                     AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build(),
                     AttributeDefinition.builder().attributeName("col1").attributeType(ScalarAttributeType.S).build()),
              ImmutableList.of(
                    new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.INCLUDE, ImmutableList.of("col1")),
                    new DynamoDBIndex("col1-gsi", "col1", Optional.empty(), ProjectionType.INCLUDE, ImmutableList.of("col2")),
                    new DynamoDBIndex("col2-lsi", "hashKey", Optional.of("col2"), ProjectionType.ALL, ImmutableList.of())
              ), 1000, 10, 5);
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0", "col1", "col2"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0", "col1", "col2"), ImmutableMap.of("col0", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0", "col1", "col2"), ImmutableMap.of("col1", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0", "col1", "col2"), ImmutableMap.of("col2", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0", "col1"), ImmutableMap.of("col0", singleValueSet)).getName());
        assertEquals("col1-gsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col1", "col2"), ImmutableMap.of("col1", singleValueSet)).getName());
        assertEquals("col2-lsi", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey", "col0", "col1"), ImmutableMap.of("hashKey", singleValueSet, "col2", singleValueSet)).getName());
    }

    @Test
    public void testAliasColumnEdgeCases()
    {
        // Test empty string
        assertEquals("#", DDBPredicateUtils.aliasColumn(""));
        
        // Test single character
        assertEquals("#a", DDBPredicateUtils.aliasColumn("a"));
        assertEquals("#_", DDBPredicateUtils.aliasColumn("_"));
        assertEquals("#1", DDBPredicateUtils.aliasColumn("1"));
        assertEquals("#_", DDBPredicateUtils.aliasColumn("!"));
        
        // Test consecutive special characters
        assertEquals("#column_name", DDBPredicateUtils.aliasColumn("column---name"));
        assertEquals("#column_name", DDBPredicateUtils.aliasColumn("column!!!name"));
        assertEquals("#column_name", DDBPredicateUtils.aliasColumn("column@@@name"));
        
        // Test leading/trailing special characters
        assertEquals("#_column", DDBPredicateUtils.aliasColumn("!column"));
        assertEquals("#column_", DDBPredicateUtils.aliasColumn("column!"));
        assertEquals("#_column_", DDBPredicateUtils.aliasColumn("!column!"));
        
        // Test unicode characters
        assertEquals("#column_name", DDBPredicateUtils.aliasColumn("column\u00A9name"));
        
        // Test very long column name
        String longName = "a".repeat(100) + "!" + "b".repeat(100);
        String expectedLongAlias = "#" + "a".repeat(100) + "_" + "b".repeat(100);
        assertEquals(expectedLongAlias, DDBPredicateUtils.aliasColumn(longName));
    }

    @Test
    public void testIndexContainsAllRequiredColumnsEdgeCases()
    {
        logger.info("testIndexContainsAllRequiredColumnsEdgeCases - enter");
        
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.INCLUDE, ImmutableList.of("col1"))
                ), 1000, 10, 5);
        
        DynamoDBIndex index = table.getIndexes().get(0);
        
        // Test null inputs
        assertEquals(false, DDBPredicateUtils.indexContainsAllRequiredColumns(null, index, table));
        assertEquals(false, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("col1"), null, table));
        assertEquals(false, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("col1"), index, null));
        
        // Test empty requested columns
        assertEquals(true, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of(), index, table));
        
        // Test with ALL projection type
        DynamoDBIndex allIndex = new DynamoDBIndex("all-gsi", "col0", Optional.empty(), ProjectionType.ALL, ImmutableList.of());
        assertEquals(true, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("anyColumn"), allIndex, table));
        
        // Test with KEYS_ONLY projection type
        DynamoDBIndex keysOnlyIndex = new DynamoDBIndex("keys-gsi", "col0", Optional.of("col1"), ProjectionType.KEYS_ONLY, ImmutableList.of());
        assertEquals(true, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("hashKey", "sortKey", "col0", "col1"), keysOnlyIndex, table));
        assertEquals(false, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("hashKey", "sortKey", "col0", "col1", "col2"), keysOnlyIndex, table));
        
        // Test with INCLUDE projection type - edge cases
        assertEquals(true, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("hashKey"), index, table));
        assertEquals(true, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("sortKey"), index, table));
        assertEquals(true, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("col0"), index, table));
        assertEquals(true, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("col1"), index, table));
        assertEquals(false, DDBPredicateUtils.indexContainsAllRequiredColumns(ImmutableList.of("col2"), index, table));
    }

    @Test
    public void testGetBestIndexForPredicatesEdgeCases()
    {
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        
        // Test table with no range key
        DynamoDBTable tableNoRangeKey = new DynamoDBTable("tableName", "hashKey", Optional.empty(),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(tableNoRangeKey, ImmutableList.of("hashKey"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("col0-gsi", DDBPredicateUtils.getBestIndexForPredicates(tableNoRangeKey, ImmutableList.of("col0"), ImmutableMap.of("col0", singleValueSet)).getName());
        
        // Test table with no indexes
        DynamoDBTable tableNoIndexes = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(), 1000, 10, 5);
        
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(tableNoIndexes, ImmutableList.of("hashKey"), ImmutableMap.of("hashKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(tableNoIndexes, ImmutableList.of("sortKey"), ImmutableMap.of("sortKey", singleValueSet)).getName());
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(tableNoIndexes, ImmutableList.of("col0"), ImmutableMap.of("col0", singleValueSet)).getName());
        
        // Test empty predicates
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of("hashKey"), ImmutableMap.of()).getName());
        
        // Test empty requested columns
        assertEquals("tableName", DDBPredicateUtils.getBestIndexForPredicates(table, ImmutableList.of(), ImmutableMap.of("hashKey", singleValueSet)).getName());
    }

    @Test
    public void testBuildFilterPredicatesFromPlanEdgeCases()
    {
        // Test with null plan
        assertEquals(new HashMap<>(), DDBPredicateUtils.buildFilterPredicatesFromPlan(null));
        
        // Test with empty plan
        Plan emptyPlan = Plan.newBuilder().build();
        assertEquals(new HashMap<>(), DDBPredicateUtils.buildFilterPredicatesFromPlan(emptyPlan));
    }

    @Test
    public void testGenerateFilterExpressionEdgeCases()
    {
        // Test with empty predicates
        assertEquals(null, DDBPredicateUtils.generateFilterExpression(
                new HashSet<>(), 
                ImmutableMap.of(), 
                new ArrayList<>(), 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null)
        ));
        
        // Test with all columns ignored
        ValueSet singleValueSet = SortedRangeSet.of(Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value"));
        assertEquals(null, DDBPredicateUtils.generateFilterExpression(
                Set.of("col1", "col2"), 
                ImmutableMap.of("col1", singleValueSet, "col2", singleValueSet), 
                new ArrayList<>(), 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null)
        ));
        
        // Test with some columns ignored
        String result = DDBPredicateUtils.generateFilterExpression(
                Set.of("col2"), 
                ImmutableMap.of("col1", singleValueSet, "col2", singleValueSet), 
                new ArrayList<>(), 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null)
        );
        assertNotNull(result);
        assertTrue(result.contains("#col1"));
        assertFalse(result.contains("#col2"));
    }

    @Test
    public void testGenerateFilterExpressionForPlanEdgeCases()
    {
        assertEquals(null, DDBPredicateUtils.generateFilterExpressionForPlan(
                new HashSet<>(), 
                new HashMap<>(), 
                new ArrayList<>(), 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null)
        ));
        
        // Test with null filter result from generateSingleColumnFilter
        Map<String, List<ColumnPredicate>> emptyPredicates = ImmutableMap.of("col1", ImmutableList.of());
        assertEquals(null, DDBPredicateUtils.generateFilterExpressionForPlan(
                new HashSet<>(), 
                emptyPredicates, 
                new ArrayList<>(), 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null)
        ));
    }

    @Test
    public void testValidateColumnRangeEdgeCases()
    {
        Range validRange1 = Range.range(new BlockAllocatorImpl(), VARCHAR.getType(), "a", true, "z", true);
        Range validRange2 = Range.range(new BlockAllocatorImpl(), VARCHAR.getType(), "a", false, "z", false);
        Range validRange3 = Range.greaterThan(new BlockAllocatorImpl(), VARCHAR.getType(), "a");
        Range validRange4 = Range.lessThan(new BlockAllocatorImpl(), VARCHAR.getType(), "z");
        Range validRange5 = Range.equal(new BlockAllocatorImpl(), VARCHAR.getType(), "value");
        
        // These should all work without throwing exceptions when used in generateSingleColumnFilter
        try {
            DDBPredicateUtils.generateSingleColumnFilter(
                    "testCol", 
                    SortedRangeSet.of(validRange1), 
                    new ArrayList<>(), 
                    new IncrementingValueNameProducer(), 
                    new DDBRecordMetadata(null), 
                    false
            );
            
            DDBPredicateUtils.generateSingleColumnFilter(
                    "testCol", 
                    SortedRangeSet.of(validRange2), 
                    new ArrayList<>(), 
                    new IncrementingValueNameProducer(), 
                    new DDBRecordMetadata(null), 
                    false
            );
            
            DDBPredicateUtils.generateSingleColumnFilter(
                    "testCol", 
                    SortedRangeSet.of(validRange3), 
                    new ArrayList<>(), 
                    new IncrementingValueNameProducer(), 
                    new DDBRecordMetadata(null), 
                    false
            );
            
            DDBPredicateUtils.generateSingleColumnFilter(
                    "testCol", 
                    SortedRangeSet.of(validRange4), 
                    new ArrayList<>(), 
                    new IncrementingValueNameProducer(), 
                    new DDBRecordMetadata(null), 
                    false
            );
            
            DDBPredicateUtils.generateSingleColumnFilter(
                    "testCol", 
                    SortedRangeSet.of(validRange5), 
                    new ArrayList<>(), 
                    new IncrementingValueNameProducer(), 
                    new DDBRecordMetadata(null), 
                    false
            );
        } catch (Exception e) {
            fail("Valid ranges should not throw exceptions: " + e.getMessage());
        }
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithNullPredicates()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(), 1000, 10, 5);
        
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("hashKey"), null);
        assertEquals("tableName", result.getName());
        assertEquals("hashKey", result.getHashKey());
        assertEquals(Optional.of("sortKey"), result.getRangeKey());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithEmptyPredicates()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(), 1000, 10, 5);
        
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("hashKey"), new HashMap<>());
        assertEquals("tableName", result.getName());
        assertEquals("hashKey", result.getHashKey());
        assertEquals(Optional.of("sortKey"), result.getRangeKey());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithEqualityPredicates()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        Map<String, List<ColumnPredicate>> predicates = ImmutableMap.of(
                "col0", ImmutableList.of(new ColumnPredicate("col0", SubstraitOperator.EQUAL, "value", new ArrowType.Utf8()))
        );
        
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("col0"), predicates);
        assertEquals("col0-gsi", result.getName());
        assertEquals("col0", result.getHashKey());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithNonEqualityPredicates()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        Map<String, List<ColumnPredicate>> predicates = ImmutableMap.of(
                "col0", ImmutableList.of(new ColumnPredicate("col0", SubstraitOperator.GREATER_THAN, "value", new ArrowType.Utf8()))
        );
        
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("col0"), predicates);
        assertEquals("tableName", result.getName());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithHashAndRangeKeyPredicates()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.of("col1"), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        Map<String, List<ColumnPredicate>> predicates = ImmutableMap.of(
                "col0", ImmutableList.of(new ColumnPredicate("col0", SubstraitOperator.EQUAL, "value1", new ArrowType.Utf8())),
                "col1", ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.EQUAL, "value2", new ArrowType.Utf8()))
        );
        
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("col0", "col1"), predicates);
        assertEquals("col0-gsi", result.getName());
        assertEquals("col0", result.getHashKey());
        assertEquals(Optional.of("col1"), result.getRangeKey());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithProjectionConstraints()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.KEYS_ONLY, ImmutableList.of())
                ), 1000, 10, 5);
        
        Map<String, List<ColumnPredicate>> predicates = ImmutableMap.of(
                "col0", ImmutableList.of(new ColumnPredicate("col0", SubstraitOperator.EQUAL, "value", new ArrowType.Utf8()))
        );
        
        // Request columns that are not projected in the index
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("col0", "col2"), predicates);
        assertEquals("tableName", result.getName());
        
        // Request only projected columns
        result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("hashKey", "sortKey", "col0"), predicates);
        assertEquals("col0-gsi", result.getName());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithMultipleIndices()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col1").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.ALL, ImmutableList.of()),
                        new DynamoDBIndex("col1-gsi", "col1", Optional.empty(), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        Map<String, List<ColumnPredicate>> predicates = ImmutableMap.of(
                "col0", ImmutableList.of(new ColumnPredicate("col0", SubstraitOperator.EQUAL, "value1", new ArrowType.Utf8())),
                "col1", ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.EQUAL, "value2", new ArrowType.Utf8()))
        );
        
        // Should return first matching index
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("col0", "col1"), predicates);
        assertEquals("col0-gsi", result.getName());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithTableHashKeyPredicate()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-gsi", "col0", Optional.empty(), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        Map<String, List<ColumnPredicate>> predicates = ImmutableMap.of(
                "hashKey", ImmutableList.of(new ColumnPredicate("hashKey", SubstraitOperator.EQUAL, "value", new ArrowType.Utf8()))
        );
        
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("hashKey", "sortKey"), predicates);
        assertEquals("tableName", result.getName());
        assertEquals("hashKey", result.getHashKey());
    }

    @Test
    public void testGetBestIndexForPredicatesForPlanWithLSI()
    {
        DynamoDBTable table = new DynamoDBTable("tableName", "hashKey", Optional.of("sortKey"),
                ImmutableList.of(
                        AttributeDefinition.builder().attributeName("hashKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("col0").attributeType(ScalarAttributeType.S).build()),
                ImmutableList.of(
                        new DynamoDBIndex("col0-lsi", "hashKey", Optional.of("col0"), ProjectionType.ALL, ImmutableList.of())
                ), 1000, 10, 5);
        
        Map<String, List<ColumnPredicate>> predicates = ImmutableMap.of(
                "hashKey", ImmutableList.of(new ColumnPredicate("hashKey", SubstraitOperator.EQUAL, "value1", new ArrowType.Utf8())),
                "col0", ImmutableList.of(new ColumnPredicate("col0", SubstraitOperator.EQUAL, "value2", new ArrowType.Utf8()))
        );
        
        DynamoDBIndex result = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, ImmutableList.of("hashKey", "col0"), predicates);
        assertEquals("col0-lsi", result.getName());
        assertEquals("hashKey", result.getHashKey());
        assertEquals(Optional.of("col0"), result.getRangeKey());
    }

    @Test
    public void testGenerateSingleColumnFilterWithEmptyPredicates()
    {
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(), 
                new ArrayList<>(), 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals(null, result);
    }

    @Test
    public void testGenerateSingleColumnFilterWithEqualPredicate()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.EQUAL, "value1", new ArrowType.Utf8())),
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("#col1 = :v0", result);
        assertEquals(1, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithMultipleEqualPredicates()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(
                        new ColumnPredicate("col1", SubstraitOperator.EQUAL, "value1", new ArrowType.Utf8()),
                        new ColumnPredicate("col1", SubstraitOperator.EQUAL, "value2", new ArrowType.Utf8())
                ), 
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("#col1 IN (:v0,:v1)", result);
        assertEquals(2, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithNotEqualPredicate()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.NOT_EQUAL, "value1", new ArrowType.Utf8())),
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("#col1 <> :v0", result);
        assertEquals(1, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithMultipleNotEqualPredicates()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(
                        new ColumnPredicate("col1", SubstraitOperator.NOT_EQUAL, "value1", new ArrowType.Utf8()),
                        new ColumnPredicate("col1", SubstraitOperator.NOT_EQUAL, "value2", new ArrowType.Utf8())
                ), 
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("NOT #col1 IN (:v0,:v1)", result);
        assertEquals(2, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithGreaterThanPredicate()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.GREATER_THAN, "value1", new ArrowType.Utf8())),
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("(#col1 > :v0)", result);
        assertEquals(1, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithLessThanPredicate()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.LESS_THAN, "value1", new ArrowType.Utf8())),
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("(#col1 < :v0)", result);
        assertEquals(1, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithBetweenPredicate()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(
                        new ColumnPredicate("col1", SubstraitOperator.GREATER_THAN_OR_EQUAL_TO, "value1", new ArrowType.Utf8()),
                        new ColumnPredicate("col1", SubstraitOperator.LESS_THAN_OR_EQUAL_TO, "value2", new ArrowType.Utf8())
                ), 
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("(#col1 BETWEEN :v0 AND :v1)", result);
        assertEquals(2, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithSortKeyConstraint()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "sortKey", 
                ImmutableList.of(
                        new ColumnPredicate("sortKey", SubstraitOperator.GREATER_THAN, "value1", new ArrowType.Utf8()),
                        new ColumnPredicate("sortKey", SubstraitOperator.LESS_THAN, "value2", new ArrowType.Utf8())
                ), 
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                true
        );
        // Should only include upper bound for sort key
        assertEquals("(#sortKey < :v0)", result);
        assertEquals(1, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithIsNullPredicate()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.IS_NULL, null, new ArrowType.Utf8())),
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("(attribute_not_exists(#col1) OR #col1 <> :v0)", result);
        assertEquals(1, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithIsNotNullPredicate()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "col1", 
                ImmutableList.of(new ColumnPredicate("col1", SubstraitOperator.IS_NOT_NULL, null, new ArrowType.Utf8())),
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("(attribute_exists(#col1) AND #col1 = :v0)", result);
        assertEquals(1, accumulator.size());
    }

    @Test
    public void testGenerateSingleColumnFilterWithSpecialCharactersInColumnName()
    {
        List<AttributeValue> accumulator = new ArrayList<>();
        String result = DDBPredicateUtils.generateSingleColumnFilter(
                "column-with$special@chars", 
                ImmutableList.of(new ColumnPredicate("column-with$special@chars", SubstraitOperator.EQUAL, "value1", new ArrowType.Utf8())),
                accumulator, 
                new IncrementingValueNameProducer(), 
                new DDBRecordMetadata(null), 
                false
        );
        assertEquals("#column_with_special_chars = :v0", result);
        assertEquals(1, accumulator.size());
    }
}