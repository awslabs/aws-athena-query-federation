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
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBIndex;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
}
