/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class QueryUtilsTest
{
    private static final String ID_FIELD = "_id";
    private static final String CATEGORY_FIELD = "category";
    private static final String PRICE_FIELD = "price";
    private static final String BOOKS_VALUE = "books";
    private static final String ELECTRONICS_VALUE = "electronics";
    private static final String CLOTHING_VALUE = "clothing";
    private static final String OBJECT_ID_1 = "4ecbe7f9e8c1c9092c000027";
    private static final String OBJECT_ID_2 = "4ecbe7f9e8c1c9092c000028";
    private static final String EQ_OPERATOR = "$eq";
    private static final String IN_OPERATOR = "$in";
    private static final int PRICE_VALUE = 100;

    private final BlockAllocatorImpl allocator = new BlockAllocatorImpl();

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testMakePredicateWithSortedRangeSet()
    {
        Field field = new Field("year", FieldType.nullable(new ArrowType.Int(32, true)), null);

        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.lessThan(allocator, Types.MinorType.INT.getType(), 1950),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1952),
                        Range.range(allocator, Types.MinorType.INT.getType(), 1955, false, 1972, true),
                        Range.greaterThanOrEqual(allocator, Types.MinorType.INT.getType(), 2010)),
                false
        );
        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);
        Document expected = new Document("$or", ImmutableList.of(
                new Document("year", new Document("$lt", 1950)),
                new Document("year", new Document("$gt", 1955).append("$lte", 1972)),
                new Document("year", new Document("$gte", 2010)),
                new Document("year", new Document("$eq", 1952))
        ));
        assertEquals(expected, result);
    }

    @Test
    public void testMakePredicateWithId()
    {
        Field field = new Field("_id", FieldType.nullable(new ArrowType.Utf8()), null);

        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "4ecbe7f9e8c1c9092c000027")),
                false
        );
        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);
        Document expected =
                new Document("_id", new Document("$eq", new ObjectId("4ecbe7f9e8c1c9092c000027")));
        assertEquals(expected, result);
    }

    @Test
    public void testParseFilter()
    {
        String jsonFilter = "{ \"field\": { \"$eq\": \"value\" } }";

        Document result = QueryUtils.parseFilter(jsonFilter);
        assertNotNull(result);
        assertEquals("value", ((Document) result.get("field")).get("$eq"));
    }

    @Test
    public void testParseFilterInvalidJson()
    {
        String invalidJsonFilter = "{ field: { $eq: value } }";

        assertThrows(IllegalArgumentException.class, () -> {
            QueryUtils.parseFilter(invalidJsonFilter);
        });
    }

    @Test
    public void makePredicate_withEquatableValueSet_returnsEqualityPredicate()
    {
        Field field = new Field(CATEGORY_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a single value EquatableValueSet with nulls not allowed
        EquatableValueSet.Builder builder = EquatableValueSet.newBuilder(
                allocator, Types.MinorType.VARCHAR.getType(), false, false);
        builder.add(BOOKS_VALUE);
        ValueSet equatableSet = builder.build();

        Document result = QueryUtils.makePredicate(field, equatableSet);
        assertNotNull("Result should not be null", result);

        Document expected = new Document(CATEGORY_FIELD, new Document(EQ_OPERATOR, BOOKS_VALUE));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void makePredicate_withEquatableValueSetNumeric_returnsNumericEqualityPredicate()
    {
        Field field = new Field(PRICE_FIELD, FieldType.nullable(new ArrowType.Int(32, true)), null);

        // Create a single value EquatableValueSet with nulls not allowed
        EquatableValueSet.Builder builder = EquatableValueSet.newBuilder(
                allocator, Types.MinorType.INT.getType(), false, false);
        builder.add(PRICE_VALUE);
        ValueSet equatableSet = builder.build();

        Document result = QueryUtils.makePredicate(field, equatableSet);
        assertNotNull("Result should not be null", result);

        Document expected = new Document(PRICE_FIELD, new Document(EQ_OPERATOR, PRICE_VALUE));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void makePredicate_withMultipleObjectIds_returnsInPredicateWithObjectIds()
    {
        Field field = new Field(ID_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a SortedRangeSet with multiple single values to trigger the IN operator case
        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), OBJECT_ID_1),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), OBJECT_ID_2)),
                false
        );

        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull("Result should not be null", result);

        // The implementation should convert strings to ObjectIds and use $in operator
        Document expected = new Document(ID_FIELD,
                new Document(IN_OPERATOR, Arrays.asList(
                        new ObjectId(OBJECT_ID_1),
                        new ObjectId(OBJECT_ID_2)
                ))
        );
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void makePredicate_withMultipleValues_returnsInPredicate()
    {
        Field field = new Field(CATEGORY_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a SortedRangeSet with multiple single values to trigger the IN operator case
        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), BOOKS_VALUE),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), ELECTRONICS_VALUE),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), CLOTHING_VALUE)),
                false
        );

        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull("Result should not be null", result);

        // For non-_id fields, the implementation should use $in with the original values
        Document expected = new Document(CATEGORY_FIELD,
                new Document(IN_OPERATOR, Arrays.asList(BOOKS_VALUE, ELECTRONICS_VALUE, CLOTHING_VALUE))
        );

        // Compare the documents directly instead of their JSON strings
        Document categoryDoc = (Document) result.get(CATEGORY_FIELD);
        Document expectedCategoryDoc = (Document) expected.get(CATEGORY_FIELD);

        assertEquals(IN_OPERATOR, expectedCategoryDoc.keySet().iterator().next());
        assertEquals(IN_OPERATOR, categoryDoc.keySet().iterator().next());

        // Compare arrays ignoring order
        assertEquals(
                new java.util.HashSet<>((java.util.List<?>) expectedCategoryDoc.get(IN_OPERATOR)),
                new java.util.HashSet<>((java.util.List<?>) categoryDoc.get(IN_OPERATOR))
        );
    }

    @Test
    public void makePredicate_withNoneConstraint_returnsExistsAndEqNullPredicate()
    {
        Field field = new Field(CATEGORY_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a ValueSet that represents no values (isNone)
        ValueSet noneSet = SortedRangeSet.none(Types.MinorType.VARCHAR.getType());

        Document result = QueryUtils.makePredicate(field, noneSet);
        assertNotNull("Result should not be null", result);

        // Should generate a predicate that matches null values
        // Using both $exists and $eq for more precise null handling in MongoDB
        Document expected = new Document(CATEGORY_FIELD,
                new Document("$exists", true)
                        .append(EQ_OPERATOR, null));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void makePredicate_withAllConstraint_returnsNotEqualNullPredicate()
    {
        Field field = new Field(CATEGORY_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a ValueSet that represents all values (isAll)
        ValueSet allSet = SortedRangeSet.notNull(allocator, Types.MinorType.VARCHAR.getType());

        Document result = QueryUtils.makePredicate(field, allSet);
        assertNotNull("Result should not be null", result);

        // Should generate a predicate that matches non-null values
        Document expected = new Document(CATEGORY_FIELD, new Document("$ne", null));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void makePredicate_withNullAllowed_returnsNull()
    {
        Field field = new Field(CATEGORY_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a ValueSet that allows null values
        ValueSet nullAllowedSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), BOOKS_VALUE)),
                true
        );

        Document result = QueryUtils.makePredicate(field, nullAllowedSet);
        assertNull("Result should be null when null is allowed", result);
    }
}
