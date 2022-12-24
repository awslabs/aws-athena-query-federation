/*-
 * #%L
 * athena-federation-sdk-dsv2
 * %%
 * Copyright (C) 2023 Amazon Web Services
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

package com.amazonaws.athena.connectors.dsv2;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.AlwaysTrue;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import static org.junit.Assert.*;

public class SparkFilterToAthenaFederationValueSetTest
{
    private BlockAllocatorImpl allocator;

    @Before
    public void setup() {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testEqualNullSafe() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        {
            EqualNullSafe filter = new EqualNullSafe("asdf", 1234);
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }
        {
            EqualNullSafe filter = new EqualNullSafe("asdf", null);
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
        }
    }

    @Test
    public void testEqualTo() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        {
            EqualTo filter = new EqualTo("asdf", 1234);
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }
        {
            EqualTo filter = new EqualTo("asdf", null);
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
        }
    }

    @Test
    public void testGreaterThan() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        GreaterThan filter = new GreaterThan("asdf", 1234);
        ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 345345)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1000)));
        assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
    }

    @Test
    public void testGreaterThanOrEqual() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        GreaterThanOrEqual filter = new GreaterThanOrEqual("asdf", 1234);
        ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 5523423)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1)));
        assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
    }

    @Test
    public void testIn() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        In filter = new In("asdf", (Object[])(new Integer[] {28374, 111, 555, 333, 2222}));
        ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 28374)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 111)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 555)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 333)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 2222)));

        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 222)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 234234)));

        assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
    }

    @Test
    public void testIsNotNull() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        IsNotNull filter = new IsNotNull("asdf");
        ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 324532458)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));

        assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
    }

    @Test
    public void testIsNull() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        IsNull filter = new IsNull("asdf");
        ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

        assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));

        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 324532458)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
    }

    @Test
    public void testLessThan() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        LessThan filter = new LessThan("asdf", 1234);
        ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1000)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 345345)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
        assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
    }

    @Test
    public void testLessThanOrEqual() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));
        LessThanOrEqual filter = new LessThanOrEqual("asdf", 1234);
        ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1000)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 345345)));
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1235)));
        assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
    }

    @Test
    public void testNot() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));

        // Not(EqualNullSafe("asdf", null)) should be equivalent to the IsNotNull test above
        {
            Not filter = new Not(new EqualNullSafe("asdf", null));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 324532458)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        // Same for Not(EqualTo("asdf", null))
        {
            Not filter = new Not(new EqualTo("asdf", null));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 324532458)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        // Not(LessThanOrEqual("asdf", 1234)) should be equivalent to GreaterThan("asdf", 1234) + IsNull("asdf")
        {
            Not filter = new Not(new LessThanOrEqual("asdf", 1234));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 345345)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1000)));
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        // Not(GreaterThan("asdf", 1234)) should be equivalent to LessThanOrEqual("asdf", 1234) + IsNull("asdf")
        {
            Not filter = new Not(new GreaterThan("asdf", 1234));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1234)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1111)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), -123)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 12341243)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1235)));
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        // Not(In("asdf", [...])) should exclude everything from the In set but now also include null
        {
            Not filter = new Not(new In("asdf", (Object[])(new Integer[] {28374, 111, 555, 333, 2222})));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 112)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 444)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1238909)));
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));

            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 28374)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 111)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 555)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 333)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 2222)));
        }

        // Not(Not(In("asdf", [...]))) should give us back what the original In filter would have been
        {
            Not filter = new Not(new Not(new In("asdf", (Object[])(new Integer[] {28374, 111, 555, 333, 2222}))));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 28374)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 111)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 555)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 333)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 2222)));

            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 222)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 234234)));

            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }
    }

    @Test
    public void testOr() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));

        {
            Or filter = new Or(new LessThanOrEqual("asdf", 50000), new GreaterThan("asdf", 80000));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 40000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80001)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 889898)));

            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        {
            Or filter = new Or(new Not(new LessThanOrEqual("asdf", 50000)), new Not(new GreaterThan("asdf", 80000)));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            // In this case since the two inverted conditions overlap and are now Or'ed, all values are valid now
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 70000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 53424)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 40000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80001)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 90908)));
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        {
            Not filter = new Not(new Or(new LessThanOrEqual("asdf", 50000), new GreaterThan("asdf", 80000)));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);
            // In this case since we are inverting the entire Or, we should only get results in
            // between 50000 (not inclusive) and 80000 (inclusive) now and also null
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 70000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 53424)));
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 40000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 90000)));
        }
    }

    @Test
    public void testAnd() {
        Schema schema = new Schema(ImmutableList.of(new Field("asdf", new FieldType(true, Types.MinorType.INT.getType(), null), ImmutableList.of())));

        {
            And filter = new And(new LessThanOrEqual("asdf", 50000), new GreaterThan("asdf", 80000));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

            // No values can possibly match this filter
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 40000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 10000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80001)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 889898)));
            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        {
            And filter = new And(new GreaterThanOrEqual("asdf", 50000), new LessThan("asdf", 80000));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

            // Values in between 50000 (inclusive)  and 80000 (exclusive) should match
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 60000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 79898)));

            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 40000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 10000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 889898)));
            assertFalse(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
        }

        {
            And filter = new And(new Not(new LessThanOrEqual("asdf", 50000)), new Not(new GreaterThan("asdf", 80000)));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

            // Values in between 50000 (exclusive) and 80000 (inclusive) should match and also null
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50001)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 60000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 79898)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80000)));
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));

            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 40000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 10000)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
            assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 889898)));
        }

        {
            Not filter = new Not(new And(new LessThanOrEqual("asdf", 50000), new GreaterThan("asdf", 80000)));
            ValueSet valueSet = SparkFilterToAthenaFederationValueSet.convert(filter, schema, allocator);

            // Values in greater than 50000 (exclusive) or less than 80000 (inclusive) should match and also null
            // Which means that this should match everything.
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50001)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 60000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 79898)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 80000)));
            assertTrue(valueSet.containsValue(Marker.nullMarker(allocator, Types.MinorType.INT.getType())));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 50000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 40000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 10000)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));
            assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 889898)));
        }
    }
}
