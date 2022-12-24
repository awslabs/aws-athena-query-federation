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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class SparkFilterToAthenaFederationValueSet
{
    private SparkFilterToAthenaFederationValueSet()
    {
    }

    public static ValueSet convert(Filter filter, Schema schema, BlockAllocator blockAllocator)
    {
        // Our connectors do not support pushdowns for nested fields.
        // So this will just allow findField to throw an exception upon encountering a nested field name.

        // Handle the leaf cases first:
        if (filter instanceof EqualNullSafe) {
            // This is the same as the <=> operator.
            // https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/sql/sources/EqualNullSafe.html
            // returns true (rather than NULL) if both inputs are NULL
            // and false (rather than NULL) if one of the input is NULL and the other is not NULL.
            //
            // In practice, Spark never provides us with EqualNullSafe with a value() of null, because those are
            // instead provided as IsNull().
            EqualNullSafe f = (EqualNullSafe) filter;
            if (f.value() == null) {
                return SortedRangeSet.onlyNull(schema.findField(f.attribute()).getType());
            }
            return SortedRangeSet.of(false, Range.equal(blockAllocator, schema.findField(f.attribute()).getType(), f.value()));
        }
        else if (filter instanceof EqualTo) {
            // There's no difference in regards to how we would represent EqualNullSafe and EqualTo as a SortedRangeSet
            // because null safety just pertains to the output value of the filter (whether its null or a bool instead).
            //
            // In practice, Spark never provides us with EqualTo with a value() of null, because those are
            // instead provided as IsNull().
            // And finally, even if Spark does provide us with EqualTo with a value() of null, we are having Spark
            // re-evaluate the pushed down filters anyway.
            EqualTo f = (EqualTo) filter;
            if (f.value() == null) {
                return SortedRangeSet.onlyNull(schema.findField(f.attribute()).getType());
            }
            return SortedRangeSet.of(false, Range.equal(blockAllocator, schema.findField(f.attribute()).getType(), f.value()));
        }
        else if (filter instanceof GreaterThan) {
            GreaterThan f = (GreaterThan) filter;
            return SortedRangeSet.of(Range.greaterThan(blockAllocator, schema.findField(f.attribute()).getType(), f.value()));
        }
        else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual f = (GreaterThanOrEqual) filter;
            return SortedRangeSet.of(Range.greaterThanOrEqual(blockAllocator, schema.findField(f.attribute()).getType(), f.value()));
        }
        else if (filter instanceof In) {
            In f = (In) filter;
            ArrowType type = schema.findField(f.attribute()).getType();
            List<Range> ranges = Arrays.stream(f.values()).map(v -> Range.equal(blockAllocator, type, v)).collect(Collectors.toList());
            return SortedRangeSet.copyOf(type, ranges, false);
        }
        else if (filter instanceof IsNotNull) {
            IsNotNull f = (IsNotNull) filter;
            return SortedRangeSet.notNull(blockAllocator, schema.findField(f.attribute()).getType());
        }
        else if (filter instanceof IsNull) {
            IsNull f = (IsNull) filter;
            return SortedRangeSet.onlyNull(schema.findField(f.attribute()).getType());
        }
        else if (filter instanceof LessThan) {
            LessThan f = (LessThan) filter;
            return SortedRangeSet.of(Range.lessThan(blockAllocator, schema.findField(f.attribute()).getType(), f.value()));
        }
        else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual f = (LessThanOrEqual) filter;
            return SortedRangeSet.of(Range.lessThanOrEqual(blockAllocator, schema.findField(f.attribute()).getType(), f.value()));
        }
        // Handle the recursive cases here:
        else if (filter instanceof And) {
            And f = (And) filter;
            return convert(f.left(), schema, blockAllocator).intersect(blockAllocator, convert(f.right(), schema, blockAllocator));
        }
        else if (filter instanceof Or) {
            Or f = (Or) filter;
            return convert(f.left(), schema, blockAllocator).union(blockAllocator, convert(f.right(), schema, blockAllocator));
        }
        else if (filter instanceof Not) {
            Not f = (Not) filter;
            return convert(f.child(), schema, blockAllocator).complement(blockAllocator);
        }
        // Unsupported filters:
        //    AlwaysTrue, AlwaysFalse, StringContains, StringEndsWith, StringStartsWith
        //    Always* can't be supported because they don't contain any column references so we can't construct
        //    a ValueSet which has to be associated with some ArrowType.
        throw new UnsupportedOperationException(String.format("Currently can't translate filter: %s . %s", filter.getClass(), filter.toString()));
    }
}
