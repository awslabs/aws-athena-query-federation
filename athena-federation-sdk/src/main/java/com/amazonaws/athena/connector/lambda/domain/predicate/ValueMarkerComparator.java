/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.domain.predicate;

import com.amazonaws.athena.connector.lambda.data.ArrowTypeComparator;

import java.util.Comparator;

public class ValueMarkerComparator
        implements Comparator<ValueMarker>
{
    private static final ValueMarkerComparator COMPARATOR = new ValueMarkerComparator();

    static int doCompare(ValueMarker o1, ValueMarker o2)
    {
        return COMPARATOR.compare(o1, o2);
    }

    @Override
    public int compare(ValueMarker o1, ValueMarker o2)
    {
        if (o1.isUpperUnbounded()) {
            return o2.isUpperUnbounded() ? 0 : 1;
        }
        if (o1.isLowerUnbounded()) {
            return o2.isLowerUnbounded() ? 0 : -1;
        }
        if (o2.isUpperUnbounded()) {
            return -1;
        }
        if (o2.isLowerUnbounded()) {
            return 1;
        }

        // INVARIANT: value and o.value are present
        if (o1.isNullValue() || o2.isNullValue()) {
            if (o1.isNullValue() == o2.isNullValue()) {
                return 0;
            }
            return o1.isNullValue() & !o2.isNullValue() ? 1 : -1;
        }

        int compare = ArrowTypeComparator.compare(o1.getType(), o1.getValue(), o2.getValue());
        if (compare == 0) {
            if (o1.getBound() == o2.getBound()) {
                return 0;
            }
            if (o1.getBound() == Marker.Bound.BELOW) {
                return -1;
            }
            if (o1.getBound() == Marker.Bound.ABOVE) {
                return 1;
            }
            // INVARIANT: bound == EXACTLY
            return (o2.getBound() == Marker.Bound.BELOW) ? 1 : -1;
        }
        return compare;
    }
}
