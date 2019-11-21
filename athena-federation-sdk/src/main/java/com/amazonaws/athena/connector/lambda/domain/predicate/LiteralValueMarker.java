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

import org.apache.arrow.vector.types.pojo.ArrowType;

public class LiteralValueMarker
        implements ValueMarker, Comparable<ValueMarker>
{
    private final Object value;
    private final ArrowType arrowType;

    protected LiteralValueMarker(Object value, ArrowType arrowType)
    {
        this.value = value;
        this.arrowType = arrowType;
    }

    public boolean isUpperUnbounded()
    {
        return false;
    }

    @Override
    public boolean isLowerUnbounded()
    {
        return false;
    }

    @Override
    public boolean isNullValue()
    {
        return value == null;
    }

    @Override
    public Object getValue()
    {
        return value;
    }

    @Override
    public Marker.Bound getBound()
    {
        return Marker.Bound.EXACTLY;
    }

    @Override
    public ArrowType getType()
    {
        return arrowType;
    }

    @Override
    public int compareTo(ValueMarker o)
    {
        return ValueMarkerComparator.doCompare(this, o);
    }
}
