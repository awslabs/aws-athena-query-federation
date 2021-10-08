/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.splits;

import java.util.Objects;

/**
 * Represents a closed interval. Endpoints are inclusive.
 *
 * @param <T> type
 */
public class SplitRange<T>
{
    private final T low;
    private final T high;

    public SplitRange(T low, T high)
    {
        this.low = low;
        this.high = high;
    }

    public T getLow()
    {
        return low;
    }

    public T getHigh()
    {
        return high;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SplitRange<?> that = (SplitRange<?>) o;
        return Objects.equals(getLow(), that.getLow()) &&
                Objects.equals(getHigh(), that.getHigh());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getLow(), getHigh());
    }

    @Override
    public String toString()
    {
        return "SplitRange{" +
                "low=" + low +
                ", high=" + high +
                '}';
    }
}
