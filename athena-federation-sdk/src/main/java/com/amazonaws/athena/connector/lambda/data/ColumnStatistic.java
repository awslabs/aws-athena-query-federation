/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.data;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

public class ColumnStatistic
{
    private Optional<Double> min;
    private Optional<Double> max;
    private Optional<Double> nullProbability;
    private Optional<Double> columnSize;

    public ColumnStatistic(
            @JsonProperty("min") Optional<Double> min,
            @JsonProperty("max") Optional<Double> max,
            @JsonProperty("nullProbability") Optional<Double> nullProbability,
            @JsonProperty("columnSize") Optional<Double> columnSize)
    {
        this.min = min;
        this.max = max;
        this.nullProbability = nullProbability;
        this.columnSize = columnSize;
    }

    public Optional<Double> getMin()
    {
        return min;
    }

    public Optional<Double> getMax()
    {
        return max;
    }

    public Optional<Double> getNullProbability()
    {
        return nullProbability;
    }

    public Optional<Double> getColumnSize()
    {
        return columnSize;
    }

    @Override
    public String toString()
    {
        return "ColumnStatistic{" +
                "min=" + min +
                ", max=" + max +
                ", nullProbability=" + nullProbability +
                ", columnSize=" + columnSize +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnStatistic that = (ColumnStatistic) o;
        return Objects.equals(min, that.min) && Objects.equals(max, that.max) && Objects.equals(nullProbability, that.nullProbability) && Objects.equals(columnSize, that.columnSize);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max, nullProbability, columnSize);
    }
}
