/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.StringJoiner;

import static java.lang.String.format;

public class OrderByField
{
    private final String columnName;
    private final Direction direction;

    @JsonCreator
    public OrderByField(@JsonProperty("columnName") String columnName,
                        @JsonProperty("direction") Direction direction)
    {
        this.columnName = columnName;
        this.direction = direction;
    }

    @JsonProperty("columnName")
    public String getColumnName()
    {
        return this.columnName;
    }

    @JsonProperty("direction")
    public Direction getDirection()
    {
        return this.direction;
    }

    public enum Direction
    {
        ASC_NULLS_FIRST(true, true),
        ASC_NULLS_LAST(true, false),
        DESC_NULLS_FIRST(false, true),
        DESC_NULLS_LAST(false, false);

        private final boolean ascending;
        private final boolean nullsFirst;

        Direction(boolean ascending, boolean nullsFirst)
        {
            this.ascending = ascending;
            this.nullsFirst = nullsFirst;
        }

        public boolean isAscending()
        {
            return ascending;
        }

        public boolean isNullsFirst()
        {
            return nullsFirst;
        }

        @Override
        public String toString()
        {
            return format("%s %s",
                    ascending ? "ASC" : "DESC",
                    nullsFirst ? "NULLS FIRST" : "NULLS LAST");
        }
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
        OrderByField that = (OrderByField) o;
        return Objects.equal(this.columnName, that.columnName) &&
               Objects.equal(this.direction, that.direction);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.columnName, this.direction);
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", OrderByField.class.getSimpleName() + "[", "]");
        return stringJoiner
                .add("columnName=" + columnName)
                .add("direction=" + direction.name())
                .toString();
    }
}
