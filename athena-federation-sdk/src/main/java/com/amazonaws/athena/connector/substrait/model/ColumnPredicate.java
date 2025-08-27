/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.substrait.model;

import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Represents a column predicate extracted from Substrait filter expressions.
 * This class encapsulates a single filter condition that can be applied to a column,
 * including the column name, comparison operator, value, and data type information.
 * Used for predicate pushdown optimization in federated query execution.
 */
public class ColumnPredicate
{
    private final String columnName;
    private final SubstraitOperator substraitOperator;
    private final Object value;
    private final ArrowType arrowType;

    /**
     * Constructs a new column predicate.
     *
     * @param column the name of the column to filter
     * @param substraitOperator the comparison operator to use
     * @param value the value to compare the column against
     * @param arrowType the Arrow data type of the column
     */
    public ColumnPredicate(String column, SubstraitOperator substraitOperator, Object value, ArrowType arrowType)
    {
        this.columnName = column;
        this.substraitOperator = substraitOperator;
        this.value = value;
        this.arrowType = arrowType;
    }

    public String getColumn()
    {
        return columnName;
    }

    public SubstraitOperator getOperator()
    {
        return substraitOperator;
    }

    public Object getValue()
    {
        return value;
    }

    public ArrowType getArrowType()
    {
        return arrowType;
    }

    @Override
    public String toString()
    {
        return columnName + " " + substraitOperator + " '" + value + "'";
    }
}
