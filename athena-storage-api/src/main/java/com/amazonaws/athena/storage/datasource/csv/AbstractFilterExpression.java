/*-
 * #%L
 * athena-storage-api
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.storage.datasource.csv;

import com.amazonaws.athena.storage.common.FilterExpression;

public abstract class AbstractFilterExpression<T> implements FilterExpression
{
    /**
     * Column name to retrieve value to apply the expression
     */
    protected final String column;

    /**
     * Expression of Type T (can be anything such as Long, String, etc.)
     */
    public T expression;

    /**
     * Fluent-styled getter
     *
     * @return Returns the column name against which the expression is being evalluated
     */
    @Override
    public String columnName()
    {
        return this.column;
    }

    /**
     * Constructor to instantiate the expression with the column name
     *
     * @param columnName Name of the column for the expression
     */
    public AbstractFilterExpression(String columnName)
    {
        this.column = columnName;
    }

    /**
     * Overloaded constructor to instantiate the expression with the column name and the expression to be evaluated
     *
     * @param columnName Name of the column for the expression
     * @param expression Expression to be evaluated
     */
    public AbstractFilterExpression(String columnName, T expression)
    {
        this.column = columnName.toLowerCase();
        this.expression = expression;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean apply(String value);

    public boolean apply(Object value)
    {
        throw new UnsupportedOperationException("Method apply(Object) is not implemented in class " + getClass().getSimpleName());
    }

    /**
     * Constitutes a string representation of the expression with column name expression type, etc.
     *
     * @return String representation of the expression
     */
    @Override
    public String toString()
    {
        return "{" +
                "'column':'" + column + "', " +
                "'expression': '" + expression + "', " +
                "'type':'" + getClass().getSimpleName()
                + "'}";
    }
}
