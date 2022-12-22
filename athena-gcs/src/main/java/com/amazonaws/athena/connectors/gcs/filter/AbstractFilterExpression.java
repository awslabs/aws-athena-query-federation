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
package com.amazonaws.athena.connectors.gcs.filter;

public abstract class AbstractFilterExpression<T> implements FilterExpression
{
    /**
     * {@inheritDoc}
     */
    protected final Integer columnIndex;

    protected final String columnName;

    /**
     * Expression of type T
     */
    public T expression;

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public Integer columnIndex()
    {
        return this.columnIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String columnName()
    {
        return this.columnName;
    }

    /**
     * Constructs this with only column index
     *
     * @param columnIndex Index of the column
     */
    @Deprecated
    public AbstractFilterExpression(Integer columnIndex, String columnName)
    {
        this.columnIndex = columnIndex;
        this.columnName = columnName;
    }

    /**
     * Constructs this with column index and an expression
     *
     * @param index      Index of the column
     * @param expression Expression to match
     */
    public AbstractFilterExpression(Integer index, String columnName, T expression)
    {
        this.columnIndex = index;
        this.columnName = columnName;
        this.expression = expression;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object filterValue()
    {
        return this.expression;
    }

    /**
     * {@inheritDoc}
     *
     * @param value Column value
     * @return
     */
    @Override
    public boolean apply(Object value)
    {
        throw new UnsupportedOperationException("Method apply(Object) is not implemented in class " + getClass().getSimpleName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean apply(String value)
    {
        throw new UnsupportedOperationException("Method apply(String) is not implemented in class " + getClass().getSimpleName());
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String toString()
    {
        return "{" +
                "'columnIndex':'" + columnIndex + "', " +
                "'columnName':'" + columnName + "', " +
                "'expression': '" + expression + "', " +
                "'type':'" + getClass().getSimpleName()
                + "'}";
    }
}
