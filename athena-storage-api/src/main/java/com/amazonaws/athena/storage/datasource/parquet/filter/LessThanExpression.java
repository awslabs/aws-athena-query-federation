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
package com.amazonaws.athena.storage.datasource.parquet.filter;

public class LessThanExpression extends LtGtEqExpression<Comparable<?>>
{
    /**
     * @param columnIndex
     * @param expression
     */
    public LessThanExpression(Integer columnIndex, Comparable<?> expression)
    {
        super(columnIndex, expression);
    }

    /**
     * {@inheritDoc}
     *
     * @param value Column value
     * @return
     */
    public boolean apply(Object value)
    {
        if (value instanceof Comparable<?>) {
            return ObjectComparator.compare(value, expression) < 0;
        }
        return false;
    }
}
