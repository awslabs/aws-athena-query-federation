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

import java.util.ArrayList;
import java.util.List;

public class Or implements FilterExpression
{
    private final List<FilterExpression> expressions = new ArrayList<>();

    /**
     * Sets or expressions
     *
     * @param expressions A list of {@link FilterExpression}
     */
    public Or(List<FilterExpression> expressions)
    {
        this.expressions.addAll(expressions);
    }

    @Override
    public Integer columnIndex()
    {
        throw new UnsupportedOperationException("Method column() columnIndex in Or expression");
    }

    @Override
    public String columnName()
    {
        throw new UnsupportedOperationException("Method columnName() columnIndex in Or expression");
    }

    @Override
    public boolean apply(String value)
    {
        for (FilterExpression expression : expressions) {
            if (expression.apply(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean apply(Object value)
    {
        throw new UnsupportedOperationException("Method apply(Object) columnIndex in Or expression");
    }
}
