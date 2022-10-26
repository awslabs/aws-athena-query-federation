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

import java.util.List;

public class AnyExpression extends AbstractCsvExpression<List<String>>
{
    /**
     * Constructor to instantiate AnyExpression expression with the column name and a collection of other expression. If any of the expressions is evaluated to true
     * the {@link AnyExpression#apply(String)} method returns true, false otherwise
     *
     * @param expressions A collection of other expression
     */
    public AnyExpression(String columnName, List<String> expression)
    {
        super(columnName, expression);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean apply(String value)
    {
        return expression.contains(value);
    }
}
