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

import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EqualsExpression extends AbstractParquetExpression<Object>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EqualsExpression.class);

    /**
     * {@inheritDoc}
     *
     * @param columnIndex
     * @param expression
     */
    public EqualsExpression(Integer columnIndex, Object expression)
    {
        super(columnIndex, null, expression);
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
        boolean evaluated;
        if (expression.getClass().equals(Text.class)) {
            evaluated = expression.toString().equals(value);
        }
        else {
            evaluated = expression.equals(value);
        }
        LOGGER.debug("Applying value {} on the following expression that evaluated to {}. value type was {}. And expression type is: {}. String val is: {}\n",
                value, evaluated, value.getClass().getName(), expression.getClass().getName(), this);
        return evaluated;
    }
}
