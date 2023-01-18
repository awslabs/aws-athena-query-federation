/*-
 * #%L
 * athena-gcs
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EqualsExpression
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EqualsExpression.class);

    public final String columnName;

    private final Object expression;

    /**
     * Constructs this with column name and an expression
     *
     * @param columnName      Name of the column
     * @param expression Expression to match
     */
    public EqualsExpression(String columnName, Object expression)
    {
       this.columnName = columnName;
       this.expression = expression;
    }

    /**
     * Applies the value to evaluate the expression
     *
     * @param value Value being examined
     * @return True if the expression evaluated to true (matches), false otherwise
     */
    public boolean apply(String value)
    {
        boolean evaluated = false;
        if (expression == null
                && (value == null || value.equals("null") || value.equals("NULL"))) {
            evaluated = true;
        }
        else if (expression != null) {
            evaluated = expression.toString().equals(value);
        }
        LOGGER.info("Evaluating {} for column {} is {}", value, columnName, evaluated);
        return evaluated;
    }
}
