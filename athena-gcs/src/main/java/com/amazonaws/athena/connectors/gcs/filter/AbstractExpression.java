/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs.filter;

public abstract class AbstractExpression
{
    public final String columnName;

    protected final Object expression;

    /**
     * Constructs this with column name and an expression
     *
     * @param columnName      Name of the column
     * @param expression Expression to match
     */
    public AbstractExpression(String columnName, Object expression)
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
    public abstract boolean apply(String value);
}
