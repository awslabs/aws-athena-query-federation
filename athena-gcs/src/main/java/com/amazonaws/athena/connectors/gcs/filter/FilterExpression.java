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

public interface FilterExpression
{
    /**
     * Index of the column in the column list
     *
     * @return Index of the underlying column of this expression
     */
    default Integer columnIndex()
    {
        throw new UnsupportedOperationException("Method column() implemented in class " + getClass().getSimpleName());
    }

    /**
     * Fluent-styled getter
     *
     * @return Returns the column name against which the expression is being evalluated
     */
    default String columnName()
    {
        throw new UnsupportedOperationException("Method column() is not implemented in class " + getClass().getSimpleName());
    }

    /**
     * Filter value that is set as expression
     * @return Constraint value that this expression will apply to evaluate
     */
    Object filterValue();

    /**
     * Applies the value to evaluate the expression
     *
     * @param value Value being examined
     * @return True if the expression evaluated to true (matches), false otherwise
     */
    boolean apply(String value);

    /**
     * Applies the value against this expression
     *
     * @param value Column value
     * @return Return if a match found, false otherwise
     */
    boolean apply(Object value);
}
