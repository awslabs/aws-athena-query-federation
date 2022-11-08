/*-
 * #%L
 * Amazon Athena Storage API
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

import com.amazonaws.athena.storage.common.FilterExpression;

import java.util.List;

public interface ConstraintEvaluator
{
    /**
     * Evaluates a value against a column's expression. The column index is specified
     *
     * @param columnIndex Index of the column
     * @param value Value to be evaluated
     * @return true if evaluated to positive, false otherwise
     */
    boolean evaluate(Integer columnIndex, Object value);

    /**
     * Provides list of all expressions the underlying evaluator has already built from constraint(s_
     * @return A list of {@link FilterExpression} instances
     */
    List<FilterExpression> getExpressions();
}
