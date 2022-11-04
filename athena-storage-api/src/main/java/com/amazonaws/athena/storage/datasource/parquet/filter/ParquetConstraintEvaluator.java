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

import com.amazonaws.athena.storage.common.FilterExpression;

import java.util.ArrayList;
import java.util.List;

public final class ParquetConstraintEvaluator implements ConstraintEvaluator
{
    private final List<Integer> columnIndices = new ArrayList<>();
    private final List<FilterExpression> and = new ArrayList<>();

    public ParquetConstraintEvaluator(List<FilterExpression> expressions)
    {
        expressions.forEach(this::addToAnd);
    }

    /**
     * {@inheritDoc}
     */
    public boolean evaluate(Integer columnIndex, Object value)
    {
        if (!columnIndices.contains(columnIndex)) {
            return true;
        }
        return doAnd(columnIndex, value);
    }

    private void addToAnd(FilterExpression expression)
    {
        if (!and.contains(expression)) {
            if (!columnIndices.contains(expression.columnIndex())) {
                columnIndices.add(expression.columnIndex());
            }
            and.add(expression);
        }
    }

    @Override
    public String toString()
    {
        return "{" +
                "'columns':" + columnIndices
                + ", 'and':" + and
                + "}";
    }

    // helpers
    private boolean doAnd(Integer columnIndex, Object value)
    {
        if (and.isEmpty()) {
            return true;
        }
        for (FilterExpression expression : and) {
            if (expression.columnIndex().equals(columnIndex)) {
                if (!expression.apply(value)) {
                    return false;
                }
            }
        }
        return true;
    }
}
