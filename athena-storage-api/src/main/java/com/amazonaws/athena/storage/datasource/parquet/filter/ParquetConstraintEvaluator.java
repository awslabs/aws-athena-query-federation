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
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class ParquetConstraintEvaluator implements ConstraintEvaluator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetConstraintEvaluator.class);

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
        boolean evaluate = doAnd(columnIndex, value);
        LOGGER.info("ParquetConstraintEvaluator.evaluate returning {}", evaluate);
        return evaluate;
    }

    @Override
    public List<FilterExpression> getExpressions()
    {
        return ImmutableList.copyOf(and);
    }

    public void addToAnd(FilterExpression expression)
    {
        if (!and.contains(expression)) {
            if (!columnIndices.contains(expression.columnIndex())) {
                LOGGER.info("Adding parquet expression {}", expression);
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
