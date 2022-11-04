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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.storage.common.FilterExpression;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.common.record.RecordMetaData;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class CsvConstraintEvaluator implements ConstraintEvaluator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvConstraintEvaluator.class);

    /**
     * Selected column int the CSV datasource
     */
    protected String[] columns;

    /**
     * An instance of {@link BlockSpiller} that spills a record when it's evaluated to be positive against all expression
     */
    protected BlockSpiller spiller;
    /**
     * Query status checker checks whether the query is still running or stopped
     */
    protected QueryStatusChecker queryStatusChecker;

    /**
     * Table schema
     */
    protected Schema schema;

    /**
     * Marker to stop evaluation. When true, it will neither apply expressions and spill, it just returns.
     * When false, it resumes evaluation process and resumes spilling
     */
    protected boolean stopEvaluation;

    /**
     * List of single clauses, usually examined with EQUAL expression
     */
    private final List<FilterExpression> singleClauses = new ArrayList<>();
    private final List<FilterExpression> in = new ArrayList<>();
    private final List<FilterExpression> or = new ArrayList<>();
    private final List<FilterExpression> and = new ArrayList<>();

    private final List<String> constraintFields = new ArrayList<>();

    public CsvConstraintEvaluator()
    {
    }

    /**
     * Sets a spiller to spill evaluated records
     *
     * @param spiller An instance of {@link BlockSpiller}
     */
    @Override
    public void withSpillerAndStatusChecker(BlockSpiller spiller, QueryStatusChecker queryStatusChecker)
    {
        this.spiller = spiller;
        this.queryStatusChecker = queryStatusChecker;
    }

    /**
     * Stops evaluation and spilling
     */
    public void stop()
    {
        LOGGER.debug("Stopping expression evaluation. Current value is {} ", this.stopEvaluation);
        this.stopEvaluation = true;
    }

    /**
     * Resumes evaluation and spilling
     */
    public void resume()
    {
        LOGGER.debug("Resuming expression evaluation. Current value is {}", this.stopEvaluation);
        this.stopEvaluation = false;
    }

    /**
     * Sets the Table schema
     *
     * @param schema An instance of {@link Schema}
     */
    public void setSchema(Schema schema)
    {
        this.schema = schema;
    }

    /**
     * Checks to see if this evaluator has anything to apply
     *
     * @return True when it has something to evaluate, false otherwise
     */
    public boolean isEmptyEvaluator()
    {
        return singleClauses.isEmpty() && in.isEmpty()
                && or.isEmpty() && and.isEmpty();
    }

    /**
     * Retrieves the selected column from CSV and sets the column names into a String array
     *
     * @param metaData An instance of {@link RecordMetaData}
     */
    public void recordMetadata(RecordMetaData metaData)
    {
        requireNonNull(metaData, "Csv RowProcessor was not set in CsvConstraintEvaluator");
        this.columns = Arrays.stream(metaData.headers()).map(col -> col.toLowerCase(Locale.ROOT)).collect(Collectors.toList()).toArray(String[]::new);
    }

    /**
     * Returns the underlying row processor, that essentially evaluates when a row is read by the CsvParser
     *
     * @return An instance of {@link RowProcessor}
     */
    public RowProcessor processor()
    {
        return this.new CsvRowProcessor();
    }

    /**
     * Add an expression into the list of in list
     *
     * @param expression An instance of {@link FilterExpression}
     */
    protected void addToIn(FilterExpression expression)
    {
        in.add(expression);
    }

    /**
     * @return List of {@link FilterExpression}
     */
    protected List<FilterExpression> in()
    {
        return in;
    }

    /**
     * Add an expression into the list of or list
     *
     * @param expression An instance of {@link FilterExpression}
     */
    protected void addToOr(FilterExpression expression)
    {
        or.add(expression);
    }

    /**
     * @return List of {@link FilterExpression}
     */
    protected List<FilterExpression> or()
    {
        return or;
    }

    /**
     * Add an expression into the list of and list
     *
     * @param expression An instance of {@link FilterExpression}
     */
    protected void addToAnd(FilterExpression expression)
    {
        if (!and.contains(expression)) {
            and.add(expression);
        }
    }

    @Override
    public String toString()
    {
        return "{" +
                "'columns':" + Arrays.toString(columns)
                + ", 'singleClauses':" + singleClauses
                + ", 'in':" + in
                + ", 'or':" + or
                + ", 'and':" + and
                + ", 'and':" + and
                + "}";
    }

    // helpers

    /**
     * Evaluates and expressions
     *
     * @param columnName Name of the column whose value will be evaluated from one or more rows
     * @param value      Value to be examined
     * @return True if expression matches with the value, false otherwise
     */
    private boolean doAnd(String columnName, String value)
    {
        if (and.isEmpty()) {
            return true;
        }
        for (FilterExpression expression : and) {
            if (expression.columnName().equals(columnName)) {
                if (!expression.apply(value)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Evaluates and expressions
     *
     * @param columnName Name of the column whose value will be evaluated from one or more rows
     * @param value      Value to be examined
     * @return True if expression matches with the value, false otherwise
     */
    private boolean doOr(String columnName, String value)
    {
        if (or.isEmpty()) {
            return true;
        }
        for (FilterExpression expression : or) {
            if (expression.columnName().equals(columnName)) {
                if (expression.apply(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Evaluates expressions for equality
     *
     * @param columnName Name of the column whose value will be evaluated from one or more rows
     * @param value      Value to be examined
     * @return True if expression matches with the value, false otherwise
     */
    private boolean doMatchExpressions(String columnName, String value)
    {
        if (singleClauses.isEmpty()) {
            return true;
        }
        boolean matched;
        for (FilterExpression expression : singleClauses) {
            if (expression.columnName().equals(columnName)) {
                matched = expression.apply(value);
                if (!matched) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Row processor that process each and every record while the row is being parsed by the CsvParser. During processing,
     * the processor evaluations all expressions to check matches. If all expression evaluated to true, it spills the records, otherwise ignore (avoid assembling)
     */
    private class CsvRowProcessor extends AbstractRowProcessor
    {
        @Override
        public void rowProcessed(String[] row, ParsingContext context)
        {
            if (!queryStatusChecker.isQueryRunning()
                    || stopEvaluation || spiller == null) {
                return;
            }
            boolean matched;
            Map<String, Object> fieldValueMap = new HashMap<>();
            for (int i = 0; i < row.length; i++) {
                String value = row[i];
                String columnName = columns[i];
                if (!constraintFields.contains(columnName)) {
                    constraintFields.add(columnName);
                }
                matched = doMatchExpressions(columnName, value);
                if (!matched
                        || !doAnd(columnName, value)
                        || !doOr(columnName, value)) {
                    return;
                }
                fieldValueMap.put(columnName, value);
            }
            spiller.writeRows((Block block, int rowNum) -> {
                boolean isMatched = true;
                for (final Field field : schema.getFields()) {
                    Object fieldValue = fieldValueMap.get(field.getName());
                    isMatched &= block.offerValue(field.getName(), rowNum, fieldValue);
                }
                return isMatched ? 1 : 0;
            });
        }
    }
}
