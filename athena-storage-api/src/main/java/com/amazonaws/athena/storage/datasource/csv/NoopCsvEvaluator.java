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
package com.amazonaws.athena.storage.datasource.csv;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.AbstractRowProcessor;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;
import java.util.Map;

class NoopCsvEvaluator extends ConstraintEvaluatorAdapter
{
    public NoopCsvEvaluator()
    {
        super();
    }

    /**
     * Creates a row processor that does nothing but spills the processed row
     *
     * @return An instance of {@link RowProcessor}
     */
    @Override
    public RowProcessor processor()
    {
        return new AbstractRowProcessor()
        {
            /**
             * Invoked when a CSV parser parses a record using {@link CsvParser#parseNext()}
             * Usually it doesn't filter and just spilled record
             *
             * @param row A collection of field values as a single record
             * @param context An instance of CSV {@link ParsingContext}
             */
            @Override
            public void rowProcessed(String[] row, ParsingContext context)
            {
                if (NoopCsvEvaluator.this.stopEvaluation
                        || !queryStatusChecker.isQueryRunning()
                        || NoopCsvEvaluator.this.spiller == null) {
                    return;
                }
                Map<String, Object> fieldValueMap = new HashMap<>();
                for (int i = 0; i < row.length; i++) {
                    String value = row[i];
                    String columnName = columns[i];
                    fieldValueMap.put(columnName, value);
                }
                NoopCsvEvaluator.this.spiller.writeRows((Block block, int rowNum) -> {
                    boolean isMatched = true;
                    for (final Field field : NoopCsvEvaluator.this.schema.getFields()) {
                        Object fieldValue = fieldValueMap.get(field.getName());
                        isMatched &= block.offerValue(field.getName(), rowNum, fieldValue);
                    }
                    return isMatched ? 1 : 0;
                });
            }
        };
    }
}
