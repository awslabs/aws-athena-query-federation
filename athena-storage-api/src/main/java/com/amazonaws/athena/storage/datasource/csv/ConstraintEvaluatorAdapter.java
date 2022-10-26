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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.common.record.RecordMetaData;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class ConstraintEvaluatorAdapter implements ConstraintEvaluator
{
    protected boolean stopEvaluation;
    protected BlockSpiller spiller;
    protected QueryStatusChecker queryStatusChecker;
    protected String[] columns;
    protected Schema schema;

    @Override
    public void recordMetadata(RecordMetaData metaData)
    {
        requireNonNull(metaData, "Csv RowProcessor was not set in CsvConstraintEvaluator");
        this.columns = Arrays.stream(metaData.headers()).map(col -> col.toLowerCase(Locale.ROOT)).collect(Collectors.toList()).toArray(String[]::new);
    }

    @Override
    public RowProcessor processor()
    {
        throw new UnsupportedOperationException("Operation processor not implemented");
    }

    @Override
    public void withSpillerAndStatusChecker(BlockSpiller spiller, QueryStatusChecker queryStatusChecker)
    {
        this.spiller = spiller;
        this.queryStatusChecker = queryStatusChecker;
    }

    @Override
    public void setSchema(Schema schema)
    {
        this.schema = schema;
    }

    @Override
    public void stop()
    {
        this.stopEvaluation = true;
    }

    @Override
    public void resume()
    {
        this.stopEvaluation = false;
    }
}
