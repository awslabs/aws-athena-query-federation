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

import java.util.ArrayList;
import java.util.List;

public class And implements ParquetExpression
{
    /**
     * A list of {@link ParquetExpression} instances
     */
    private final List<ParquetExpression> expressions = new ArrayList<>();

    /**
     * Construct this instance with the provided list of {@link ParquetExpression}
     *
     * @param expressions A list of {@link ParquetExpression}
     */
    public And(List<ParquetExpression> expressions)
    {
        this.expressions.addAll(expressions);
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
        for (ParquetExpression expression : expressions) {
            if (!expression.apply(value)) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public String toString()
    {
        return "And{" +
                "expressions=" + expressions +
                '}';
    }
}
