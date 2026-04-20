/*-
 * #%L
 * athena-teradata
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.teradata;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Custom SQL dialect for Teradata that extends Calcite's {@link org.apache.calcite.sql.dialect.TeradataSqlDialect}
 * to handle Teradata-specific SQL generation for Athena query federation.
 *
 * <p>This dialect overrides the default OFFSET/FETCH unparsing behavior because Teradata does not
 * support standard SQL LIMIT, OFFSET, or FETCH NEXT clauses. Athena's query federation framework
 * uses Calcite to generate SQL pushed down to the source database, and without this override,
 * Calcite would emit OFFSET/FETCH syntax that Teradata cannot execute.</p>
 *
 * @see TeradataQueryStringBuilder#getSqlDialect()
 * @see TeradataQueryStringBuilder#appendLimitOffset(com.amazonaws.athena.connector.lambda.domain.Split, com.amazonaws.athena.connector.lambda.domain.predicate.Constraints)
 */
public class TeradataSqlDialect extends org.apache.calcite.sql.dialect.TeradataSqlDialect
{
    /**
     * Constructs a new {@code TeradataSqlDialect} using the {@link #DEFAULT_CONTEXT} from
     * Calcite's built-in Teradata dialect.
     */
    public TeradataSqlDialect()
    {
        super(DEFAULT_CONTEXT);
    }

    /**
     * No-op override that suppresses OFFSET/FETCH clause generation.
     *
     * <p>Teradata does not support the standard SQL OFFSET/FETCH NEXT syntax. This method
     * intentionally writes nothing to the {@link SqlWriter}, preventing Calcite from emitting
     * unsupported clauses in the pushed-down query.</p>
     *
     * @param writer the SQL writer building the output query
     * @param offset the OFFSET node (ignored)
     * @param fetch  the FETCH node (ignored)
     */
    @Override
    public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
                                   @Nullable SqlNode fetch)
    {
        // No-op: Teradata does not support LIMIT/OFFSET/FETCH
    }
}
