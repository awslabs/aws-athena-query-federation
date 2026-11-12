/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.TableName;

import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_QUOTE_CHARACTER;

/**
 * Builds Impala-safe quoted identifiers for dynamic SQL. JDBC {@code ?} placeholders apply to values,
 * not table references, so identifiers must be quoted and escaped explicitly.
 */
public class ImpalaUtils
{
    private ImpalaUtils()
    {
    }

    /**
     * Wraps a single identifier in backticks; doubles any embedded backticks.
     */
    public static String quoteIdentifier(String identifier)
    {
        String escaped = identifier.replace(IMPALA_QUOTE_CHARACTER, IMPALA_QUOTE_CHARACTER + IMPALA_QUOTE_CHARACTER);
        return IMPALA_QUOTE_CHARACTER + escaped + IMPALA_QUOTE_CHARACTER;
    }

    /**
     * {@code schema.table} for metadata statements, upper-casing each segment then quoting so names
     * cannot break out of identifier context.
     */
    public static String qualifiedTableForMetadataSql(TableName tableName)
    {
        return quoteIdentifier(tableName.getSchemaName().toUpperCase())
                + "." + quoteIdentifier(tableName.getTableName().toUpperCase());
    }
}
