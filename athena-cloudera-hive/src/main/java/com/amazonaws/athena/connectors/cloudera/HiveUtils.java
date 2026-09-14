/*-
 * #%L
 * athena-cloudera-hive
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
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;

import static com.amazonaws.athena.connectors.cloudera.HiveConstants.HIVE_QUOTE_CHARACTER;

/**
 * Builds Hive-safe quoted identifiers for dynamic SQL. JDBC {@code ?} placeholders apply to values,
 * not table references, so identifiers must be quoted and escaped explicitly.
 */
public class HiveUtils
{
    private static final SqlDialect HIVE_SQL_DIALECT = HiveSqlDialect.DEFAULT;

    private HiveUtils()
    {
    }

    /**
     * Wraps a single identifier in backticks; doubles any embedded backticks.
     */
    public static String quoteIdentifier(String identifier)
    {
        String escaped = identifier.replace(HIVE_QUOTE_CHARACTER, HIVE_QUOTE_CHARACTER + HIVE_QUOTE_CHARACTER);
        return HIVE_QUOTE_CHARACTER + escaped + HIVE_QUOTE_CHARACTER;
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

    /**
     * Single-quoted pattern for {@code SHOW TABLE EXTENDED IN ... LIKE '...'}. Hive string literals
     * accept both quote-doubling and C-style backslash escapes. Backslashes are escaped first, then
     * {@link HiveSqlDialect#quoteStringLiteral(String)} applies Hive dialect quoting rules. The name
     * is upper-cased to match prior connector behavior.
     */
    public static String likePatternLiteral(String pattern)
    {
        String escaped = pattern.toUpperCase().replace("\\", "\\\\");
        return HIVE_SQL_DIALECT.quoteStringLiteral(escaped);
    }
}
