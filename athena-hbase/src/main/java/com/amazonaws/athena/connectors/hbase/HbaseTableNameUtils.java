/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

/**
 * This class helps with resolving the differences in casing between HBase and Presto. Presto expects all
 * databases, tables, and columns to be lower case. This class allows us to resolve HBase tables
 * which may have captial letters in them without issue. It does so by fetching all table names and doing
 * a case insensitive search over them. It will first try to do a targeted get to reduce the penalty for
 * tables which don't have capitalization.
 * 
 * Modeled off of DynamoDBTableResolver.java
 *
 * TODO add caching
 */
public final class HbaseTableNameUtils
{
    //The HBase namespce qualifier character which commonly separates namespaces and column families from tables and columns.
    protected static final String NAMESPACE_QUALIFIER = ":";
    protected static final String ENABLE_CASE_INSENSITIVE_MATCH = "enable_case_insensitive_match";
    private static final Logger logger = LoggerFactory.getLogger(HbaseTableNameUtils.class);

    private HbaseTableNameUtils() {}
    
    /**
     * Helper which goes from a schema and table name to an HBase table name string
     * @param schema a schema name
     * @param table the name of the table
     * @return
     */
   public static String getQualifiedTableName(String schema, String table)
   {
       String namespacePrefix = schema + NAMESPACE_QUALIFIER;
       return table.startsWith(namespacePrefix) ? table : namespacePrefix + table;
   }

    /**
     * Helper which goes from an Athena Federation SDK TableName to an HBase table name string.
     *
     * @param tableName An Athena Federation SDK TableName.
     * @return The corresponding HBase table name string.
     */
    public static String getQualifiedTableName(TableName tableName)
    {
        return getQualifiedTableName(tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * Helper which goes from a schema and table name to an HBase TableName
     * @param schema the schema name
     * @param table the name of the table
     * @return The corresponding HBase TableName
     */
    public static org.apache.hadoop.hbase.TableName getQualifiedTable(String schema, String table)
    {
        return org.apache.hadoop.hbase.TableName.valueOf(getQualifiedTableName(schema, table));
    }

    /**
     * Helper which goes from an Athena Federation SDK TableName to an HBase TableName.
     *
     * @param tableName An Athena Federation SDK TableName.
     * @return The corresponding HBase TableName.
     */
    public static org.apache.hadoop.hbase.TableName getQualifiedTable(TableName tableName)
    {
        return org.apache.hadoop.hbase.TableName.valueOf(getQualifiedTableName(tableName));
    }

    /**
     * Gets the hbase table name from Athena table name. This is to allow athena to query uppercase table names
     * (since athena does not support them). If an hbase table name is found with the athena table name, it is returned.
     * Otherwise, tryCaseInsensitiveSearch is used to find the corresponding hbase table.
     *
     * @param tableName the case insensitive table name
     * @return the hbase table name
     */
    public static org.apache.hadoop.hbase.TableName getHbaseTableName(Map<String, String> configOptions, HBaseConnection conn, TableName athTableName)
            throws IOException
    {
        if (!isCaseInsensitiveMatchEnable(configOptions) || !athTableName.getTableName().equals(athTableName.getTableName().toLowerCase())) {
            return getQualifiedTable(athTableName);
        }
        return tryCaseInsensitiveSearch(conn, athTableName);
    }

    /**
     * Performs a case insensitive table search by listing all table names in the schema (namespace), mapping them
     * to their lowercase transformation, and then mapping the given tableName back to a unique table. To prevent ambiguity,
     * an IllegalStateException is thrown if multiple tables map to the given tableName.
     * @param conn the HBaseConnection used to retrieve the tables
     * @param tableName The Athena TableName to find the mapping to
     * @return The HBase TableName containing the found HBase table and the Athena Schema (namespace)
     * @throws IOException 
     */
    @VisibleForTesting
    protected static org.apache.hadoop.hbase.TableName tryCaseInsensitiveSearch(HBaseConnection conn, TableName tableName)
            throws IOException
    {
        logger.info("Case Insensitive Match enabled. Searching for Table {}.", tableName.getTableName());
        Multimap<String, String> lowerCaseNameMapping = ArrayListMultimap.create();
        org.apache.hadoop.hbase.TableName[] tableNames = conn.listTableNamesByNamespace(tableName.getSchemaName());
        for (org.apache.hadoop.hbase.TableName nextTableName : tableNames) {
            lowerCaseNameMapping.put(nextTableName.getQualifierAsString().toLowerCase(Locale.ENGLISH), nextTableName.getNameAsString());
        }
        Collection<String> mappedNames = lowerCaseNameMapping.get(tableName.getTableName());
        if (mappedNames.size() != 1) {
            throw new IllegalStateException(String.format("Either no tables or multiple tables resolved from case insensitive name %s: %s", tableName.getTableName(), mappedNames));
        }
        org.apache.hadoop.hbase.TableName result = org.apache.hadoop.hbase.TableName.valueOf(mappedNames.iterator().next());
        logger.info("CaseInsensitiveMatch, TableName resolved to: {}", result.getNameAsString());
        return result;
    }

    private static boolean isCaseInsensitiveMatchEnable(Map<String, String> configOptions)
    {
        String enableCaseInsensitiveMatchEnvValue = configOptions.getOrDefault(ENABLE_CASE_INSENSITIVE_MATCH, "false").toLowerCase();
        boolean enableCaseInsensitiveMatch = enableCaseInsensitiveMatchEnvValue.equals("true");
        logger.info("{} environment variable set to: {}. Resolved to: {}",
                ENABLE_CASE_INSENSITIVE_MATCH, enableCaseInsensitiveMatchEnvValue, enableCaseInsensitiveMatch);

        return enableCaseInsensitiveMatch;
    }
    
}
