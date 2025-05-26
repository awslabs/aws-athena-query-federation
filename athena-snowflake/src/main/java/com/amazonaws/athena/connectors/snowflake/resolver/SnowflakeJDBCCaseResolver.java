/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SnowflakeJDBCCaseResolver
        extends DefaultJDBCCaseResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeJDBCCaseResolver.class);
    private static final String SCHEMA_NAME_QUERY_TEMPLATE = "select * from INFORMATION_SCHEMA.SCHEMATA where lower(SCHEMA_NAME) = ?";
    private static final String TABLE_NAME_QUERY_TEMPLATE = "select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ? and lower(TABLE_NAME) = ?";
    private static final String SCHEMA_NAME_COLUMN_KEY = "SCHEMA_NAME";
    private static final String TABLE_NAME_COLUMN_KEY = "TABLE_NAME";

    public SnowflakeJDBCCaseResolver(String sourceType)
    {
        super(sourceType, FederationSDKCasingMode.ANNOTATION, FederationSDKCasingMode.NONE);
    }

    @VisibleForTesting
    public SnowflakeJDBCCaseResolver(String sourceType, FederationSDKCasingMode casingMode)
    {
        super(sourceType, casingMode, casingMode);
    }

    @Override
    protected String getCaseInsensitivelySchemaNameQueryTemplate()
    {
        return SCHEMA_NAME_QUERY_TEMPLATE;
    }

    @Override
    protected String getCaseInsensitivelySchemaNameColumnKey()
    {
        return SCHEMA_NAME_COLUMN_KEY;
    }

    @Override
    protected List<String> getCaseInsensitivelyTableNameQueryTemplate()
    {
        return List.of(TABLE_NAME_QUERY_TEMPLATE);
    }

    @Override
    protected String getCaseInsensitivelyTableNameColumnKey()
    {
        return TABLE_NAME_COLUMN_KEY;
    }

    /*
    *  Snowflake previous behavior was defaulting to upper case if no annotation found under ANNOTATION mode.
    *  Keeping this default behavior...
     */
    @Override
    protected TableName getTableNameFromQueryAnnotation(TableName table)
    {
        LOGGER.debug("Snowflake: getTableNameFromQueryAnnotation: " + table);
        // Snowflake previous behavior was defaulting to upper case if no annotation found under ANNOTATION mode.
        // Keeping this default behavior...
        if (!table.getTableName().contains("@")) {
            LOGGER.info("Snowflake: ANNOTATION enabled, but no ANNOTATION found, back to original behavior UPPER_CASE.: " + table);
            return new TableName(table.getSchemaName().toUpperCase(), table.getTableName().toUpperCase());
        }

        return super.getTableNameFromQueryAnnotation(table);
    }
}
