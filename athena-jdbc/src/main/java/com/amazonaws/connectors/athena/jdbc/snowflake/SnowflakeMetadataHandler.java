/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.connectors.athena.jdbc.snowflake;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.manager.JDBCUtil;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Handles metadata for Snowflake. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class SnowflakeMetadataHandler
        extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeMetadataHandler.class);

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler} instead.
     */
    public SnowflakeMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(JdbcConnectionFactory.DatabaseEngine.SNOWFLAKE));
    }

    public SnowflakeMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        super(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES));
    }

    @VisibleForTesting
    protected SnowflakeMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
            final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }

    @Override
    protected String escapeNamePattern(final String name, final String escape)
    {
        return super.escapeNamePattern(name, escape).toUpperCase();
    }

    @Override
    protected ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        LOGGER.warn(
          "[SNOWFLAKE DEBUG] catalogName: {}, SchemaName: {}, TableName: {}",
          catalogName,
          escapeNamePattern(tableHandle.getSchemaName(), escape),
          escapeNamePattern(tableHandle.getTableName(), escape)
        );
        return metadata.getColumns(
                catalogName,
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        LOGGER.warn("[SNOWFLAKE DEBUG] getPartitionSchema ");
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        return schemaBuilder.build();
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker) { }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("doGetSplits: enter - " + getSplitsRequest);

        String catalogName = getSplitsRequest.getCatalogName();
        Set<Split> splits = new HashSet<>();

        Split split = Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey()).build();
        splits.add(split);

        LOGGER.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }

    /**
     * Converts an ARRAY column's TYPE_NAME (provided by the jdbc metadata) to an ArrowType.
     * @param typeName The column's TYPE_NAME (e.g. _int4, _text, _float8, etc...)
     * @param precision Used for BigDecimal ArrowType
     * @param scale Used for BigDecimal ArrowType
     * @return ArrowType equivalent of the fieldType.
     */
    @Override
    protected ArrowType getArrayArrowTypeFromTypeName(String typeName, int precision, int scale)
    {
        switch(typeName) {
            case "_bool":
                return Types.MinorType.BIT.getType();
            case "_int2":
                return Types.MinorType.SMALLINT.getType();
            case "_int4":
                return Types.MinorType.INT.getType();
            case "_int8":
                return Types.MinorType.BIGINT.getType();
            case "_float4":
                return Types.MinorType.FLOAT4.getType();
            case "_float8":
                return Types.MinorType.FLOAT8.getType();
            case "_date":
                return Types.MinorType.DATEDAY.getType();
            case "_timestamp":
                return Types.MinorType.DATEMILLI.getType();
            case "_numeric":
                return new ArrowType.Decimal(precision, scale);
            default:
                return super.getArrayArrowTypeFromTypeName(typeName, precision, scale);
        }
    }
}
