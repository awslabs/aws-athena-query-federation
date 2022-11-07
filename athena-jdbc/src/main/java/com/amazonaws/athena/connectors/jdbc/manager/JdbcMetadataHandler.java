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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.connection.RdsSecretsCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.splits.Splitter;
import com.amazonaws.athena.connectors.jdbc.splits.SplitterFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstracts JDBC metadata handler and provides common reusable metadata handling.
 */
public abstract class JdbcMetadataHandler
        extends MetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetadataHandler.class);
    private static final String SQL_SPLITS_STRING = "select min(%s), max(%s) from %s.%s;";
    private static final int DEFAULT_NUM_SPLITS = 20;
    private final JdbcConnectionFactory jdbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final SplitterFactory splitterFactory = new SplitterFactory();

    /**
     * Used only by Multiplexing handler. All calls will be delegated to respective database handler.
     */
    protected JdbcMetadataHandler(String sourceType)
    {
        super(sourceType);
        this.jdbcConnectionFactory = null;
        this.databaseConnectionConfig = null;
    }

    protected JdbcMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig.getEngine());
        this.jdbcConnectionFactory = Validate.notNull(jdbcConnectionFactory, "jdbcConnectionFactory must not be null");

        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
    }

    @VisibleForTesting
    protected JdbcMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
            final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(null, secretsManager, athena, databaseConnectionConfig.getEngine(), null, null);
        this.jdbcConnectionFactory = Validate.notNull(jdbcConnectionFactory, "jdbcConnectionFactory must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
    }

    protected JdbcConnectionFactory getJdbcConnectionFactory()
    {
        return jdbcConnectionFactory;
    }

    protected JdbcCredentialProvider getCredentialProvider()
    {
        final String secretName = databaseConnectionConfig.getSecret();
        if (StringUtils.isNotBlank(secretName)) {
            LOGGER.info("Using Secrets Manager.");
            return new RdsSecretsCredentialProvider(getSecret(secretName));
        }

        return null;
    }

    @Override
    public ListSchemasResponse doListSchemaNames(final BlockAllocator blockAllocator, final ListSchemasRequest listSchemasRequest)
            throws Exception
    {
        try (Connection connection = jdbcConnectionFactory.getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List schema names for Catalog {}", listSchemasRequest.getQueryId(), listSchemasRequest.getCatalogName());
            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), listDatabaseNames(connection));
        }
    }

    private Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws SQLException
    {
        try (ResultSet resultSet = jdbcConnection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!schemaName.equals("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
    }

    @Override
    public ListTablesResponse doListTables(final BlockAllocator blockAllocator, final ListTablesRequest listTablesRequest)
            throws Exception
    {
        try (Connection connection = jdbcConnectionFactory.getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List table names for Catalog {}, Table {}", listTablesRequest.getQueryId(), listTablesRequest.getCatalogName(), listTablesRequest.getSchemaName());
            return new ListTablesResponse(listTablesRequest.getCatalogName(),
                    listTables(connection, listTablesRequest.getSchemaName()), null);
        }
    }

    private List<TableName> listTables(final Connection jdbcConnection, final String databaseName)
            throws SQLException
    {
        try (ResultSet resultSet = getTables(jdbcConnection, databaseName)) {
            ImmutableList.Builder<TableName> list = ImmutableList.builder();
            while (resultSet.next()) {
                list.add(getSchemaTableName(resultSet));
            }
            return list.build();
        }
    }

    private ResultSet getTables(final Connection connection, final String schemaName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                null,
                new String[] {"TABLE", "VIEW", "EXTERNAL TABLE"});
    }

    private TableName getSchemaTableName(final ResultSet resultSet)
            throws SQLException
    {
        return new TableName(
                resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"));
    }

    protected String escapeNamePattern(final String name, final String escape)
    {
        if ((name == null) || (escape == null)) {
            return name;
        }
        Preconditions.checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        Preconditions.checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        String escapedName = name.replace(escape, escape + escape);
        escapedName = escapedName.replace("_", escape + "_");
        escapedName = escapedName.replace("%", escape + "%");
        return escapedName;
    }

    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        try (Connection connection = jdbcConnectionFactory.getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(), getSchema(connection, getTableRequest.getTableName(), partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
        }
    }

    private Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws SQLException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
            boolean found = false;
            while (resultSet.next()) {
                ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"));
                String columnName = resultSet.getString("COLUMN_NAME");
                if (columnType != null && SupportedTypes.isSupported(columnType)) {
                    if (columnType instanceof ArrowType.List) {
                        schemaBuilder.addListField(columnName, getArrayArrowTypeFromTypeName(
                                resultSet.getString("TYPE_NAME"),
                                resultSet.getInt("COLUMN_SIZE"),
                                resultSet.getInt("DECIMAL_DIGITS")));
                    }
                    else {
                        schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
                    }
                }
                else {
                    // Default to VARCHAR ArrowType
                    LOGGER.warn("getSchema: Unable to map type for column[" + columnName +
                            "] to a supported type, attempted " + columnType + " - defaulting type to VARCHAR.");
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, new ArrowType.Utf8()).build());
                }
                found = true;
            }

            if (!found) {
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }

            // add partition columns
            partitionSchema.getFields().forEach(schemaBuilder::addField);

            return schemaBuilder.build();
        }
    }

    private ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                catalogName,
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    /**
     * Delegates creation of partition schema to database type implementation.
     *
     * @param catalogName Athena provided catalog name.
     * @return schema. See {@link Schema}
     */
    public abstract Schema getPartitionSchema(final String catalogName);

    @Override
    public abstract void getPartitions(
            final BlockWriter blockWriter,
            final GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception;

    @Override
    public abstract GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest);

    protected List<String> getSplitClauses(final TableName tableName)
    {
        List<String> splitClauses = new ArrayList<>();
        try (Connection jdbcConnection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
                ResultSet resultSet = jdbcConnection.getMetaData().getPrimaryKeys(null, tableName.getSchemaName(), tableName.getTableName())) {
            List<String> primaryKeyColumns = new ArrayList<>();
            while (resultSet.next()) {
                primaryKeyColumns.add(resultSet.getString("COLUMN_NAME"));
            }
            if (!primaryKeyColumns.isEmpty()) {
                try (Statement statement = jdbcConnection.createStatement();
                        ResultSet minMaxResultSet = statement.executeQuery(String.format(SQL_SPLITS_STRING, primaryKeyColumns.get(0), primaryKeyColumns.get(0),
                                tableName.getSchemaName(), tableName.getTableName()))) {
                    minMaxResultSet.next(); // expecting one result row
                    Optional<Splitter> optionalSplitter = splitterFactory.getSplitter(primaryKeyColumns.get(0), minMaxResultSet, DEFAULT_NUM_SPLITS);

                    if (optionalSplitter.isPresent()) {
                        Splitter splitter = optionalSplitter.get();
                        while (splitter.hasNext()) {
                            String splitClause = splitter.nextRangeClause();
                            LOGGER.info("Split generated {}", splitClause);
                            splitClauses.add(splitClause);
                        }
                    }
                }
            }
        }
        catch (Exception ex) {
            LOGGER.warn("Unable to split data.", ex);
        }

        return splitClauses;
    }

    /**
     * Converts an ARRAY column's TYPE_NAME (provided by the jdbc metadata) to an ArrowType.
     * @param typeName The column's TYPE_NAME (e.g. _int4, _text, _float8, etc...)
     * @param precision Used for BigDecimal ArrowType
     * @param scale Used for BigDecimal ArrowType
     * @return Utf8 ArrowType (VARCHAR)
     */
    protected ArrowType getArrayArrowTypeFromTypeName(String typeName, int precision, int scale)
    {
        // Default ARRAY type is VARCHAR.
        return new ArrowType.Utf8();
    }
}
