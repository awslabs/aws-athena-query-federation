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
package com.amazonaws.athena.connectors.jdbc;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandlerFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;

import java.util.Map;

/**
 * Metadata handler multiplexer that supports multiple engines e.g. MySQL, PostGreSql and Redshift in same Lambda.
 *
 * Uses catalog name and associations to database types to route operations.
 */
public class MultiplexingJdbcMetadataHandler
        extends JdbcMetadataHandler
{
    private static final int MAX_CATALOGS_TO_MULTIPLEX = 100;
    protected Map<String, JdbcMetadataHandler> metadataHandlerMap;

    static final String CATALOG_NOT_REGISTERED_ERROR_TEMPLATE = "Catalog is not supported in multiplexer. After registering the catalog in Athena, must set " +
            "'%s_connection_string' environment variable in Lambda. See JDBC connector README for further details.";

    /**
     * @param metadataHandlerMap catalog -> JdbcMetadataHandler
     */
    protected MultiplexingJdbcMetadataHandler(final AWSSecretsManager secretsManager, final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory,
            final Map<String, JdbcMetadataHandler> metadataHandlerMap, final DatabaseConnectionConfig databaseConnectionConfig)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
        this.metadataHandlerMap = Validate.notEmpty(metadataHandlerMap, "metadataHandlerMap must not be empty");

        if (this.metadataHandlerMap.size() > MAX_CATALOGS_TO_MULTIPLEX) {
            throw new RuntimeException("Max 100 catalogs supported in multiplexer.");
        }
    }

    /**
     * Initializes mux routing map. Creates a reverse index of Athena catalogs supported by a database instance. Max 100 catalogs supported currently.
     */
    protected MultiplexingJdbcMetadataHandler(JdbcMetadataHandlerFactory jdbcMetadataHandlerFactory)
    {
        super(jdbcMetadataHandlerFactory.getEngine());
        this.metadataHandlerMap = Validate.notEmpty(JDBCUtil.createJdbcMetadataHandlerMap(System.getenv(), jdbcMetadataHandlerFactory), "Could not find any delegatee.");
    }

    private void validateMultiplexer(final String catalogName)
    {
        if (this.metadataHandlerMap.get(catalogName) == null) {
            throw new RuntimeException(String.format(CATALOG_NOT_REGISTERED_ERROR_TEMPLATE, catalogName));
        }
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        validateMultiplexer(catalogName);
        return this.metadataHandlerMap.get(catalogName).getPartitionSchema(catalogName);
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
            throws Exception
    {
        validateMultiplexer(listSchemasRequest.getCatalogName());
        return this.metadataHandlerMap.get(listSchemasRequest.getCatalogName()).doListSchemaNames(blockAllocator, listSchemasRequest);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
            throws Exception
    {
        validateMultiplexer(listTablesRequest.getCatalogName());
        return this.metadataHandlerMap.get(listTablesRequest.getCatalogName()).doListTables(blockAllocator, listTablesRequest);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
            throws Exception
    {
        validateMultiplexer(getTableRequest.getCatalogName());
        return this.metadataHandlerMap.get(getTableRequest.getCatalogName()).doGetTable(blockAllocator, getTableRequest);
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        validateMultiplexer(getTableLayoutRequest.getCatalogName());
        this.metadataHandlerMap.get(getTableLayoutRequest.getCatalogName()).getPartitions(blockWriter, getTableLayoutRequest, queryStatusChecker);
    }

    @Override
    public GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest getTableLayoutRequest)
            throws Exception
    {
        validateMultiplexer(getTableLayoutRequest.getCatalogName());
        return this.metadataHandlerMap.get(getTableLayoutRequest.getCatalogName()).doGetTableLayout(blockAllocator, getTableLayoutRequest);
    }

    @Override
    public GetSplitsResponse doGetSplits(
            final BlockAllocator blockAllocator, final GetSplitsRequest getSplitsRequest)
    {
        validateMultiplexer(getSplitsRequest.getCatalogName());
        return this.metadataHandlerMap.get(getSplitsRequest.getCatalogName()).doGetSplits(blockAllocator, getSplitsRequest);
    }
}
