package com.amazonaws.athena.connector.lambda.handlers;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class GlueMetadataHandler
        extends MetadataHandler
{
    private static final String CATALOG_NAME_ENV_OVERRIDE = "glue_catalog";

    private final AWSGlue awsGlue;

    public GlueMetadataHandler(AWSGlue awsGlue, String sourceType)
    {
        super(sourceType);
        this.awsGlue = awsGlue;
    }

    protected GlueMetadataHandler(AWSGlue awsGlue,
            EncryptionKeyFactory encryptionKeyFactory,
            AWSSecretsManager secretsManager,
            String sourceType,
            String spillBucket,
            String spillPrefix)
    {
        super(encryptionKeyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
    }

    protected AWSGlue getAwsGlue()
    {
        return awsGlue;
    }

    protected String getCatalog(MetadataRequest request)
    {
        String override = System.getenv(CATALOG_NAME_ENV_OVERRIDE);
        if (override == null) {
            return request.getIdentity().getAccount();
        }
        return override;
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws Exception
    {
        return doListSchemaNames(blockAllocator, request, null);
    }

    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request, DatabaseFilter filter)
            throws Exception
    {
        GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest();
        getDatabasesRequest.setCatalogId(getCatalog(request));

        List<String> schemas = new ArrayList<>();
        String nextToken = null;
        do {
            getDatabasesRequest.setNextToken(nextToken);
            GetDatabasesResult result = awsGlue.getDatabases(getDatabasesRequest);

            for (Database next : result.getDatabaseList()) {
                if (filter == null || filter.filter(next)) {
                    schemas.add(next.getName());
                }
            }

            nextToken = result.getNextToken();
        }
        while (nextToken != null);

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        return doListTables(blockAllocator, request, null);
    }

    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request, TableFilter filter)
            throws Exception
    {
        GetTablesRequest getTablesRequest = new GetTablesRequest();
        getTablesRequest.setCatalogId(getCatalog(request));
        getTablesRequest.setDatabaseName(request.getSchemaName());

        Set<TableName> tables = new HashSet<>();
        String nextToken = null;
        do {
            getTablesRequest.setNextToken(nextToken);
            GetTablesResult result = awsGlue.getTables(getTablesRequest);

            for (Table next : result.getTableList()) {
                if (filter == null || filter.filter(next)) {
                    tables.add(new TableName(request.getSchemaName(), next.getName()));
                }
            }

            nextToken = result.getNextToken();
        }
        while (nextToken != null);

        return new ListTablesResponse(request.getCatalogName(), tables);
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        return doGetTable(blockAllocator, request, null);
    }

    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request, TableFilter filter)
            throws Exception
    {
        TableName tableName = request.getTableName();
        com.amazonaws.services.glue.model.GetTableRequest getTableRequest = new com.amazonaws.services.glue.model.GetTableRequest();
        getTableRequest.setCatalogId(getCatalog(request));
        getTableRequest.setDatabaseName(tableName.getSchemaName());
        getTableRequest.setName(tableName.getTableName());

        GetTableResult result = awsGlue.getTable(getTableRequest);
        Table table = result.getTable();

        if (filter != null && !filter.filter(table)) {
            throw new RuntimeException("No matching table found " + request.getTableName());
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        table.getParameters().entrySet().forEach(next -> schemaBuilder.addMetadata(next.getKey(), next.getValue()));

        Set<String> partitionCols = table.getPartitionKeys()
                .stream().map(next -> next.getName()).collect(Collectors.toSet());

        for (Column next : table.getStorageDescriptor().getColumns()) {
            schemaBuilder.addField(convertField(next.getName(), next.getType()));
            if (next.getComment() != null) {
                schemaBuilder.addMetadata(next.getName(), next.getComment());
            }
        }

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                schemaBuilder.build(),
                partitionCols);
    }

    @Override
    protected abstract GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
            throws Exception;

    @Override
    protected abstract GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
            throws Exception;

    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }

    public interface TableFilter
    {
        /**
         * Used to filter table results.
         *
         * @param table The table to evaluate.
         * @return True if the provided table should be in the result, False if not.
         */
        boolean filter(Table table);
    }

    public interface DatabaseFilter
    {
        /**
         * Used to filter database results.
         *
         * @param database The database to evaluate.
         * @return True if the provided database should be in the result, False if not.
         */
        boolean filter(Database database);
    }
}
