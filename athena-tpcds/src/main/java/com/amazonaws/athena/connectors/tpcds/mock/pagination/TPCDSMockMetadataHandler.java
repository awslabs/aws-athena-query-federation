/*-
 * #%L
 * athena-tpcds
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
package com.amazonaws.athena.connectors.tpcds.mock.pagination;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler;
import com.amazonaws.athena.connectors.tpcds.TPCDSUtils;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

/**
 * Handles metadata requests for the Athena TPC-DS Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Provides 5 Schems, each representing a different scale factor (1,10,100,250,1000)
 * 2. Each schema has 25 TPC-DS tables
 * 3. Each table is divided into NUM_SPLITS splits * scale_factor/10
 */
public class TPCDSMockMetadataHandler extends TPCDSMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSMockMetadataHandler.class);
    private static final String MOCK_TABLE_NAME_PREFIX = "mock_table_";

    protected static final Set<String> SCHEMA_NAMES = new HashSet<>();

    static {
        for (int i = 0; i < 150; i++) {
            SCHEMA_NAMES.add("tpcds" + i);
        }
    }

    public TPCDSMockMetadataHandler(Map<String, String> configOptions)
    {
        super(configOptions);
    }

    @VisibleForTesting
    protected TPCDSMockMetadataHandler(
            EncryptionKeyFactory keyFactory,
            SecretsManagerClient secretsManager,
            AthenaClient athena,
            String spillBucket,
            String spillPrefix,
            java.util.Map<String, String> configOptions)
    {
        super(keyFactory, secretsManager, athena, spillBucket, spillPrefix, configOptions);
    }

    /**
     * Returns our static list of schemas which correspond to the scale factor of the dataset we will generate.
     *
     * @see MetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);
        return new ListSchemasResponse(request.getCatalogName(), SCHEMA_NAMES);
    }

    /**
     * Used to get the list of static tables from TerraData's TPCDS generator.
     *
     * @see MetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);
        List<TableName> tableList = getTableList(request);

        int startToken = request.getNextToken() == null ? 0 : Integer.parseInt(request.getNextToken());
        int pageSize = request.getPageSize();
        String nextToken = null;

        if (request.getPageSize() != UNLIMITED_PAGE_SIZE_VALUE) {
            logger.info("Pagination starting at token {} w/ page size {}", startToken, pageSize);
            List<TableName> paginatedResults = getPaginatedList(tableList, startToken, pageSize);
            nextToken = paginatedResults.isEmpty() || paginatedResults.size() < pageSize ? null : Integer.toString(startToken + pageSize);
            logger.info("Next token is {}", nextToken);

            return new ListTablesResponse(request.getCatalogName(), paginatedResults, nextToken);
        }

        return new ListTablesResponse(request.getCatalogName(), tableList, nextToken);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table using the static
     * metadata provided by TerraData's TPCDS generator.
     *
     * @see MetadataHandler
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logger.info("doGetTable: enter - " + request);
        Table table = getTable(request.getTableName());

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (Column nextCol : table.getColumns()) {
            schemaBuilder.addField(TPCDSUtils.convertColumn(nextCol));
        }

        return new GetTableResponse(request.getCatalogName(),
                new TableName(request.getTableName().getSchemaName(), request.getTableName().getTableName()),
                schemaBuilder.build(),
                Collections.EMPTY_SET);
    }

    /*
    If mock table return it as customer schema;
     */
    private Table getTable(TableName tableName)
    {
        if (tableName.getTableName().startsWith(MOCK_TABLE_NAME_PREFIX)) {
            return Table.CUSTOMER;
        }

        return TPCDSUtils.validateTable(tableName);
    }

    private List<TableName> getPaginatedList(List<TableName> inputList, int startToken, int pageSize)
    {
        if (startToken > inputList.size()) {
            return List.of();
        }
        int endToken = Math.min(startToken + pageSize, inputList.size());

        return inputList.subList(startToken, endToken);
    }

    private List<TableName> getTableList(ListTablesRequest request)
    {
        List<TableName> collect = Table.getBaseTables().stream()
                .map(next -> new TableName(request.getSchemaName(), next.getName()))
                .collect(Collectors.toList());

        for (int i = 0; i < 100; i++) {
            collect.add(new TableName(request.getSchemaName(), MOCK_TABLE_NAME_PREFIX + i));
        }

        collect.sort(Comparator.comparing(TableName::getTableName));

        return collect;
    }
}
