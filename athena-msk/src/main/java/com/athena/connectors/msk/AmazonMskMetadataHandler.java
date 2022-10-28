/*-
 * #%L
 * athena-msk
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
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
import com.athena.connectors.msk.trino.QueryExecutor;
import com.athena.connectors.msk.trino.TrinoRecord;
import com.athena.connectors.msk.trino.TrinoRecordSet;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class AmazonMskMetadataHandler extends MetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskMetadataHandler.class);

    private final QueryExecutor queryExecutor;

    AmazonMskMetadataHandler()
    {
        this(AmazonMskUtils.getQueryExecutor());
    }

    @VisibleForTesting
    public AmazonMskMetadataHandler(QueryExecutor runner)
    {
        super(AmazonMskConstants.KAFKA_SOURCE);
        this.queryExecutor = runner;
        LOGGER.debug(" Inside MskMetadataHandler constructor() ");
    }

    /**
     * It will list the schema name which is set to default
     *
     * @param blockAllocator     Defines the interface that should be implemented by all reference counting Apache Arrow resource allocator that are provided by this SDK
     * @param listSchemasRequest For listing schemas
     * @return listSchemasRequest
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        try {
            LOGGER.info("doListSchemaNames called with Catalog: {}", listSchemasRequest.getCatalogName());

            final List<String> schemas = new ArrayList<>();

            // for now, there is only one default schema
            schemas.add(AmazonMskConstants.KAFKA_SCHEMA);

            LOGGER.info("Found {} schemas!", schemas.size());
            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);
        }
        catch (Exception exception) {
            LOGGER.error("Error: ", exception);
            System.err.println("Source=AmazonMskMetadataHandler|Method=doListSchemaNames|message=Error occurred " + exception.getMessage());
            exception.printStackTrace();
        }
        return null;
    }

    /**
     * List all the tables present
     *
     * @param blockAllocator    Defines the interface that should be implemented by all reference counting Apache Arrow resource allocator that are provided by this SDK
     * @param listTablesRequest For listing tables
     * @return listTablesRequest
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        try {
            LOGGER.info("doListTables called with request {}:{}", listTablesRequest.getCatalogName(), listTablesRequest.getSchemaName());
            List<TableName> tableNames = new ArrayList<>();
            /**
             * Querying kafka using MSK's queryExecutor object. It will list all topics present in kafka
             * as tables.
             */
            TrinoRecordSet result = queryExecutor.execute("show tables");
            for (TrinoRecord record : result) {
                LOGGER.debug("Table name: " + record.getField(0));
                if (record.getField(0) != null) {
                    tableNames.add(new TableName(AmazonMskConstants.KAFKA_SCHEMA, record.getField(0).toString().toLowerCase(Locale.ROOT)));
                }
            }
            LOGGER.info("Found {} table(s)!", tableNames.size());
            return new ListTablesResponse(listTablesRequest.getCatalogName(), tableNames, null);
        }
        catch (Exception e) {
            LOGGER.error("Error:", e);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * creates new object of GetTableResponse
     *
     * @param blockAllocator  Defines the interface that should be implemented by all reference counting Apache Arrow resource allocator that are provided by this SDK
     * @param getTableRequest List all the tables
     * @return getTableRequest
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        try {
            LOGGER.debug(" STEP  doGetTable" + getTableRequest.getTableName());
            final Schema tableSchema = getSchema(getTableRequest.getTableName().getTableName());
            LOGGER.info(tableSchema.toString());
            return new GetTableResponse(AmazonMskConstants.KAFKA_CATALOG, getTableRequest.getTableName(), tableSchema);
        }
        catch (Exception e) {
            LOGGER.error("Error: ", e);
        }
        return null;
    }

    /**
     *
     * @param blockWriter
     * @param request
     * @param queryStatusChecker
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
    {
        //do nothing here
    }

    /**
     * Partition and split logic is internally handled in trino library, from athena side we are creating single split
     *
     * @param allocator Defines the interface that should be implemented by all reference counting Apache Arrow resource allocators that are provided by this SDK
     * @param request   Get table name on which splits are done
     * @return GetSplitsResponse
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        LOGGER.debug(" Inside doGetSplits");
        String tableName = request.getTableName().getTableName();
        String sql = "select count(*) FROM " + AmazonMskConstants.KAFKA_SCHEMA + "." + tableName;
        LOGGER.debug(" Count SQL query : " + sql);

        TrinoRecordSet result = queryExecutor.execute(sql);
        long numberOfRows = 0;
        if (!result.isEmpty()) {
            TrinoRecord row = result.get(0);
            LOGGER.debug("Table '" + tableName + "' row count : " + row.getField(0));
            if (row.getField(0) != null) {
                numberOfRows = Long.parseLong(row.getField(0).toString());
            }
        }
        LOGGER.debug(" Number of Rows: " + numberOfRows);
        Set<Split> splits = new HashSet<>();
        // Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);
        // Create a new split (added to the splits set) that includes the domain and endpoint, and
        // shard information (to be used later by the Record Handler).
        Map<String, String> map = new HashMap<>();
        map.put(Long.toString(numberOfRows), Long.toString(0));
        splits.add(new Split(spillLocation, makeEncryptionKey(), map));
        return new GetSplitsResponse(request.getCatalogName(), splits);
    }

    /**
     * Get the schema of the table
     * @param tableName Name of the table
     * @return schema
     */
    private Schema getSchema(String tableName)
    {
        LOGGER.debug(" getSchema()");
        TrinoRecordSet result = queryExecutor.execute("describe " + tableName);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (TrinoRecord record : result) {
            LOGGER.debug("Field name: " + record.getField(0));
            if (record.getField(0) != null) {
                schemaBuilder.addField(record.getField(0).toString(), ArrowTypeConverter.toArrowType(record.getField(1).toString()));
                LOGGER.debug(" Field Name: " + record.getField(0).toString());
                LOGGER.debug(" Field Type: " + ArrowTypeConverter.toArrowType(record.getField(1).toString()));
            }
        }
        return schemaBuilder.build();
    }
}
