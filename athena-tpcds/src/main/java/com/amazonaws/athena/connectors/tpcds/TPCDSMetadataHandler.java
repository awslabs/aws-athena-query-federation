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
package com.amazonaws.athena.connectors.tpcds;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableSet;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles metadata requests for the Athena TPC-DS Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Provides 5 Schems, each representing a different scale factor (1,10,100,250,1000)
 * 2. Each schema has 25 TPC-DS tables
 * 3. Each table is divided into NUM_SPLITS splits * scale_factor/10
 */
public class TPCDSMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSMetadataHandler.class);

    //The name of the field that contains the number of the split. This is used for parallelizing data generation.
    protected static final String SPLIT_NUMBER_FIELD = "splitNum";
    //The name of the field that contains the total number of splits that were generated.
    //This is used for parallelizing data generation.
    protected static final String SPLIT_TOTAL_NUMBER_FIELD = "totalNumSplits";
    //The is the name of the field that contains the scale factor of the schema used in the request.
    protected static final String SPLIT_SCALE_FACTOR_FIELD = "scaleFactor";
    //The list of valid schemas which also convey the scale factor
    protected static final Set<String> SCHEMA_NAMES = ImmutableSet.of("tpcds1", "tpcds10", "tpcds100", "tpcds250", "tpcds1000");

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "tpcds";

    public TPCDSMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
    }

    @VisibleForTesting
    protected TPCDSMetadataHandler(
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
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

        List<TableName> tables = Table.getBaseTables().stream()
                .map(next -> new TableName(request.getSchemaName(), next.getName()))
                .collect(Collectors.toList());

        return new ListTablesResponse(request.getCatalogName(), tables, null);
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

        Table table = TPCDSUtils.validateTable(request.getTableName());

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (Column nextCol : table.getColumns()) {
            schemaBuilder.addField(TPCDSUtils.convertColumn(nextCol));
        }

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                schemaBuilder.build(),
                Collections.EMPTY_SET);
    }

    /**
     * We do not support partitioning at this time since Partition Pruning Performance is not part of the dimensions
     * we test using TPCDS. By making this a NoOp the Athena Federation SDK will automatically generate a single
     * placeholder partition to signal to Athena that there is indeed data that needs to be read and that it should
     * call get splits.
     *
     * @see MetadataHandler
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        //NoOp
    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s). We are generating a fixed
     * number of splits based on the scale factor.
     *
     * @see MetadataHandler
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        String catalogName = request.getCatalogName();
        int scaleFactor = TPCDSUtils.extractScaleFactor(request.getTableName().getSchemaName());
        int totalSplits = (int) Math.ceil(((double) scaleFactor / 48D));    //each split would be ~48MB

        logger.info("doGetSplits: Generating {} splits for {} at scale factor {}",
                totalSplits, request.getTableName(), scaleFactor);

        int nextSplit = request.getContinuationToken() == null ? 0 : Integer.parseInt(request.getContinuationToken());
        Set<Split> splits = new HashSet<>();
        for (int i = nextSplit; i < totalSplits; i++) {
            splits.add(Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add(SPLIT_NUMBER_FIELD, String.valueOf(i))
                    .add(SPLIT_TOTAL_NUMBER_FIELD, String.valueOf(totalSplits))
                    .add(SPLIT_SCALE_FACTOR_FIELD, String.valueOf(scaleFactor))
                    .build());
            if (splits.size() >= 1000) {
                return new GetSplitsResponse(catalogName, splits, String.valueOf(i + 1));
            }
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }
}
