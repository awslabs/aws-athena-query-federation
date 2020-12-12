/*-
 * #%L
 *  athena-slack-member-analytics
 * %%
 * Copyright (C) 2020 Amazon Web Services
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
package com.amazonaws.connectors.athena.slack;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.Block;
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
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.connectors.athena.slack.util.SlackSchemaUtility;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.time.LocalDate;

import org.json.JSONObject;



/**
 * 
 * This class is responsible for providing Athena with metadata about the schemas (aka databases),
 * tables, and table partitions that your source contains. Lastly, this class tells Athena how to split up reads against
 * this source. This gives you control over the level of performance and parallelism your source can support.
 *
 **/
public class SlackMetadataHandler extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(SlackMetadataHandler.class);
    
    /**
     * Used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "slackanalytics";

    public SlackMetadataHandler()
    {
        super(SOURCE_TYPE);
    }

    @VisibleForTesting
    protected SlackMetadataHandler(EncryptionKeyFactory keyFactory,
            AWSSecretsManager awsSecretsManager,
            AmazonAthena athena,
            String spillBucket,
            String spillPrefix)
           
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request){
        logger.info("doListSchemaNames: enter - " + request);

        Set<String> schemas = new HashSet<>();

        schemas.add("slackanalytics");

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {
        logger.info("doListTables: enter - " + request);

        List<TableName> tables = new ArrayList<>();

        tables.add(new TableName(request.getSchemaName(), "member_analytics"));

        return new ListTablesResponse(request.getCatalogName(), tables);
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request){
        logger.info("doGetTable: Retrieving metadata for table {}.{} ", 
            request.getTableName().getSchemaName(),
            request.getTableName().getTableName());
            
        Set<String> partitionColNames = SlackSchemaUtility.getPartitions(request.getTableName().getTableName());
        
        SchemaBuilder tableSchemaBuilder = SlackSchemaUtility.getSchemaBuilder(request.getTableName().getTableName());

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @note Partitions are partially opaque to Amazon Athena in that it only understands your partition columns and
     * how to filter out partitions that do not meet the query's constraints. Any additional columns you add to the
     * partition data are ignored by Athena but passed on to calls on GetSplits.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        logger.info("getPartitions: enter");
        
        /**
         * For this implementation we are partitioning by date and registering
         * partitions for the last 30 days. At the moment the Slack Member Analytics
         * API has no method to get all possible dates. Returning the last 30
         * days for partition prooning.
         * 
         * TODO - Take this value from an env variable.
         **/
        LocalDate today = LocalDate.now();
        
        logger.info("getPartitions: enter - retrieved partitions from {} to {}", today, today.minusDays(30));

        for(int i = 1; i<=30; i++){
             
            final String dateValue = today.minusDays(i).toString();
            
            blockWriter.writeRows((Block block, int row) -> {
                boolean matched = true;
                matched &= block.setValue("date", row, dateValue);
                //If all fields matches then we wrote 1 row during this call so we return 1
                return matched ? 1 : 0;
            });
        }
        

    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, and partition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
        throws Exception
    {
        logger.info("doGetSplits: enter - split request: " + request);

        String catalogName = request.getCatalogName();
        String authToken = SlackSchemaUtility.getSlackToken();
        Set<Split> splits = new HashSet<>();

        Block partitions = request.getPartitions();

        FieldReader dateValue = partitions.getFieldReader("date");
        logger.info("doGetSplits: Partition count  " + partitions.getRowCount());
        for (int i = 0; i < partitions.getRowCount(); i++) {
            //Set the readers to the partition row we area on
            dateValue.setPosition(i);
            logger.info("doGetSplits: position={}, value={}", i, dateValue.readText());

            /**
             * TODO: For each partition in the request, create 1 or more splits. Splits
             *   are parallelizable units of work. Each represents a part of your table
             *   that needs to be read for the query. Splits are opaque to Athena aside from the
             *   spill location and encryption key. All properties added to a split are solely
             *   for your use when Athena calls your readWithContraints(...) function to perform
             *   the read. 
             * 
             *   In this example we just need to know the partition details (date).
             **/
             Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
             .add("date", dateValue.readText().toString())
             .add("authToken", authToken)
             .build();

             splits.add(split);
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }

}
