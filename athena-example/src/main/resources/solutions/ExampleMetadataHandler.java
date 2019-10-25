/*-
 * #%L
 * athena-example
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
package com.amazonaws.connectors.athena.example;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
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
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example Metadatahandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with metadata about the schemas (aka databases),
 * tables, and table partitions that your source contains. Lastly, this class tells Athena how to split up reads against
 * this source. This gives you control over the level of performance and parallelism your source can support.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class ExampleMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandler.class);

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    private static final String SOURCE_TYPE = "example";

    public ExampleMetadataHandler()
    {
        super(SOURCE_TYPE);
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
    protected ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);

        Set<String> schemas = new HashSet<>();
        schemas.add("schema1");

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
    protected ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);

        List<TableName> tables = new ArrayList<>();
        tables.add(new TableName(request.getSchemaName(), "table1"));

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
    protected GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logger.info("doGetTable: enter - " + request);

        Set<String> partitionColNames = new HashSet<>();

        partitionColNames.add("year");
        partitionColNames.add("month");
        partitionColNames.add("day");

        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();
        tableSchemaBuilder.addIntField("year")
                .addIntField("month")
                .addIntField("day")
                .addStringField("account_id")
                .addStructField("transaction")
                .addChildField("transaction", "id", Types.MinorType.INT.getType())
                .addChildField("transaction", "completed", Types.MinorType.BIT.getType())
                //Metadata who's name matches a column name
                //is interpreted as the description of that
                //column when you run "show tables" queries.
                .addMetadata("year", "The year that the payment took place in.")
                .addMetadata("month", "The month that the payment took place in.")
                .addMetadata("day", "The day that the payment took place in.")
                .addMetadata("account_id", "The account_id used for this payment.")
                .addMetadata("transaction", "The payment transaction details.")
                //This metadata field is for our own use, Athena will ignore and pass along fields it doesn't expect.
                //we will use this later when we implement doGetTableLayout(...)
                .addMetadata("partitionCols", "year,month,day");

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @return A GetTableLayoutResponse which primarily contains:
     * 1. An Apache Arrow Block with 0 or more partitions to read. 0 partitions implies there are 0 rows to read.
     * 2. Set<String> of partition column names which should correspond to columns in your Apache Arrow Block.
     * @note Partitions are opaque to Amazon Athena in that it does not understand their contents, just that it must call
     * doGetSplits(...) for each partition you return in order to determine which reads to perform and if those reads
     * can be parallelized. This means the contents of this response are more for you than they are for Athena.
     */
    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator allocator, GetTableLayoutRequest request)
            throws Exception
    {
        logger.info("doGetTableLayout: enter - " + request);

        String catalogName = request.getCatalogName();
        TableName tableName = request.getTableName();
        SchemaBuilder schemBuilder = SchemaBuilder.newBuilder();
        Set<String> partitionColNames = new HashSet<>();

        for (String nextCol : request.getProperties().get("partitionCols").split(",")) {
            partitionColNames.add(nextCol);
        }

        schemBuilder.addIntField("year")
                .addIntField("month")
                .addIntField("day");

        //Create the block that will be used in our response
        Schema partitionSchema = schemBuilder.build();
        Block partitions = allocator.createBlock(partitionSchema);

        int numPartitions = 0;
        try (ConstraintEvaluator evaluator = new ConstraintEvaluator(allocator, partitionSchema, request.getConstraints())) {
            for (int year = 2000; year < 2030; year++) {
                if (evaluator.apply("year", year)) {
                    for (int month = 1; month < 13; month++) {
                        if (evaluator.apply("month", month)) {
                            //TODO: Add partition pruning for the 'month' field (hint: copy & modify the evaluator if-clause above for 'year')
                            for (int day = 1; day < 31; day++) {
                                if (evaluator.apply("day", day)) {
                                    partitions.setValue("year", rowNum, year);
                                    partitions.setValue("month", rowNum, month);
                                    partitions.setValue("day", rowNum, day);
                                    numPartitions++;
                                }
                            }
                        }
                    }
                }
            }
        }

        partitions.setRowCount(numPartitions);

        logger.info("doGetTableLayout: exit - " + numPartitions);
        return new GetTableLayoutResponse(catalogName, tableName, partitions, partitionColNames);
    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     */
    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        logger.info("doGetSplits: enter - " + request);

        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();

        Block partitions = request.getPartitions();

        FieldReader day = partitions.getFieldReader("day");
        FieldReader month = partitions.getFieldReader("month");
        FieldReader year = partitions.getFieldReader("year");
        for (int i = 0; i < partitions.getRowCount(); i++) {
            //Set the readers to the partition row we area on
            year.setPosition(i);
            month.setPosition(i);
            day.setPosition(i);

            Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add("year", String.valueOf(year.readInteger()))
                    .add("month", String.valueOf(month.readInteger()))
                    .add("day", String.valueOf(day.readInteger()))
                    .build();

            splits.add(split);
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }
}
