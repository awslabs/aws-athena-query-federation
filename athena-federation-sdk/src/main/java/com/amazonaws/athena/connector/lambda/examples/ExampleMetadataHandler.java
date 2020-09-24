package com.amazonaws.athena.connector.lambda.examples;

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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
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
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * All items in the "com.amazonaws.athena.connector.lambda.examples" that this class belongs to are part of an
 * 'Example' connector. We do not recommend using any of the classes in this package directly. Instead you can/should
 * copy and modify as needed.
 * <p>
 * This class defined an example MetadataHandler that supports a single schema and single table which showcases most
 * of the features offered by the Amazon Athena Query Federation SDK. Some notable characteristics include:
 * 1. Highly partitioned table.
 * 2. Paginated split generation.
 * 3. S3 Spill support.
 * 4. Spill encryption using either KMS KeyFactory or LocalKeyFactory.
 * 5. A wide range of field types including complex Struct and List types.
 * <p>
 *
 * @note All schema names, table names, and column names must be lower case at this time. Any entities that are uppercase or
 * mixed case will not be accessible in queries and will be lower cased by Athena's engine to ensure consistency across
 * sources. As such you may need to handle this when integrating with a source that supports mixed case. As an example,
 * you can look at the CloudwatchTableResolver in the athena-cloudwatch module for one potential approach to this challenge.
 * <p>
 * @see MetadataHandler
 */
public class ExampleMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandler.class);
    //Used to aid in diagnostic logging
    private static final String SOURCE_TYPE = "custom";
    //The name of the Lambda Environment vaiable that toggles generating simulated Throttling events to trigger Athena's
    //Congestion control logic.
    private static final String SIMULATE_THROTTLES = "SIMULATE_THROTTLES";
    //The number of splits to generated for each Partition. Keep in mind this connector generates random data, a real
    //source is unlikely to have such a setting.
    protected static final int NUM_PARTS_PER_SPLIT = 10;
    //This is used to illustrate how to use continuation tokens to handle partitions that generate a large number
    //of splits. This helps avoid hitting the Lambda response size limit.
    protected static final int MAX_SPLITS_PER_REQUEST = 300;
    //Field name for storing partition location information.
    protected static final String PARTITION_LOCATION = "location";
    //Field name for storing an example property on our partitions and splits.
    protected static final String SERDE = "serde";

    //Stores how frequently to generate a simulated throttling event.
    private final int simulateThrottle;
    //Controls if spill encryption should be enabled or disabled.
    private boolean encryptionEnabled = true;
    //Counter that is used in conjunction with simulateThrottle to generated simulated throttling events.
    private int count = 0;

    /**
     * Default constructor used by Lambda.
     */
    public ExampleMetadataHandler()
    {
        super(SOURCE_TYPE);
        this.simulateThrottle = (System.getenv(SIMULATE_THROTTLES) == null) ? 0 : Integer.parseInt(System.getenv(SIMULATE_THROTTLES));
    }

    /**
     * Full DI constructor used mostly for testing
     *
     * @param keyFactory The EncryptionKeyFactory to use for spill encryption.
     * @param awsSecretsManager The AWSSecretsManager client that can be used when attempting to resolve secrets.
     * @param athena The Athena client that can be used to fetch query termination status to fast-fail this handler.
     * @param spillBucket The S3 Bucket to use when spilling results.
     * @param spillPrefix The S3 prefix to use when spilling results.
     */
    @VisibleForTesting
    protected ExampleMetadataHandler(EncryptionKeyFactory keyFactory,
            AWSSecretsManager awsSecretsManager,
            AmazonAthena athena,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        //Read the Lambda environment variable for controlling simulated throttles.
        this.simulateThrottle = (System.getenv(SIMULATE_THROTTLES) == null) ? 0 : Integer.parseInt(System.getenv(SIMULATE_THROTTLES));
    }

    /**
     * Used to toggle encryption during unit tests.
     *
     * @param enableEncryption
     */
    @VisibleForTesting
    protected void setEncryption(boolean enableEncryption)
    {
        this.encryptionEnabled = enableEncryption;
    }

    /**
     * Demonstrates how you can capture the identity of the caller that ran the Athena query which triggered the Lambda invocation.
     *
     * @param request
     */
    private void logCaller(FederationRequest request)
    {
        FederatedIdentity identity = request.getIdentity();
        logger.info("logCaller: account[" + identity.getAccount() + "] arn[" + identity.getArn() + "]");
    }

    /**
     * Returns a static, single schema. A connector for a real data source would likely query that source's metadata
     * to create a real list of schemas.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return The ListSchemasResponse which mostly contains the list of schemas (aka databases).
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logCaller(request);
        List<String> schemas = new ArrayList<>();
        schemas.add(ExampleTable.schemaName);
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Returns a static list of TableNames. A connector for a real data source would likely query that source's metadata
     * to create a real list of TableNames for the requested schema name.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse containing the list of available TableNames.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logCaller(request);
        List<TableName> tables = new ArrayList<>();
        tables.add(new TableName(ExampleTable.schemaName, ExampleTable.tableName));

        //The below filter for null schema is not typical, we do this to generate a specific semantic error
        //that is exercised in our unit test suite.
        return new ListTablesResponse(request.getCatalogName(),
                tables.stream()
                        .filter(table -> request.getSchemaName() == null || request.getSchemaName().equals(table.getSchemaName()))
                        .collect(Collectors.toList()));
    }

    /**
     * Retrieves a static Table schema for the example table. A connector for a real data source would likely query that
     * source's metadata to create a table definition.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse containing the definition of the table (e.g. table schema and partition columns)
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logCaller(request);
        if (!request.getTableName().getSchemaName().equals(ExampleTable.schemaName) ||
                !request.getTableName().getTableName().equals(ExampleTable.tableName)) {
            throw new IllegalArgumentException("Unknown table " + request.getTableName());
        }

        Set<String> partitionCols = new HashSet<>();
        partitionCols.add("month");
        partitionCols.add("year");
        partitionCols.add("day");
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), ExampleTable.schema, partitionCols);
    }

    /**
     * Here we inject the two additional columns we define for partition metadata. These columns are ignored by
     * Athena but passed along to our code when Athena calls GetSplits(...). If you do not require any additional
     * metadata on your partitions you may choose not to implement this function.
     *
     * @param partitionSchemaBuilder The SchemaBuilder you can use to add additional columns and metadata to the
     * partitions response.
     * @param request The GetTableLayoutResquest that triggered this call.
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        /**
         * Add any additional fields we might need to our partition response schema.
         * These additional fields are ignored by Athena but will be passed to GetSplits(...)
         * when Athena calls our lambda function to plan the distributed read of our partitions.
         */
        partitionSchemaBuilder.addField(PARTITION_LOCATION, new ArrowType.Utf8())
                .addField(SERDE, new ArrowType.Utf8());
    }

    /**
     * Our example table is partitions on year, month, day so we loop over a range of years, months, and days to generate
     * our example partitions. A connector for a real data source would likely query that source's metadata
     * to create a real list of partitions.
     *  @param writer Used to write rows (partitions) into the Apache Arrow response. The writes are automatically constrained.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker
     */
    @Override
    public void getPartitions(BlockWriter writer, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
    {
        logCaller(request);

        /**
         * Now use the constraint that was in the request to do some partition pruning. Here we are just
         * generating some fake values for the partitions but in a real implementation you'd use your metastore
         * or knowledge of the actual table's physical layout to do this.
         */
        for (int year = 1990; year < 2020; year++) {
            for (int month = 0; month < 12; month++) {
                for (int day = 0; day < 30; day++) {
                    final int dayVal = day;
                    final int monthVal = month;
                    final int yearVal = year;
                    writer.writeRows((Block block, int rowNum) -> {
                        //these are our partition columns and were defined by the call to doGetTable(...)
                        boolean matched = true;
                        matched &= block.setValue("day", rowNum, dayVal);
                        matched &= block.setValue("month", rowNum, monthVal);
                        matched &= block.setValue("year", rowNum, yearVal);

                        //these are additional field we added by overriding enhancePartitionSchema(...)
                        matched &= block.setValue(PARTITION_LOCATION, rowNum, "s3://" + request.getPartitionCols());
                        matched &= block.setValue(SERDE, rowNum, "TextInputFormat");

                        //if all fields passed then we wrote 1 row
                        return matched ? 1 : 0;
                    });
                }
            }
        }
    }

    /**
     * For each partition we generate a pre-determined number of splits based on the NUM_PARTS_PER_SPLIT setting. This
     * method also demonstrates how to handle calls for batches of partitions and also leverage this API's ability
     * to paginated. A connector for a real data source would likely query that source's metadata to determine if/how
     * to split up the read operations for a particular partition.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which contains a list of splits as an optional continuation token if we were not
     * able to generate all splits for the partitions in this batch.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        logCaller(request);
        logger.info("doGetSplits: spill location " + makeSpillLocation(request));

        /**
         * It is important to try and throw any throttling events before writing data since Athena may not be able to
         * continue the query, due to consistency errors, if you throttle after writing data.
         */
        if (simulateThrottle > 0 && count++ % simulateThrottle == 0) {
            logger.info("readWithConstraint: throwing throttle Exception!");
            throw new FederationThrottleException("Please slow down for this simulated throttling event");
        }

        ContinuationToken requestToken = ContinuationToken.decode(request.getContinuationToken());
        int partitionContd = requestToken.getPartition();
        int partContd = requestToken.getPart();

        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            //We use the makeEncryptionKey() method from our parent class to make an EncryptionKey
            EncryptionKey encryptionKey = makeEncryptionKey();

            //We prepare to read our custom metadata fields from the partition so that we can pass this info to the split(s)
            FieldReader locationReader = partitions.getFieldReader(SplitProperties.LOCATION.getId());
            locationReader.setPosition(curPartition);
            FieldReader storageClassReader = partitions.getFieldReader(SplitProperties.SERDE.getId());
            storageClassReader.setPosition(curPartition);

            //Do something to decide if this partition needs to be subdivided into multiple, possibly concurrent,
            //table scan operations (aka splits)
            for (int curPart = partContd; curPart < NUM_PARTS_PER_SPLIT; curPart++) {
                if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                    //We exceeded the number of split we want to return in a single request, return and provide
                    //a continuation token.
                    return new GetSplitsResponse(request.getCatalogName(),
                            splits,
                            ContinuationToken.encode(curPartition, curPart));
                }

                //We use makeSpillLocation(...) from our parent class to get a unique SpillLocation for each split
                Split.Builder splitBuilder = Split.newBuilder(makeSpillLocation(request), encryptionEnabled ? encryptionKey : null)
                        .add(SplitProperties.LOCATION.getId(), String.valueOf(locationReader.readText()))
                        .add(SplitProperties.SERDE.getId(), String.valueOf(storageClassReader.readText()))
                        .add(SplitProperties.SPLIT_PART.getId(), String.valueOf(curPart));

                //Add the partition column values to the split's properties.
                //We are doing this because our example record reader depends on it, your specific needs
                //will likely vary. Our example only supports a limited number of partition column types.
                for (String next : request.getPartitionCols()) {
                    FieldReader reader = partitions.getFieldReader(next);
                    reader.setPosition(curPartition);

                    switch (reader.getMinorType()) {
                        case UINT2:
                            splitBuilder.add(next, Integer.valueOf(reader.readCharacter()).toString());
                            break;
                        case UINT4:
                        case INT:
                            splitBuilder.add(next, String.valueOf(reader.readInteger()));
                            break;
                        case UINT8:
                        case BIGINT:
                            splitBuilder.add(next, String.valueOf(reader.readLong()));
                            break;
                        default:
                            throw new RuntimeException("Unsupported partition column type. " + reader.getMinorType());
                    }
                }

                splits.add(splitBuilder.build());
            }

            //part continuation only applies within a partition so we complete that partial partition and move on
            //to the next one.
            partContd = 0;
        }

        return new GetSplitsResponse(request.getCatalogName(), splits, null);
    }

    /**
     * We use the ping signal to simply log the fact that a ping request came in.
     *
     * @param request The PingRequest.
     */
    public void onPing(PingRequest request)
    {
        logCaller(request);
    }

    /**
     * We use this as our static metastore for the example implementation
     */
    protected static class ExampleTable
    {
        public static final String schemaName = "custom_source";
        public static final String tableName = "fake_table";
        public static final Schema schema;

        static {
            schema = new SchemaBuilder().newBuilder()
                    .addField("col1", new ArrowType.Date(DateUnit.DAY))
                    .addField("day", new ArrowType.Int(32, true))
                    .addField("month", new ArrowType.Int(32, true))
                    .addField("year", new ArrowType.Int(32, true))
                    .addField("col3", new ArrowType.Bool())
                    .addField("col4", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                    .addField("col5", new ArrowType.Utf8())
                    .addField("datemilli", Types.MinorType.DATEMILLI.getType())
                    .addField("int", Types.MinorType.INT.getType())
                    .addField("tinyint", Types.MinorType.TINYINT.getType())
                    .addField("smallint", Types.MinorType.SMALLINT.getType())
                    .addField("bigint", Types.MinorType.BIGINT.getType())
                    .addField("float4", Types.MinorType.FLOAT4.getType())
                    .addField("float8", Types.MinorType.FLOAT8.getType())
                    .addField("bit", Types.MinorType.BIT.getType())
                    .addField("varchar", Types.MinorType.VARCHAR.getType())
                    .addField("varbinary", Types.MinorType.VARBINARY.getType())
                    .addField("decimal", new ArrowType.Decimal(10, 2))
                    .addField("decimalLong", new ArrowType.Decimal(36, 2))
                    //Example of a List of Structs
                    .addField(
                            FieldBuilder.newBuilder("list", new ArrowType.List())
                                    .addField(
                                            FieldBuilder.newBuilder("innerStruct", Types.MinorType.STRUCT.getType())
                                                    .addStringField("varchar")
                                                    .addBigIntField("bigint")
                                                    .build())
                                    .build())
                    //Example of a List Of Lists
                    .addField(
                            FieldBuilder.newBuilder("outerlist", new ArrowType.List())
                                    .addListField("innerList", Types.MinorType.VARCHAR.getType())
                                    .build())
                    .addMetadata("partitionCols", "day,month,year")
                    .addMetadata("randomProp1", "randomPropVal1")
                    .addMetadata("randomProp2", "randomPropVal2").build();
        }

        private ExampleTable() {}
    }
}
