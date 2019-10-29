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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
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

public class ExampleMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandler.class);
    private static final String SOURCE_TYPE = "custom";
    private static final String SIMULATE_THROTTLES = "SIMULATE_THROTTLES";

    protected static final int NUM_PARTS_PER_SPLIT = 10;
    //This is used to illustrate how to use continuation tokens to handle partitions that generate a large number
    //of splits. This helps avoid hitting the Lambda response size limit.
    protected static final int MAX_SPLITS_PER_REQUEST = 300;

    protected static final String PARTITION_LOCATION = "location";
    protected static final String SERDE = "serde";

    private final int simulateThrottle;
    private boolean encryptionEnabled = true;
    private int count = 0;

    public ExampleMetadataHandler()
    {
        super(SOURCE_TYPE);
        this.simulateThrottle = (System.getenv(SIMULATE_THROTTLES) == null) ? 0 : Integer.parseInt(System.getenv(SIMULATE_THROTTLES));
    }

    @VisibleForTesting
    protected ExampleMetadataHandler(EncryptionKeyFactory keyFactory,
            AWSSecretsManager awsSecretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, awsSecretsManager, SOURCE_TYPE, spillBucket, spillPrefix);
        this.simulateThrottle = (System.getenv(SIMULATE_THROTTLES) == null) ? 0 : Integer.parseInt(System.getenv(SIMULATE_THROTTLES));
    }

    @VisibleForTesting
    protected void setEncryption(boolean enableEncryption)
    {
        this.encryptionEnabled = enableEncryption;
    }

    private void logCaller(FederationRequest request)
    {
        FederatedIdentity identity = request.getIdentity();
        logger.info("logCaller: account[" + identity.getAccount() + "] id[" + identity.getId() + "]  principal[" + identity.getPrincipal() + "]");
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logCaller(request);
        List<String> schemas = new ArrayList<>();
        schemas.add(ExampleTable.schemaName);
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logCaller(request);
        List<TableName> tables = new ArrayList<>();
        tables.add(new TableName(ExampleTable.schemaName, ExampleTable.tableName));

        return new ListTablesResponse(request.getCatalogName(),
                tables.stream()
                        .filter(table -> request.getSchemaName() == null || request.getSchemaName().equals(table.getSchemaName()))
                        .collect(Collectors.toList()));
    }

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

    @Override
    public void getPartitions(ConstraintEvaluator constraintEvaluator,
            BlockWriter writer,
            GetTableLayoutRequest request)
    {
        logCaller(request);

        /**
         * Now use the constraint that was in the request to do some partition pruning. Here we are just
         * generating some fake values for the partitions but in a real implementation you'd use your metastore
         * or knowledge of the actual table's physical layout to do this.
         */
        for (int year = 1990; year < 2020; year++) {
            if (constraintEvaluator.apply("year", year)) {
                for (int month = 0; month < 12; month++) {
                    if (constraintEvaluator.apply("month", month)) {
                        for (int day = 0; day < 30; day++) {
                            if (constraintEvaluator.apply("day", day)) {
                                final int dayVal = day;
                                final int monthVal = month;
                                final int yearVal = year;
                                writer.writeRows((Block block, int rowNum) -> {
                                    //these are our partition columns and were defined by the call to doGetTable(...)
                                    block.setValue("day", rowNum, dayVal);
                                    block.setValue("month", rowNum, monthVal);
                                    block.setValue("year", rowNum, yearVal);

                                    //these are additional field we added by overriding enhancePartitionSchema(...)
                                    block.setValue(PARTITION_LOCATION, rowNum, "s3://" + request.getPartitionCols());
                                    block.setValue(SERDE, rowNum, "TextInputFormat");
                                    //we wrote 1 row
                                    return 1;
                                });
                            }
                        }
                    }
                }
            }
        }
    }

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
            EncryptionKey encryptionKey = makeEncryptionKey();
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

                Split.Builder splitBuilder = Split.newBuilder(makeSpillLocation(request), encryptionEnabled ? encryptionKey : null)
                        .add(SplitProperties.LOCATION.getId(), String.valueOf(locationReader.readText()))
                        .add(SplitProperties.SERDE.getId(), String.valueOf(storageClassReader.readText()))
                        .add(SplitProperties.SPLIT_PART.getId(), String.valueOf(curPart));

                //Add the partition column values to the split's properties.
                //We are doing this because our example record reader depends on it, your specific needs
                //will likely vary
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
