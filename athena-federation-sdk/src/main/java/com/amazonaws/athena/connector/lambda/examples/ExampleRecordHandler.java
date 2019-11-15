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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * All items in the "com.amazonaws.athena.connector.lambda.examples" that this class belongs to are part of an
 * 'Example' connector. We do not recommend using any of the classes in this package directly. Instead you can/should
 * copy and modify as needed.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual rows level data from our simulated
 * source. Athena will call readWithConstraint(...) on this class for each 'Split' we generated in ExampleMetadataHandler.
 * <p>
 *
 * @see com.amazonaws.athena.connector.lambda.handlers.RecordHandler
 */
public class ExampleRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandler.class);
    //used in diagnostic logging
    private static final String SOURCE_TYPE = "custom";
    //The name of the environment variable to read for the number of rows to generate per Split instead of
    //the default below.
    private static final String NUM_ROWS_PER_SPLIT = "NUM_ROWS_PER_SPLIT";
    //The name of the Lambda Environment vaiable that toggles generating simulated Throttling events to trigger Athena's
    //Congestion control logic.
    private static final String SIMULATE_THROTTLES = "SIMULATE_THROTTLES";
    //The number of rows to generate per Split
    private int numRowsPerSplit = 400_000;
    //Stores how frequently to generate a simulated throttling event.
    private final int simulateThrottle;
    //Counter that is used in conjunction with simulateThrottle to generated simulated throttling events.
    private int count = 0;

    /**
     * Default constructor used by Lambda.
     */
    public ExampleRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient());
        if (System.getenv(NUM_ROWS_PER_SPLIT) != null) {
            numRowsPerSplit = Integer.parseInt(System.getenv(NUM_ROWS_PER_SPLIT));
        }
    }

    /**
     * Full DI constructor used mostly for testing
     *
     * @param amazonS3 The AmazonS3 client to use for spills.
     * @param secretsManager The AWSSecretsManager client that can be used when attempting to resolve secrets.
     * @param athena The Athena client that can be used to fetch query termination status to fast-fail this handler.
     */
    @VisibleForTesting
    protected ExampleRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE);
        this.simulateThrottle = (System.getenv(SIMULATE_THROTTLES) == null) ? 0 : Integer.parseInt(System.getenv(SIMULATE_THROTTLES));
    }

    /**
     * Used to set the number of rows per split. This method is mostly used for testing where setting the environment
     * variable to override the default is not practical.
     *
     * @param numRows The number of rows to generate per split.
     */
    @VisibleForTesting
    protected void setNumRows(int numRows)
    {
        this.numRowsPerSplit = numRows;
    }

    /**
     * Demonstrates how you can capture the identity of the caller that ran the Athena query which triggered the Lambda invocation.
     *
     * @param request
     */
    private void logCaller(FederationRequest request)
    {
        FederatedIdentity identity = request.getIdentity();
        logger.info("logCaller: account[" + identity.getAccount() + "] id[" + identity.getId() + "]  principal[" + identity.getPrincipal() + "]");
    }

    /**
     * We use the ping signal to simply log the fact that a ping request came in.
     *
     * @param request The PingRequest.
     */
    protected void onPing(PingRequest request)
    {
        logCaller(request);
    }

    /**
     * Here we generate our simulated row data. A real connector would instead connect to the actual source and read
     * the data corresponding to the requested split.
     *  @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles applying constraints, chunking the response, encrypting, and spilling to S3.
     * @param request The ReadRecordsRequest containing the split and other details about what to read.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest request, QueryStatusChecker queryStatusChecker)
    {
        /**
         * It is important to try and throw any throttling events before writing data since Athena may not be able to
         * continue the query, due to consistency errors, if you throttle after writing data.
         */
        if (simulateThrottle > 0 && count++ % simulateThrottle == 0) {
            logger.info("readWithConstraint: throwing throttle Exception!");
            throw new FederationThrottleException("Please slow down for this simulated throttling event");
        }

        logCaller(request);
        for (int i = 0; i < numRowsPerSplit; i++) {
            if (!queryStatusChecker.isQueryRunning()) {
                return;
            }
            final int seed = i;
            spiller.writeRows((Block block, int rowNum) -> {
                //This is just filling the row with random data and then partition values that match the split
                //in a real implementation you would read your real data.
                boolean rowMatched = makeRandomRow(block, rowNum, seed);
                addPartitionColumns(request.getSplit(), block, rowNum);
                return rowMatched ? 1 : 0;
            });
        }
    }

    /**
     * Helper function that we use to ensure the partition columns values are not randomly generated and instead
     * correspond to the partition that the Split belongs to. This is important because if they do not match
     * then the rows will likely get filtered out of the result. This method is only applicable to our random
     * row data as a real connector would not have to worry about a missmatch of these values because they would
     * of course match their storage.
     *
     * @param split The Split that we are generating partition column values for.
     * @param block The Block we need to write the partition column values into.
     * @param blockRow The row twe need to write the partition column values into.
     */
    private void addPartitionColumns(Split split, Block block, int blockRow)
    {
        for (String nextPartition : ExampleMetadataHandler.ExampleTable.schema.getCustomMetadata().get("partitionCols").split(",")) {
            FieldVector vector = block.getFieldVector(nextPartition);
            if (vector != null) {
                switch (vector.getMinorType()) {
                    case INT:
                    case UINT2:
                    case BIGINT:
                        block.setValue(nextPartition, blockRow, Integer.valueOf(split.getProperty(nextPartition)));
                        break;
                    default:
                        throw new RuntimeException(vector.getMinorType() + " is not supported");
                }
            }
        }
    }

    /**
     * This should be replaced with something that actually reads useful data.
     */
    private boolean makeRandomRow(Block block, int blockRow, int seed)
    {
        Set<String> partitionCols = new HashSet<>();
        String partitionColsMetadata = block.getSchema().getCustomMetadata().get("partitionCols");
        if (partitionColsMetadata != null) {
            partitionCols.addAll(Arrays.asList(partitionColsMetadata.split(",")));
        }

        boolean matches = true;
        for (Field next : block.getSchema().getFields()) {
            String fieldName = next.getName();
            if (!partitionCols.contains(fieldName)) {
                if (!matches) {
                    return false;
                }
                boolean negative = seed % 2 == 1;
                Types.MinorType fieldType = Types.getMinorTypeForArrowType(next.getType());
                switch (fieldType) {
                    case INT:
                        int iVal = seed * (negative ? -1 : 1);
                        matches &= block.setValue(fieldName, blockRow, iVal);
                        break;
                    case DATEMILLI:
                        matches &= block.setValue(fieldName, blockRow, 100_000L);
                        break;
                    case DATEDAY:
                        matches &= block.setValue(fieldName, blockRow, 100_000);
                        break;
                    case TINYINT:
                    case SMALLINT:
                        int stVal = (seed % 4) * (negative ? -1 : 1);
                        matches &= block.setValue(fieldName, blockRow, stVal);
                        break;
                    case UINT1:
                    case UINT2:
                    case UINT4:
                    case UINT8:
                        int uiVal = seed % 4;
                        matches &= block.setValue(fieldName, blockRow, uiVal);
                        break;
                    case FLOAT4:
                        float fVal = seed * 1.1f * (negative ? -1 : 1);
                        matches &= block.setValue(fieldName, blockRow, fVal);
                        break;
                    case FLOAT8:
                    case DECIMAL:
                        double d8Val = seed * 1.1D * (negative ? -1 : 1);
                        matches &= block.setValue(fieldName, blockRow, d8Val);
                        break;
                    case BIT:
                        boolean bVal = seed % 2 == 0;
                        matches &= block.setValue(fieldName, blockRow, bVal);
                        break;
                    case BIGINT:
                        long lVal = seed * 1L * (negative ? -1 : 1);
                        matches &= block.setValue(fieldName, blockRow, lVal);
                        break;
                    case VARCHAR:
                        String vVal = "VarChar" + seed;
                        matches &= block.setValue(fieldName, blockRow, vVal);
                        break;
                    case VARBINARY:
                        byte[] binaryVal = ("VarChar" + seed).getBytes();
                        matches &= block.setValue(fieldName, blockRow, binaryVal);
                        break;
                    case LIST:
                        //This is setup for the specific kinds of lists we have in our example schema,
                        //it is not universal. List<String> and List<Struct{string,bigint}> is what
                        //this block supports.
                        Field child = block.getFieldVector(fieldName).getField().getChildren().get(0);
                        List<Object> value = new ArrayList<>();
                        Types.MinorType childType = Types.getMinorTypeForArrowType(child.getType());
                        switch (childType) {
                            case LIST:
                                List<String> list = new ArrayList<>();
                                list.add(String.valueOf(1000));
                                list.add(String.valueOf(1001));
                                list.add(String.valueOf(1002));
                                value.add(list);
                                break;
                            case STRUCT:
                                Map<String, Object> struct = new HashMap<>();
                                struct.put("varchar", "chars");
                                struct.put("bigint", 100L);
                                value.add(struct);
                                break;
                            default:
                                throw new RuntimeException(childType + " is not supported");
                        }
                        matches &= block.setComplexValue(fieldName, blockRow, FieldResolver.DEFAULT, value);
                        break;
                    default:
                        throw new RuntimeException(fieldType + " is not supported");
                }
            }
        }
        return matches;
    }
}
