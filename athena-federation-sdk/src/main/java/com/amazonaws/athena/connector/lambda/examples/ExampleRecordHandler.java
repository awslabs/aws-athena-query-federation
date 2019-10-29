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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
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

public class ExampleRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandler.class);
    private static final String SOURCE_TYPE = "custom";
    private static final String NUM_ROWS_PER_SPLIT = "NUM_ROWS_PER_SPLIT";
    private static final String SIMULATE_THROTTLES = "SIMULATE_THROTTLES";

    private final AmazonS3 amazonS3;
    private int numRowsPerSplit = 400_000;

    private final int simulateThrottle;
    private int count = 0;

    public ExampleRecordHandler()
    {
        this(AmazonS3ClientBuilder.standard().build(), AWSSecretsManagerClientBuilder.standard().build());
        if (System.getenv(NUM_ROWS_PER_SPLIT) != null) {
            numRowsPerSplit = Integer.parseInt(System.getenv(NUM_ROWS_PER_SPLIT));
        }
    }

    @VisibleForTesting
    protected ExampleRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager)
    {
        super(amazonS3, secretsManager, SOURCE_TYPE);
        this.amazonS3 = amazonS3;
        this.simulateThrottle = (System.getenv(SIMULATE_THROTTLES) == null) ? 0 : Integer.parseInt(System.getenv(SIMULATE_THROTTLES));
    }

    protected void setNumRows(int numRows)
    {
        this.numRowsPerSplit = numRows;
    }

    private void logCaller(FederationRequest request)
    {
        FederatedIdentity identity = request.getIdentity();
        logger.info("logCaller: account[" + identity.getAccount() + "] id[" + identity.getId() + "]  principal[" + identity.getPrincipal() + "]");
    }

    protected void onPing(PingRequest request)
    {
        logCaller(request);
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest request)
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
            final int seed = i;
            spiller.writeRows((Block block, int rowNum) -> {
                //This is just filling the row with random data and then partition values that match the split
                //in a real implementation you would read your real data.
                boolean rowMatched = makeRandomRow(constraintEvaluator, block, rowNum, seed);
                addPartitionColumns(request.getSplit(), block, rowNum);
                return rowMatched ? 1 : 0;
            });
        }
    }

    private void addPartitionColumns(Split split, Block block, int blockRow)
    {
        for (String nextPartition : ExampleMetadataHandler.ExampleTable.schema.getCustomMetadata().get("partitionCols").split(",")) {
            FieldVector vector = block.getFieldVector(nextPartition);
            if (vector != null) {
                switch (vector.getMinorType()) {
                    case INT:
                    case UINT2:
                    case BIGINT:
                        BlockUtils.setValue(vector, blockRow, Integer.valueOf(split.getProperty(nextPartition)));
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
    private boolean makeRandomRow(ConstraintEvaluator constraintEvaluator, Block block, int blockRow, int seed)
    {
        Set<String> partitionCols = new HashSet<>();
        String partitionColsMetadata = block.getSchema().getCustomMetadata().get("partitionCols");
        if (partitionColsMetadata != null) {
            partitionCols.addAll(Arrays.asList(partitionColsMetadata.split(",")));
        }

        boolean matches = true;
        for (Field next : block.getSchema().getFields()) {
            FieldVector vector = block.getFieldVector(next.getName());
            if (!partitionCols.contains(next.getName())) {
                if (!matches) {
                    return false;
                }
                boolean negative = seed % 2 == 1;
                switch (vector.getMinorType()) {
                    case INT:
                        int iVal = seed * (negative ? -1 : 1);
                        matches &= constraintEvaluator.apply(vector.getField().getName(), iVal);
                        BlockUtils.setValue(vector, blockRow, iVal);
                        break;
                    case DATEMILLI:
                        matches &= constraintEvaluator.apply(vector.getField().getName(), 100_000L);
                        BlockUtils.setValue(vector, blockRow, 100_000L);
                        break;
                    case DATEDAY:
                        matches &= constraintEvaluator.apply(vector.getField().getName(), 100_000);
                        BlockUtils.setValue(vector, blockRow, 100_000);
                        break;
                    case TINYINT:
                    case SMALLINT:
                        int stVal = (seed % 4) * (negative ? -1 : 1);
                        matches &= constraintEvaluator.apply(vector.getField().getName(), stVal);
                        BlockUtils.setValue(vector, blockRow, stVal);
                        break;
                    case UINT1:
                    case UINT2:
                    case UINT4:
                    case UINT8:
                        int uiVal = seed % 4;
                        matches &= constraintEvaluator.apply(vector.getField().getName(), uiVal);
                        BlockUtils.setValue(vector, blockRow, uiVal);
                        break;
                    case FLOAT4:
                        float fVal = seed * 1.1f * (negative ? -1 : 1);
                        matches &= constraintEvaluator.apply(vector.getField().getName(), fVal);
                        BlockUtils.setValue(vector, blockRow, fVal);
                        break;
                    case FLOAT8:
                    case DECIMAL:
                        double d8Val = seed * 1.1D * (negative ? -1 : 1);
                        matches &= constraintEvaluator.apply(vector.getField().getName(), d8Val);
                        BlockUtils.setValue(vector, blockRow, d8Val);
                        break;
                    case BIT:
                        boolean bVal = seed % 2 == 0;
                        matches &= constraintEvaluator.apply(vector.getField().getName(), bVal);
                        BlockUtils.setValue(vector, blockRow, bVal);
                        break;
                    case BIGINT:
                        long lVal = seed * 1L * (negative ? -1 : 1);
                        matches &= constraintEvaluator.apply(vector.getField().getName(), lVal);
                        BlockUtils.setValue(vector, blockRow, lVal);
                        break;
                    case VARCHAR:
                        String vVal = "VarChar" + seed;
                        matches &= constraintEvaluator.apply(vector.getField().getName(), vVal);
                        BlockUtils.setValue(vector, blockRow, vVal);
                        break;
                    case VARBINARY:
                        byte[] binaryVal = ("VarChar" + seed).getBytes();
                        matches &= constraintEvaluator.apply(vector.getField().getName(), binaryVal);
                        BlockUtils.setValue(vector, blockRow, binaryVal);
                        break;
                    case LIST:
                        //This is setup for the specific kinds of lists we have in our example schema,
                        //it is not universal. List<String> and List<Struct{string,bigint}> is what
                        //this block supports.
                        Field child = vector.getField().getChildren().get(0);
                        List<Object> value = new ArrayList<>();
                        switch (Types.getMinorTypeForArrowType(child.getType())) {
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
                                throw new RuntimeException(vector.getMinorType() + " is not supported");
                        }
                        BlockUtils.setComplexValue(vector, blockRow, FieldResolver.DEFAULT, value);
                        break;
                    default:
                        throw new RuntimeException(vector.getMinorType() + " is not supported");
                }
            }
        }
        return matches;
    }
}
