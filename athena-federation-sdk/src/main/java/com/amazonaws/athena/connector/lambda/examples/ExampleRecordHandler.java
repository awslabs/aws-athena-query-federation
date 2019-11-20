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
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
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
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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
     *
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles applying constraints, chunking the response, encrypting, and spilling to S3.
     * @param request The ReadRecordsRequest containing the split and other details about what to read.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest request, QueryStatusChecker queryStatusChecker)
    {
        long startTime = System.currentTimeMillis();

        /**
         * It is important to try and throw any throttling events before writing data since Athena may not be able to
         * continue the query, due to consistency errors, if you throttle after writing data.
         */
        if (simulateThrottle > 0 && count++ % simulateThrottle == 0) {
            logger.info("readWithConstraint: throwing throttle Exception!");
            throw new FederationThrottleException("Please slow down for this simulated throttling event");
        }

        logCaller(request);

        Set<String> partitionCols = new HashSet<>();
        String partitionColsMetadata = request.getSchema().getCustomMetadata().get("partitionCols");
        if (partitionColsMetadata != null) {
            partitionCols.addAll(Arrays.asList(partitionColsMetadata.split(",")));
        }

        Set<Field> requiredFields = new HashSet<>();
        for (Field next : request.getSchema().getFields()) {
            String fieldName = next.getName();
            if (!partitionCols.contains(fieldName)) {
                requiredFields.add(next);
            }
        }

        int year = Integer.valueOf(request.getSplit().getProperty("year"));
        int month = Integer.valueOf(request.getSplit().getProperty("month"));
        int day = Integer.valueOf(request.getSplit().getProperty("day"));

        GeneratedRowWriter rowWriter = new GeneratedRowWriter(getConstraintEvaluator(), year, month, day);
        for (int i = 0; i < numRowsPerSplit; i++) {
            if (!queryStatusChecker.isQueryRunning()) {
                return;
            }
            spiller.writeRows(rowWriter);
        }

        logger.info("readWithConstraint: Completed generating rows in {} ms", System.currentTimeMillis() - startTime);
    }

    private static class GeneratedRowWriter
            implements BlockWriter.RowWriter
    {
        private final List<Projector> projectors = new ArrayList<>();
        private Block block;
        private final int year;
        private final int month;
        private final int day;
        private final ConstraintEvaluator evaluator;
        private final AtomicLong seed = new AtomicLong(0);

        public GeneratedRowWriter(ConstraintEvaluator evaluator, int year, int month, int day)
        {
            this.year = year;
            this.month = month;
            this.day = day;
            this.evaluator = evaluator;
        }

        private void init(Block block)
        {
            if (this.block == null || this.block.getRowCount() != block.getRowCount()) {
                logger.info("init: Detected a new block, rebuilding projectors");
                this.block = block;
                projectors.clear();
                //TODO this doesn't actually check if the block changed, need to do that
                for (FieldVector vector : block.getFieldVectors()) {
                    Projector projector = checkAndMakePartitionCol(vector);
                    if (projector == null) {
                        projector = makeRandomValueProjector(vector);
                    }
                    projectors.add(projector);
                }
            }
        }

        @Override
        public int writeRows(Block block, int rowNum)
        {
            init(block);
            boolean negative = rowNum % 2 == 1;
            boolean matched = true;
            int rowSeed = (int) seed.getAndIncrement();
            for (Projector next : projectors) {
                matched &= next.project(rowSeed, rowNum, negative);
            }
            return matched ? 1 : 0;
        }

        private Projector checkAndMakePartitionCol(FieldVector vector)
        {
            //Handle our partition columns
            if (vector.getField().getName().equals("year")) {
                return (int seed, int rowNum, boolean negative) -> {
                    ((IntVector) vector).setSafe(rowNum, year);
                    return true;
                };
            }
            else if (vector.getField().getName().equals("month")) {
                return (int seed, int rowNum, boolean negative) -> {
                    ((IntVector) vector).setSafe(rowNum, month);
                    return true;
                };
            }
            else if (vector.getField().getName().equals("day")) {
                return (int seed, int rowNum, boolean negative) -> {
                    ((IntVector) vector).setSafe(rowNum, day);
                    return true;
                };
            }
            return null;
        }

        private Projector makeRandomValueProjector(FieldVector fieldVector)
        {
            Field field = fieldVector.getField();
            String fieldName = field.getName();
            Optional<ConstraintProjector> constraintProjector = evaluator.makeConstraintProjector(fieldName);
            ConstraintProjector constraint = constraintProjector.isPresent() ? constraintProjector.get() : (Object value) -> true;
            Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
            switch (fieldType) {
                case INT:
                    return (int seed, int rowNum, boolean negative) -> {
                        int iVal = seed * (negative ? -1 : 1);
                        ((IntVector) fieldVector).setSafe(rowNum, iVal);
                        return constraint.apply(iVal);
                    };
                case DATEMILLI:
                    return (int seed, int rowNum, boolean negative) -> {
                        int iVal = seed * (negative ? -1 : 1);
                        ((DateMilliVector) fieldVector).setSafe(rowNum, iVal);
                        return constraint.apply(iVal);
                    };
                case DATEDAY:
                    return (int seed, int rowNum, boolean negative) -> {
                        int iVal = seed * (negative ? -1 : 1);
                        ((DateDayVector) fieldVector).setSafe(rowNum, iVal);
                        return constraint.apply(iVal);
                    };
                case TINYINT:
                    return (int seed, int rowNum, boolean negative) -> {
                        int stVal = (seed % 4) * (negative ? -1 : 1);
                        ((TinyIntVector) fieldVector).setSafe(rowNum, stVal);
                        return constraint.apply(stVal);
                    };
                case SMALLINT:
                    return (int seed, int rowNum, boolean negative) -> {
                        int stVal = (seed % 4) * (negative ? -1 : 1);
                        ((SmallIntVector) fieldVector).setSafe(rowNum, stVal);
                        return constraint.apply(stVal);
                    };

                case UINT1:
                    return (int seed, int rowNum, boolean negative) -> {
                        int uVal = seed % 4;
                        ((UInt1Vector) fieldVector).setSafe(rowNum, uVal);
                        return constraint.apply(uVal);
                    };
                case UINT2:
                    return (int seed, int rowNum, boolean negative) -> {
                        int uVal = seed % 4;
                        ((UInt2Vector) fieldVector).setSafe(rowNum, uVal);
                        return constraint.apply(uVal);
                    };
                case UINT4:
                    return (int seed, int rowNum, boolean negative) -> {
                        int uVal = seed % 4;
                        ((UInt4Vector) fieldVector).setSafe(rowNum, uVal);
                        return constraint.apply(uVal);
                    };
                case UINT8:
                    return (int seed, int rowNum, boolean negative) -> {
                        int uVal = seed % 4;
                        ((UInt8Vector) fieldVector).setSafe(rowNum, uVal);
                        return constraint.apply(uVal);
                    };
                case FLOAT4:
                    return (int seed, int rowNum, boolean negative) -> {
                        float fVal = seed * 1.1f * (negative ? -1 : 1);
                        ((Float4Vector) fieldVector).setSafe(rowNum, fVal);
                        return constraint.apply(fVal);
                    };
                case FLOAT8:
                    return (int seed, int rowNum, boolean negative) -> {
                        double d8Val = seed * 1.1D;
                        ((Float8Vector) fieldVector).setSafe(rowNum, d8Val);
                        return constraint.apply(d8Val);
                    };
                case DECIMAL:
                    return (int seed, int rowNum, boolean negative) -> {
                        double d8Val = seed * 1.1D * (negative ? -1 : 1);
                        DecimalVector dVector = ((DecimalVector) fieldVector);
                        BigDecimal bdVal = new BigDecimal(d8Val);
                        bdVal = bdVal.setScale(dVector.getScale(), RoundingMode.HALF_UP);
                        dVector.setSafe(rowNum, bdVal);
                        return constraint.apply(bdVal);
                    };
                case BIT:
                    return (int seed, int rowNum, boolean negative) -> {
                        int bVal = seed % 2;
                        ((BitVector) fieldVector).setSafe(rowNum, bVal);
                        return constraint.apply(bVal);
                    };
                case BIGINT:
                    return (int seed, int rowNum, boolean negative) -> {
                        long lVal = seed * 1L * (negative ? -1 : 1);
                        ((BigIntVector) fieldVector).setSafe(rowNum, lVal);
                        return constraint.apply(lVal);
                    };
                case VARCHAR:
                    return (int seed, int rowNum, boolean negative) -> {
                        String vVal = "VarChar" + seed;
                        ((VarCharVector) fieldVector).setSafe(rowNum, (vVal).getBytes(Charsets.UTF_8));
                        return constraint.apply(vVal);
                    };
                case VARBINARY:
                    return (int seed, int rowNum, boolean negative) -> {
                        String vVal = "VarChar" + seed;
                        ((VarBinaryVector) fieldVector).setSafe(rowNum, (vVal).getBytes(Charsets.UTF_8));
                        return constraint.apply((vVal).getBytes(Charsets.UTF_8));
                    };
                case LIST:
                    //This is setup for the specific kinds of lists we have in our example schema,
                    //it is not universal. List<String> and List<Struct{string,bigint}> is what
                    //this block supports.
                    Field child = block.getFieldVector(fieldName).getField().getChildren().get(0);
                    Types.MinorType childType = Types.getMinorTypeForArrowType(child.getType());

                    switch (childType) {
                        case LIST:
                            return (int seed, int rowNum, boolean negative) -> {
                                UnionListWriter writer = ((ListVector) fieldVector).getWriter();
                                writer.setPosition(rowNum);
                                writer.startList();
                                BaseWriter.ListWriter innerWriter = writer.list();
                                innerWriter.startList();
                                for (int i = 0; i < 3; i++) {
                                    byte[] bytes = String.valueOf(1000 + i).getBytes(Charsets.UTF_8);
                                    try (ArrowBuf buf = fieldVector.getAllocator().buffer(bytes.length)) {
                                        buf.writeBytes(bytes);
                                        innerWriter.varChar().writeVarChar(0, buf.readableBytes(), buf);
                                    }
                                }
                                innerWriter.endList();
                                writer.endList();
                                ((ListVector) fieldVector).setNotNull(rowNum);
                                return true;
                            };
                        case STRUCT:
                            return (int seed, int rowNum, boolean negative) -> {
                                UnionListWriter writer = ((ListVector) fieldVector).getWriter();
                                writer.setPosition(rowNum);
                                writer.startList();

                                BaseWriter.StructWriter structWriter = writer.struct();
                                structWriter.start();

                                byte[] bytes = "chars".getBytes(Charsets.UTF_8);
                                try (ArrowBuf buf = fieldVector.getAllocator().buffer(bytes.length)) {
                                    buf.writeBytes(bytes);
                                    structWriter.varChar("varchar").writeVarChar(0, buf.readableBytes(), buf);
                                }
                                structWriter.bigInt("bigint").writeBigInt(100L);
                                structWriter.end();

                                writer.endList();
                                ((ListVector) fieldVector).setNotNull(rowNum);
                                return true;
                            };
                        default:
                            throw new RuntimeException(childType + " is not supported");
                    }
                default:
                    throw new RuntimeException(fieldType + " is not supported");
            }
        }
    }

    private interface Projector
    {
        boolean project(int seed, int rowNum, boolean negative);
    }
}
