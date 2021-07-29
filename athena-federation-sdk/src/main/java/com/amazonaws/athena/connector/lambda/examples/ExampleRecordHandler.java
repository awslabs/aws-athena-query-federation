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
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
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
import com.google.common.base.Charsets;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashSet;
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
        logger.info("logCaller: account[" + identity.getAccount() + "] arn[" + identity.getArn() + "]");
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

        int year = Integer.valueOf(request.getSplit().getProperty("year"));
        int month = Integer.valueOf(request.getSplit().getProperty("month"));
        int day = Integer.valueOf(request.getSplit().getProperty("day"));

        final RowContext rowContext = new RowContext(year, month, day);

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(request.getConstraints());
        for (Field next : request.getSchema().getFields()) {
            Extractor extractor = makeExtractor(next, rowContext);
            if (extractor != null) {
                builder.withExtractor(next.getName(), extractor);
            }
            else {
                builder.withFieldWriterFactory(next.getName(), makeFactory(next, rowContext));
            }
        }

        GeneratedRowWriter rowWriter = builder.build();
        for (int i = 0; i < numRowsPerSplit; i++) {
            rowContext.seed = i;
            rowContext.negative = i % 2 == 0;
            if (!queryStatusChecker.isQueryRunning()) {
                return;
            }
            spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, rowContext) ? 1 : 0);
        }

        logger.info("readWithConstraint: Completed generating rows in {} ms", System.currentTimeMillis() - startTime);
    }

    /**
     * Creates an Extractor for the given field. In this example the extractor just creates some random data.
     */
    private Extractor makeExtractor(Field field, RowContext rowContext)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        //This is an artifact of us generating data, you can't generate the partitions cols
        //they need to match the split otherwise filtering will brake in unexpected ways.
        if (field.getName().equals("year")) {
            return (IntExtractor) (Object context, NullableIntHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = rowContext.getYear();
            };
        }
        else if (field.getName().equals("month")) {
            return (IntExtractor) (Object context, NullableIntHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = rowContext.getMonth();
            };
        }
        else if (field.getName().equals("day")) {
            return (IntExtractor) (Object context, NullableIntHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = rowContext.getDay();
            };
        }

        switch (fieldType) {
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ((RowContext) context).seed * (((RowContext) context).negative ? -1 : 1);
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ((RowContext) context).seed * (((RowContext) context).negative ? -1 : 1);
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ((RowContext) context).seed * (((RowContext) context).negative ? -1 : 1);
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = (byte) ((((RowContext) context).seed % 4) * (((RowContext) context).negative ? -1 : 1));
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = (short) ((((RowContext) context).seed % 4) * (((RowContext) context).negative ? -1 : 1));
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ((float) ((RowContext) context).seed) * 1.1f * (((RowContext) context).negative ? -1f : 1f);
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ((double) ((RowContext) context).seed) * 1.1D;
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    dst.isSet = 1;
                    double d8Val = ((RowContext) context).seed * 1.1D * (((RowContext) context).negative ? -1d : 1d);
                    BigDecimal bdVal = new BigDecimal(d8Val);
                    dst.value = bdVal.setScale(((ArrowType.Decimal) field.getType()).getScale(), RoundingMode.HALF_UP);
                };
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ((RowContext) context).seed % 2;
                };
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ((RowContext) context).seed * 1L * (((RowContext) context).negative ? -1 : 1);
                };
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = "VarChar" + ((RowContext) context).seed;
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    dst.isSet = 1;
                    dst.value = ("VarChar" + ((RowContext) context).seed).getBytes(Charsets.UTF_8);
                };
            default:
                return null;
        }
    }

    /**
     * Since GeneratedRowWriter doesn't yet support complex types (STRUCT, LIST) we use this to
     * create our own FieldWriters via customer FieldWriterFactory. In this case we are producing
     * FieldWriters that only work for our exact example schema. This will be enhanced with a more
     * generic solution in a future release.
     */
    private FieldWriterFactory makeFactory(Field field, RowContext rowContext)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        switch (fieldType) {
            case LIST:
                Field child = field.getChildren().get(0);
                Types.MinorType childType = Types.getMinorTypeForArrowType(child.getType());
                switch (childType) {
                    case LIST:
                        return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                                (FieldWriter) (Object context, int rowNum) -> {
                                    UnionListWriter writer = ((ListVector) vector).getWriter();
                                    writer.setPosition(rowNum);
                                    writer.startList();
                                    BaseWriter.ListWriter innerWriter = writer.list();
                                    innerWriter.startList();
                                    for (int i = 0; i < 3; i++) {
                                        byte[] bytes = String.valueOf(1000 + i).getBytes(Charsets.UTF_8);
                                        try (ArrowBuf buf = vector.getAllocator().buffer(bytes.length)) {
                                            buf.writeBytes(bytes);
                                            innerWriter.varChar().writeVarChar(0, (int) (buf.readableBytes()), buf);
                                        }
                                    }
                                    innerWriter.endList();
                                    writer.endList();
                                    ((ListVector) vector).setNotNull(rowNum);
                                    return true;
                                };
                    case STRUCT:
                        return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                                (FieldWriter) (Object context, int rowNum) -> {
                                    UnionListWriter writer = ((ListVector) vector).getWriter();
                                    writer.setPosition(rowNum);
                                    writer.startList();

                                    BaseWriter.StructWriter structWriter = writer.struct();
                                    structWriter.start();

                                    byte[] bytes = "chars".getBytes(Charsets.UTF_8);
                                    try (ArrowBuf buf = vector.getAllocator().buffer(bytes.length)) {
                                        buf.writeBytes(bytes);
                                        structWriter.varChar("varchar").writeVarChar(0, (int) (buf.readableBytes()), buf);
                                    }
                                    structWriter.bigInt("bigint").writeBigInt(100L);
                                    structWriter.end();

                                    writer.endList();
                                    ((ListVector) vector).setNotNull(rowNum);
                                    return true;
                                };
                    default:
                        throw new IllegalArgumentException("Unsupported type " + childType);
                }
            case MAP:
                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (FieldWriter) (Object context, int rowNum) -> {
                            UnionMapWriter writer  = ((MapVector) vector).getWriter();
                            writer.setPosition(rowNum);
                            writer.startMap();
                            writer.startEntry();
                            byte[] bytes = "chars".getBytes(Charsets.UTF_8);
                            try (ArrowBuf buf = vector.getAllocator().buffer(bytes.length)) {
                                buf.writeBytes(bytes);
                                writer.key().varChar("key").writeVarChar(0, (int) (buf.readableBytes()), buf);
                            }

                            writer.value().integer("value").writeInt(1001);
                            writer.endEntry();
                            writer.endMap();
                            ((MapVector) vector).setNotNull(rowNum);
                            return true;
                        };
            default:
                throw new IllegalArgumentException("Unsupported type " + fieldType);
        }
    }

    private static class RowContext
    {
        private int seed;
        private boolean negative;
        private final int year;
        private final int month;
        private final int day;

        public RowContext(int year, int month, int day)
        {
            this.year = year;
            this.month = month;
            this.day = day;
        }

        public int getYear()
        {
            return year;
        }

        public int getMonth()
        {
            return month;
        }

        public int getDay()
        {
            return day;
        }

        public int getSeed()
        {
            return seed;
        }

        public void setSeed(int seed)
        {
            this.seed = seed;
        }

        public boolean isNegative()
        {
            return negative;
        }

        public void setNegative(boolean negative)
        {
            this.negative = negative;
        }
    }
}
