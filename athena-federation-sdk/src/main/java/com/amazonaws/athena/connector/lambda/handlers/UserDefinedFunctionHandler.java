package com.amazonaws.athena.connector.lambda.handlers;

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
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.projectors.ArrowValueProjector;
import com.amazonaws.athena.connector.lambda.data.projectors.ProjectorUtils;
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
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionResponse;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionType;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.handlers.FederationCapabilities.CAPABILITIES;
import static com.amazonaws.athena.connector.lambda.handlers.SerDeVersion.SERDE_VERSION;
import static com.google.common.base.Preconditions.checkState;

/**
 * Athena UDF users are expected to extend this class to create UDFs.
 */
public abstract class UserDefinedFunctionHandler
        implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(UserDefinedFunctionHandler.class);

    private static final int RETURN_COLUMN_COUNT = 1;
    //Used to tag log lines generated by this connector for diagnostic purposes when interacting with Athena.
    private final String sourceType;

    public UserDefinedFunctionHandler(String sourceType)
    {
        this.sourceType = sourceType;
    }

    @Override
    public final void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
    {
        try (BlockAllocator allocator = new BlockAllocatorImpl()) {
            ObjectMapper objectMapper = VersionedObjectMapperFactory.create(allocator);
            try (FederationRequest rawRequest = objectMapper.readValue(inputStream, FederationRequest.class)) {
                if (rawRequest instanceof PingRequest) {
                    try (PingResponse response = doPing((PingRequest) rawRequest)) {
                        assertNotNull(response);
                        objectMapper.writeValue(outputStream, response);
                    }
                    return;
                }

                if (!(rawRequest instanceof UserDefinedFunctionRequest)) {
                    throw new RuntimeException("Expected a UserDefinedFunctionRequest but found "
                            + rawRequest.getClass());
                }

                doHandleRequest(allocator, objectMapper, (UserDefinedFunctionRequest) rawRequest, outputStream);
            }
            catch (Exception ex) {
                throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
            }
        }
    }

    protected final void doHandleRequest(BlockAllocator allocator,
            ObjectMapper objectMapper,
            UserDefinedFunctionRequest req,
            OutputStream outputStream)
            throws Exception
    {
        logger.info("doHandleRequest: request[{}]", req);
        try (UserDefinedFunctionResponse response = processFunction(allocator, req)) {
            logger.info("doHandleRequest: response[{}]", response);
            assertNotNull(response);
            objectMapper.writeValue(outputStream, response);
        }
    }

    @VisibleForTesting
    UserDefinedFunctionResponse processFunction(BlockAllocator allocator, UserDefinedFunctionRequest req)
            throws Exception
    {
        UserDefinedFunctionType functionType = req.getFunctionType();
        switch (functionType) {
            case SCALAR:
                return processScalarFunction(allocator, req);
            default:
                throw new UnsupportedOperationException("Unsupported function type " + functionType);
        }
    }

    private UserDefinedFunctionResponse processScalarFunction(BlockAllocator allocator, UserDefinedFunctionRequest req)
            throws Exception
    {
        Method udfMethod = extractScalarFunctionMethod(req);
        Block inputRecords = req.getInputRecords();
        Schema outputSchema = req.getOutputSchema();

        Block outputRecords = processRows(allocator, udfMethod, inputRecords, outputSchema);
        return new UserDefinedFunctionResponse(outputRecords, udfMethod.getName());
    }

    /**
     * Processes a group by rows. This method takes in a block of data (containing multiple rows), process them and
     * returns multiple rows of the output column in a block.
     * <p>
     * UDF methods are invoked row-by-row in a for loop. Arrow values are converted to Java Objects and then passed into
     * the UDF java method. This is not very efficient because we might potentially be doing a lot of data copying.
     * Advanced users could choose to override this method and directly deal with Arrow data to achieve better
     * performance.
     *
     * @param allocator arrow memory allocator
     * @param udfMethod the extracted java method matching the User-Defined-Function defined in Athena.
     * @param inputRecords input data in Arrow format
     * @param outputSchema output data schema in Arrow format
     * @return output data in Arrow format
     */
    protected Block processRows(BlockAllocator allocator, Method udfMethod, Block inputRecords, Schema outputSchema)
            throws Exception
    {
        int rowCount = inputRecords.getRowCount();

        List<ArrowValueProjector> valueProjectors = Lists.newArrayList();

        for (Field field : inputRecords.getFields()) {
            FieldReader fieldReader = inputRecords.getFieldReader(field.getName());
            ArrowValueProjector arrowValueProjector = ProjectorUtils.createArrowValueProjector(fieldReader);
            valueProjectors.add(arrowValueProjector);
        }

        Field outputField = outputSchema.getFields().get(0);
        GeneratedRowWriter outputRowWriter = createOutputRowWriter(outputField, valueProjectors, udfMethod);

        Block outputRecords = allocator.createBlock(outputSchema);
        outputRecords.setRowCount(rowCount);

        try {
            for (int rowNum = 0; rowNum < rowCount; ++rowNum) {
                outputRowWriter.writeRow(outputRecords, rowNum, rowNum);
            }
        }
        catch (Throwable t) {
            try {
                outputRecords.close();
            }
            catch (Exception e) {
                logger.error("Error closing output block", e);
            }
            throw t;
        }

        return outputRecords;
    }

    /**
     * Use reflection to find tha java method that maches the UDF function defined in Athena SQL.
     *
     * @param req UDF request
     * @return java method matching the UDF defined in Athena query.
     */
    private Method extractScalarFunctionMethod(UserDefinedFunctionRequest req)
    {
        String methodName = req.getMethodName();
        Class[] argumentTypes = extractJavaTypes(req.getInputRecords().getSchema());
        Class[] returnTypes = extractJavaTypes(req.getOutputSchema());
        checkState(returnTypes.length == RETURN_COLUMN_COUNT,
                String.format("Expecting %d return columns, found %d in method signature.",
                        RETURN_COLUMN_COUNT, returnTypes.length));
        Class returnType = returnTypes[0];

        Method udfMethod;
        try {
            udfMethod = this.getClass().getMethod(methodName, argumentTypes);
            logger.info(String.format("Found UDF method %s with input types [%s] and output types [%s]",
                    methodName, Arrays.toString(argumentTypes), returnType.getName()));
        }
        catch (NoSuchMethodException e) {
            String msg = "Failed to find UDF method. " + e.getMessage()
                    + " Please make sure the method name contains only lowercase and the method signature (name and"
                    + " argument types) in Lambda matches the function signature defined in SQL.";
            throw new RuntimeException(msg, e);
        }

        if (!returnType.equals(udfMethod.getReturnType())) {
            throw new IllegalArgumentException("signature return type " + returnType +
                    " does not match udf implementation return type " + udfMethod.getReturnType());
        }

        return udfMethod;
    }

    private Class[] extractJavaTypes(Schema schema)
    {
        Class[] types = new Class[schema.getFields().size()];

        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); ++i) {
            Types.MinorType minorType = Types.getMinorTypeForArrowType(fields.get(i).getType());
            types[i] = BlockUtils.getJavaType(minorType);
        }

        return types;
    }

    private final PingResponse doPing(PingRequest request)
    {
        PingResponse response = new PingResponse(request.getCatalogName(), request.getQueryId(), sourceType, CAPABILITIES, SERDE_VERSION);
        try {
            onPing(request);
        }
        catch (Exception ex) {
            logger.warn("doPing: encountered an exception while delegating onPing.", ex);
        }
        return response;
    }

    protected void onPing(PingRequest request)
    {
        //NoOp
    }

    private void assertNotNull(FederationResponse response)
    {
        if (response == null) {
            throw new RuntimeException("Response was null");
        }
    }

    private GeneratedRowWriter createOutputRowWriter(Field outputField, List<ArrowValueProjector> valueProjectors, Method udfMethod)
    {
        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder();
        Extractor extractor = makeExtractor(outputField, valueProjectors, udfMethod);
        if (extractor != null) {
            builder.withExtractor(outputField.getName(), extractor);
        }
        else {
            builder.withFieldWriterFactory(outputField.getName(), makeFactory(outputField, valueProjectors, udfMethod));
        }
        return builder.build();
    }

    /**
     * Creates an Extractor for the given outputField.
     * @param outputField  outputField
     * @param valueProjectors projectors that we use to read input data.
     * @param udfMethod
     * @return
     */
    private Extractor makeExtractor(Field outputField, List<ArrowValueProjector> valueProjectors, Method udfMethod)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(outputField.getType());

        Object[] arguments = new Object[valueProjectors.size()];

        switch (fieldType) {
            case INT:
                return (IntExtractor) (Object inputRowNum, NullableIntHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (int) result;
                    }
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object inputRowNum, NullableDateMilliHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = ((LocalDateTime) result).atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli();
                    }
                };
            case DATEDAY:
                return (DateDayExtractor) (Object inputRowNum, NullableDateDayHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (int) ((LocalDate) result).toEpochDay();
                    }
                };
            case TINYINT:
                return (TinyIntExtractor) (Object inputRowNum, NullableTinyIntHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (byte) result;
                    }
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object inputRowNum, NullableSmallIntHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (short) result;
                    }
                };
            case FLOAT4:
                return (Float4Extractor) (Object inputRowNum, NullableFloat4Holder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (float) result;
                    }
                };
            case FLOAT8:
                return (Float8Extractor) (Object inputRowNum, NullableFloat8Holder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (double) result;
                    }
                };
            case DECIMAL:
                return (DecimalExtractor) (Object inputRowNum, NullableDecimalHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = ((BigDecimal) result);
                    }
                };
            case BIT:
                return (BitExtractor) (Object inputRowNum, NullableBitHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = ((boolean) result) ? 1 : 0;
                    }
                };
            case BIGINT:
                return (BigIntExtractor) (Object inputRowNum, NullableBigIntHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (long) result;
                    }
                };
            case VARCHAR:
                return (VarCharExtractor) (Object inputRowNum, NullableVarCharHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = ((String) result);
                    }
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object inputRowNum, NullableVarBinaryHolder dst) ->
                {
                    Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);

                    if (result == null) {
                        dst.isSet = 0;
                    }
                    else {
                        dst.isSet = 1;
                        dst.value = (byte[]) result;
                    }
                };
            default:
                return null;
        }
    }

    private FieldWriterFactory makeFactory(Field field, List<ArrowValueProjector> valueProjectors, Method udfMethod)
    {
        Object[] arguments = new Object[valueProjectors.size()];

        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        switch (fieldType) {
            case LIST:
            case STRUCT:
                return (FieldVector vector, Extractor extractor, ConstraintProjector ignored) ->
                        (Object inputRowNum, int outputRowNum) -> {
                            Object result = invokeMethod(udfMethod, arguments, (int) inputRowNum, valueProjectors);
                            BlockUtils.setComplexValue(vector, outputRowNum, FieldResolver.DEFAULT, result);
                            return true;    // push-down does not apply in UDFs
                        };

            default:
                throw new IllegalArgumentException("Unsupported type " + fieldType);
        }
    }

    private Object invokeMethod(Method udfMethod,
                                Object[] arguments,
                                int inputRowNum,
                                List<ArrowValueProjector> valueProjectors)
    {
        for (int col = 0; col < valueProjectors.size(); ++col) {
            arguments[col] = valueProjectors.get(col).project(inputRowNum);
        }

        try {
            return udfMethod.invoke(this, arguments);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e) {
            if (Objects.isNull(e)) {
                throw new RuntimeException(e);
            }
            throw new RuntimeException(e.getCause());
        }
        catch (IllegalArgumentException e) {
            String msg = String.format("%s. Expected function types %s, got types %s",
                    e.getMessage(),
                    Arrays.stream(udfMethod.getParameterTypes()).map(clazz -> clazz.getName()).collect(Collectors.toList()),
                    Arrays.stream(arguments).map(arg -> arg.getClass().getName()).collect(Collectors.toList()));
            throw new RuntimeException(msg, e);
        }
    }
}
