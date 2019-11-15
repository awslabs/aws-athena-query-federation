package com.amazonaws.athena.connector.lambda.udf;

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
import com.amazonaws.athena.connector.lambda.data.projectors.ArrowValueProjector;
import com.amazonaws.athena.connector.lambda.data.projectors.ProjectorUtils;
import com.amazonaws.athena.connector.lambda.data.writers.ArrowValueWriter;
import com.amazonaws.athena.connector.lambda.data.writers.WriterUtils;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Athena UDF users are expected to extend this class to create UDFs.
 */
public abstract class UserDefinedFunctionHandler implements RequestStreamHandler
{
    private static final Logger logger = LoggerFactory.getLogger(UserDefinedFunctionHandler.class);

    private static final int RETURN_COLUMN_COUNT = 1;

    @Override
    public final void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
    {
        try (BlockAllocator allocator = new BlockAllocatorImpl()) {
            ObjectMapper objectMapper = ObjectMapperFactory.create(allocator);
            try (FederationRequest rawRequest = objectMapper.readValue(inputStream, FederationRequest.class)) {
                if (!(rawRequest instanceof UserDefinedFunctionRequest)) {
                    throw new RuntimeException("Expected a UserDefinedFunctionRequest but found "
                            + rawRequest.getClass());
                }

                UserDefinedFunctionRequest udfRequest = (UserDefinedFunctionRequest) rawRequest;
                try (UserDefinedFunctionResponse udfResponse = processFunction(allocator, udfRequest)) {
                    objectMapper.writeValue(outputStream, udfResponse);
                }
            }
            catch (Exception ex) {
                throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
            }
        }
    }

    @VisibleForTesting
    UserDefinedFunctionResponse processFunction(BlockAllocator allocator, UserDefinedFunctionRequest req)
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
     *
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
    {
        int rowCount = inputRecords.getRowCount();

        List<ArrowValueProjector> valueProjectors = Lists.newArrayList();

        for (Field field : inputRecords.getFields()) {
            FieldReader fieldReader = inputRecords.getFieldReader(field.getName());
            ArrowValueProjector arrowValueProjector = ProjectorUtils.createArrowValueProjector(fieldReader);
            valueProjectors.add(arrowValueProjector);
        }

        Block outputRecords = allocator.createBlock(outputSchema);
        outputRecords.setRowCount(rowCount);

        try {
            String outputFieldName = outputSchema.getFields().get(0).getName();
            FieldVector outputVector = outputRecords.getFieldVector(outputFieldName);
            ArrowValueWriter outputProjector = WriterUtils.createArrowValueWriter(outputVector);
            Object[] arguments = new Object[valueProjectors.size()];

            for (int rowNum = 0; rowNum < rowCount; ++rowNum) {
                for (int col = 0; col < valueProjectors.size(); ++col) {
                    arguments[col] = valueProjectors.get(col).project(rowNum);
                }

                try {
                    Object result = udfMethod.invoke(this, arguments);
                    outputProjector.write(rowNum, result);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
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
}
