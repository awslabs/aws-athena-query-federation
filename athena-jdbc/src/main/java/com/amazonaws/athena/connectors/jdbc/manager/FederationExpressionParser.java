/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

public abstract class FederationExpressionParser 
{
    static final Logger LOGGER = LoggerFactory.getLogger(FederationExpressionParser.class);

    private static final String quoteCharacter = "'"; // AFAIK this is going to be valid for all sources when quoting a constant.

    /**
     * Each datasource has different syntax for various operations, quotes, etc. This is the only method a subclass to implement, and otherwise will just
     * invoke parseComplexExpressions.
     */
    public abstract String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments);

    public List<String> parseComplexExpressions(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator)
    {
        if (constraints.getExpression() == null || constraints.getExpression().isEmpty()) {
            return ImmutableList.of();
        }

        List<FederationExpression> federationExpressions = constraints.getExpression();
        return federationExpressions.stream()
                    .map(federationExpression -> parseFunctionCallExpression((FunctionCallExpression) federationExpression, accumulator))
                    .collect(Collectors.toList());
    }

    /**
     * This is a recursive function, as function calls can have arguments which, themselves, are function calls.
     * @param functionCallExpression
     * @return
     */
    public String parseFunctionCallExpression(FunctionCallExpression functionCallExpression, List<TypeAndValue> accumulator)
    {
        FunctionName functionName = functionCallExpression.getFunctionName();
        List<FederationExpression> functionArguments = functionCallExpression.getArguments();

        List<String> arguments = functionArguments.stream()
            .map(argument -> {
                // base cases
                if (argument instanceof ConstantExpression) {
                    return parseConstantExpression((ConstantExpression) argument, accumulator);
                }
                else if (argument instanceof VariableExpression) {
                    return parseVariableExpression((VariableExpression) argument);
                }
                // recursive case
                else if (argument instanceof FunctionCallExpression) {
                    return parseFunctionCallExpression((FunctionCallExpression) argument, accumulator);
                }
                throw new RuntimeException("Should not reach this case - a new subclass was introduced and is not handled.");
            }).collect(Collectors.toList());

        return mapFunctionToDataSourceSyntax(functionName, functionCallExpression.getType(), arguments);
    }
    
    // basing this off of the toString() impl in Block.java
    @VisibleForTesting
    public String parseConstantExpression(ConstantExpression constantExpression, List<TypeAndValue> accumulator)
    {
        Block values = constantExpression.getValues();
        FieldReader fieldReader = values.getFieldReader(DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME);
        
        for (int i = 0; i < values.getRowCount(); i++) {
            fieldReader.setPosition(i);
            accumulator.add(new TypeAndValue(constantExpression.getType(), fieldReader.readObject()));
        }

        return Joiner.on(",").join(Collections.nCopies(values.getRowCount(), "?"));
    }

    /**
     * Various connectors have different standards for wrapping column names in some specific quote character.
     * This default implementation just returns the column name for connectors that don't have this specification.
     */
    @VisibleForTesting
    public String parseVariableExpression(VariableExpression variableExpression) 
    {
        return variableExpression.getColumnName();
    }
}
