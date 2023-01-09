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
package com.amazonaws.athena.connector.lambda.domain.predicate;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.functions.FunctionName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class FederationExpressionParser 
{
    static final Logger LOGGER = LoggerFactory.getLogger(FederationExpressionParser.class);

    private static final String quoteCharacter = "'";

    /**
     * Each datasource has different syntax for various operations, quotes, etc. This is the only method a subclass to implement, and otherwise will just
     * invoke parseComplexExpressions.
     */
    public abstract String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments);

    public List<String> parseComplexExpressions(List<Field> columns, Constraints constraints, String quoteChar)
    {
        List<String> complexExpressionConjuncts = new ArrayList<>();
        if (constraints.getExpression() == null || constraints.getExpression().isEmpty()) {
            return complexExpressionConjuncts;
        }

        List<FederationExpression> federationExpressions = constraints.getExpression();
        for (FederationExpression federationExpression : federationExpressions) {
            FunctionCallExpression functionCallExpression = (FunctionCallExpression) federationExpression;
            complexExpressionConjuncts.add(parseFunctionCallExpression(functionCallExpression, quoteChar));
        }
        return complexExpressionConjuncts;
    }

    /**
     * This is a recursive function, as function calls can have arguments which, themselves, are function calls.
     * @param functionCallExpression
     * @return
     */
    public String parseFunctionCallExpression(FunctionCallExpression functionCallExpression, String quoteChar)
    {
        return parseFunctionCallExpressionHelper(functionCallExpression, quoteChar);
    }

    protected String parseFunctionCallExpressionHelper(FunctionCallExpression functionCallExpression, String quoteChar)
    {
        FunctionName functionName = functionCallExpression.getFunctionName();
        List<FederationExpression> functionArguments = functionCallExpression.getArguments();
        List<String> arguments = new ArrayList<>();

        for (FederationExpression argument : functionArguments) {
            if (argument instanceof FunctionCallExpression) { // recursive case
                arguments.add(parseFunctionCallExpressionHelper((FunctionCallExpression) argument, quoteChar));
            } 
            else if (argument instanceof ConstantExpression) { // base case
                ConstantExpression constantExpression = (ConstantExpression) argument;
                arguments.add(parseConstantExpression(constantExpression, quoteChar));
            }
            else if (argument instanceof VariableExpression) { // base case
                VariableExpression variableExpression = (VariableExpression) argument;
                arguments.add(parseVariableExpression(variableExpression, quoteChar));
            } 
            else {
                throw new RuntimeException("Should not reach this case - a new subclass was introduced and is not handled.");
            }
        }
        return mapFunctionToDataSourceSyntax(functionName, functionCallExpression.getType(), arguments);
    }
    
    // basing this off of the toString() impl in Block.java
    @VisibleForTesting
    public String parseConstantExpression(ConstantExpression constantExpression, String quoteChar) 
    {
        Block values = constantExpression.getValues();
        String dummyColumn = values.getSchema().getFields().get(0).getName();
        FieldReader fieldReader = values.getFieldReader(dummyColumn);
        
        List<String> constants = new ArrayList<>();
        
        for (int i = 0; i < values.getRowCount(); i++) {
            fieldReader.setPosition(i);
            String strVal = BlockUtils.fieldToString(fieldReader);
            constants.add(strVal);
        }

        if (constantExpression.getType().equals(ArrowType.Utf8.INSTANCE) || constantExpression.getType().equals(ArrowType.LargeUtf8.INSTANCE)) {
            constants = constants.stream()
                                 .map(val -> quoteCharacter + val + quoteCharacter)
                                 .collect(Collectors.toList());
        }

        return Joiner.on(",").join(constants);
    }

    @VisibleForTesting
    public String parseVariableExpression(VariableExpression variableExpression, String quoteChar)
    {
        return quoteChar + variableExpression.getColumnName() + quoteChar;
    }
}
