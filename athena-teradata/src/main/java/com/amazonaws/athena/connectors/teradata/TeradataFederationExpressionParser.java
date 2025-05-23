/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata;

import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.OperatorType;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcFederationExpressionParser;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

public class TeradataFederationExpressionParser extends JdbcFederationExpressionParser
{
    public TeradataFederationExpressionParser(String quoteChar)
    {
        super(quoteChar);
    }

    @Override
    public String writeArrayConstructorClause(ArrowType type, List<String> arguments)
    {
        return Joiner.on(", ").join(arguments);
    }
    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        StandardFunctions functionEnum = StandardFunctions.fromFunctionName(functionName);
        OperatorType operatorType = functionEnum.getOperatorType();

        if (arguments == null || arguments.size() == 0) {
            throw new IllegalArgumentException("Arguments cannot be null or empty.");
        }
        switch (operatorType) {
            case UNARY:
                if (arguments.size() != 1) {
                    throw new IllegalArgumentException("Unary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case BINARY:
                if (arguments.size() != 2) {
                    throw new IllegalArgumentException("Binary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case VARARG:
                break;
            default:
                throw new RuntimeException("A new operator type was introduced without adding support for it.");
        }

        String clause = "";
        switch (functionEnum) {
            case MODULUS_FUNCTION_NAME:
                clause = Joiner.on(" MOD ").join(arguments);
                break;
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME:
                clause = writeArrayConstructorClause(type, arguments);
                break;
            default:
                clause = super.mapFunctionToDataSourceSyntax(functionName, type, arguments);
        }
        if (clause == null) {
            return "";
        }
        return "(" + clause + ")";
    }
}
