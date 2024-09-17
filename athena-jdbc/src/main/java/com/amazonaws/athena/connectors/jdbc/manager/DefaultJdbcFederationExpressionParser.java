/*-
 * #%L
 * athena-jdbc
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

import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.services.glue.model.ErrorDetails;
import com.amazonaws.services.glue.model.FederationSourceErrorCode;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

public class DefaultJdbcFederationExpressionParser extends FederationExpressionParser
{
    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        throw new AthenaConnectorException("Subclass does not yet support complex expressions.",
                new ErrorDetails().withErrorCode(FederationSourceErrorCode.OperationNotSupportedException.toString()));
    }
    
}
