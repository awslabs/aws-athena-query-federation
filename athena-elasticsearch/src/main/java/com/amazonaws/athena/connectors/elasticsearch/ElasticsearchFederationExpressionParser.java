/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.domain.predicate.FederationExpressionParser;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

public class ElasticsearchFederationExpressionParser extends FederationExpressionParser
{
    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
