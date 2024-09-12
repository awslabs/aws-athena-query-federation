/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ElasticsearchQueryPassthrough implements QueryPassthroughSignature
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchQueryPassthrough.class);
    // Constant value representing the name of the query.
    public static final String NAME = "query";

    // Constant value representing the domain of the query.
    public static final String SCHEMA_NAME = "system";

    // List of arguments for the query, statically initialized as it always contains the same value.
    public static final String SCHEMA = "SCHEMA";
    public static final String INDEX = "INDEX";
    public static final String QUERY = "QUERY";

    public static final List<String> ARGUMENTS = Arrays.asList(SCHEMA, INDEX, QUERY);

    @Override
    public String getFunctionSchema()
    {
        return SCHEMA_NAME;
    }

    @Override
    public String getFunctionName()
    {
        return NAME;
    }

    @Override
    public List<String> getFunctionArguments()
    {
        return ARGUMENTS;
    }

    @Override
    public Logger getLogger()
    {
        return LOGGER;
    }
}
