/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.dynamodb.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class DDBQueryPassthrough implements QueryPassthroughSignature
{
    // Constant value representing the name of the query.
    public static final String NAME = "query";

    // Constant value representing the domain of the query.
    public static final String SCHEMA_NAME = "system";

    // List of arguments for the query, statically initialized as it always contains the same value.
    public static final String QUERY = "QUERY";

    public static final List<String> ARGUMENTS = Arrays.asList(QUERY);

    private static final Logger LOGGER = LoggerFactory.getLogger(DDBQueryPassthrough.class);

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

    @Override
    public void customConnectorVerifications(Map<String, String> engineQptArguments)
    {
        String partiQLStatement = engineQptArguments.get(QUERY);
        String upperCaseStatement = partiQLStatement.trim().toUpperCase(Locale.ENGLISH);

        // Immediately check if the statement starts with "SELECT"
        if (!upperCaseStatement.startsWith("SELECT")) {
            throw new UnsupportedOperationException("Statement does not start with SELECT.");
        }

        // List of disallowed keywords
        Set<String> disallowedKeywords = ImmutableSet.of("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER");

        // Check if the statement contains any disallowed keywords
        for (String keyword : disallowedKeywords) {
            if (upperCaseStatement.contains(keyword)) {
                throw new UnsupportedOperationException("Unaccepted operation; only SELECT statements are allowed. Found: " + keyword);
            }
        }
    }
}
