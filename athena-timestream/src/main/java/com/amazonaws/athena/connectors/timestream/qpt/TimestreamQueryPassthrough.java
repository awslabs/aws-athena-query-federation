/*-
 * #%L
 * athena-timestream
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
package com.amazonaws.athena.connectors.timestream.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimestreamQueryPassthrough implements QueryPassthroughSignature
{
    // Constant value representing the name of the query.
    public static final String NAME = "query";

    // Constant value representing the domain of the query.
    public static final String SCHEMA_NAME = "system";

    // List of arguments for the query, statically initialized as it always contains the same value.
    public static final String QUERY = "QUERY";

    public static final List<String> ARGUMENTS = Arrays.asList(QUERY);

    private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamQueryPassthrough.class);

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
        String customerPassedQuery = engineQptArguments.get(QUERY);
        String upperCaseStatement = customerPassedQuery.trim().toUpperCase(Locale.ENGLISH);

        // Check if the statement starts with "SELECT" or "WITH" (for CTEs)
        if (!upperCaseStatement.startsWith("SELECT") && !upperCaseStatement.startsWith("WITH")) {
            throw new UnsupportedOperationException("Statement must start with SELECT or WITH.");
        }

        // List of disallowed keywords - these should be matched as whole words to avoid false positives
        // (e.g., "last_update" should not match "UPDATE", "CREATE_TIME_SERIES" should not match "CREATE")
        Set<String> disallowedKeywords = ImmutableSet.of("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER");

        // Check if the statement contains any disallowed keywords as whole words
        // Using word boundaries (\b) to ensure we match keywords, not substrings in identifiers
        for (String keyword : disallowedKeywords) {
            // Pattern matches keyword as a whole word (not part of another word)
            // \b ensures word boundaries, and we use case-insensitive matching
            Pattern pattern = Pattern.compile("\\b" + Pattern.quote(keyword) + "\\b", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(customerPassedQuery);
            if (matcher.find()) {
                throw new UnsupportedOperationException("Unaccepted operation; only SELECT statements are allowed. Found: " + keyword);
            }
        }
    }
}
