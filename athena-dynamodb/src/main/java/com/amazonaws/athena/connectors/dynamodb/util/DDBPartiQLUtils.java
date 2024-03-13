/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import java.util.Locale;
import java.util.Set;

public class DDBPartiQLUtils
{
    private DDBPartiQLUtils() {}
    /**
     * Perform rudimentary checks to confirm a statement is not performing any non-select query
     * @param partiQLStatement query
     */
    public static void verifyPartiQLSelectStatement(String partiQLStatement)
    {
        String upperCaseStatement = partiQLStatement.trim().toUpperCase(Locale.ENGLISH);

        // Immediately check if the statement starts with "SELECT"
        if (!upperCaseStatement.startsWith("SELECT")) {
            throw new UnsupportedOperationException("Statement does not start with SELECT.");
        }

        // List of disallowed keywords
        Set<String> disallowedKeywords = Set.of("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER");

        // Check if the statement contains any disallowed keywords
        for (String keyword : disallowedKeywords) {
            if (upperCaseStatement.contains(" " + keyword + " ") || upperCaseStatement.startsWith(keyword + " ")) {
                throw new UnsupportedOperationException("Unaccepted operation; only SELECT statements are allowed. Found: " + keyword);
            }
        }
    }
}
