/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SqlServerFederationExpressionParserTest
{
    private final SqlServerFederationExpressionParser parser = new SqlServerFederationExpressionParser();
    
    @Test
    public void parseVariableExpression_whenIdentifierIsPresent_returnsBracketQuotedName()
    {
        VariableExpression expr = new VariableExpression("col", new ArrowType.Int(32, true));
        assertEquals("[col]", parser.parseVariableExpression(expr));
    }
    
    @Test
    public void parseVariableExpression_whenNameContainsClosingBracket_doublesEmbeddedBracket()
    {
        VariableExpression expr = new VariableExpression("col]x", new ArrowType.Int(32, true));
        assertEquals("[col]]x]", parser.parseVariableExpression(expr));
    }
}
