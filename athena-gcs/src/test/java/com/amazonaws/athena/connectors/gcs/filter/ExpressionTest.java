/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs.filter;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExpressionTest
{
    @Test
    public void testEqualExpression()
    {
        EqualsExpression expression = new EqualsExpression("test_col", "test");
        assertTrue(expression.apply("test"), "Equal expression evaluated to false");
        assertTrue(expression.toString().contains("test_col"), "Column does not match");
    }

    @Test
    public void testAnyExpression()
    {
        AbstractExpression expression = new AnyExpression("test_col", java.util.List.of("test1", "test2"));
        assertTrue(expression.apply("test2"), "Amy expression evaluated to false");
        assertEquals("test_col", expression.getColumnName(), "Column doesn't match");
    }
}
