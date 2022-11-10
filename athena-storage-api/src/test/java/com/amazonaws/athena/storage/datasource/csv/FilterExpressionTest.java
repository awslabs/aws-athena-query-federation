/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.datasource.csv;

import com.amazonaws.athena.storage.common.FilterExpression;
import org.junit.Test;

import java.util.List;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class FilterExpressionTest
{

    @Test
    public void testAnd()
    {
        FilterExpression expression = new And(List.of(new EqualsExpression("test", "test")));
        assertTrue("test should match with test", expression.apply("test"));
    }

    @Test
    public void testOr()
    {
        FilterExpression expression = new Or(List.of(new EqualsExpression("test", "test")));
        assertTrue("test should match with test", expression.apply("test"));
    }

    @Test
    public void testIsNull()
    {
        FilterExpression expression = new IsNullExpression("test");
        assertFalse("test column wasn't null", expression.apply("test"));
    }
}
