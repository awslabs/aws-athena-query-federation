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

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import java.util.List;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({})
public class AbstractCsvExpressionTest
{

    static AbstractCsvExpression abstractCsvExpression;
    static TestExpression test;

    @BeforeClass
    public static void setUp()
    {
        test = new TestExpression("test", List.of("test"));
        abstractCsvExpression = Mockito.mock(
                AbstractCsvExpression.class,
                Mockito.CALLS_REAL_METHODS);
        Whitebox.setInternalState(abstractCsvExpression, "column", "test");
        Whitebox.setInternalState(abstractCsvExpression, "expression", List.of("test"));

    }

    @Test
    public void testToString()
    {
        assertNotNull(abstractCsvExpression.toString());
    }


    @Test
    public void testHashcode()
    {
        assertNotNull(abstractCsvExpression.hashCode());
    }

    @Test
    public void testEquals()
    {
        assertNotEquals(test, abstractCsvExpression);
    }

    static class TestExpression extends AbstractCsvExpression<List<String>>
    {
        public TestExpression(String columnName, List<String> expression)
        {
            super(columnName, expression);
        }

        @Override
        public boolean apply(String value)
        {
            return false;
        }

    }
}
