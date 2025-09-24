/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DeltaShareRecordHandlerTest
{
    @Test
    public void testHandlerClassExists()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareRecordHandler");
            assertNotNull("DeltaShareRecordHandler class should exist", handlerClass);
            assertTrue("Should be a public class", java.lang.reflect.Modifier.isPublic(handlerClass.getModifiers()));
        } catch (ClassNotFoundException e) {
            fail("DeltaShareRecordHandler class should exist: " + e.getMessage());
        }
    }
    
    @Test
    public void testHandlerExtendsCorrectSuperclass()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareRecordHandler");
            Class<?> superclass = handlerClass.getSuperclass();
            assertNotNull("Should have a superclass", superclass);
            assertEquals("Should extend RecordHandler", 
                "com.amazonaws.athena.connector.lambda.handlers.RecordHandler", superclass.getName());
        } catch (ClassNotFoundException e) {
            fail("DeltaShareRecordHandler class should exist");
        }
    }
    
    @Test
    public void testHandlerHasRequiredMethods()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareRecordHandler");
            
            boolean hasReadWithConstraint = false;
            boolean hasGetSpillConfig = false;
            
            for (java.lang.reflect.Method method : handlerClass.getDeclaredMethods()) {
                String methodName = method.getName();
                switch (methodName) {
                    case "readWithConstraint":
                        hasReadWithConstraint = true;
                        break;
                    case "getSpillConfig":
                        hasGetSpillConfig = true;
                        break;
                }
            }
            
            assertTrue("Should have readWithConstraint method", hasReadWithConstraint);
            assertTrue("Should have getSpillConfig method", hasGetSpillConfig);
            
        } catch (ClassNotFoundException e) {
            fail("DeltaShareRecordHandler class should exist");
        }
    }
    
    @Test
    public void testHandlerConstructorExists()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareRecordHandler");
            
            java.lang.reflect.Constructor<?>[] constructors = handlerClass.getConstructors();
            assertTrue("Should have at least one constructor", constructors.length > 0);
            
        } catch (ClassNotFoundException e) {
            fail("DeltaShareRecordHandler class should exist");
        }
    }
}
