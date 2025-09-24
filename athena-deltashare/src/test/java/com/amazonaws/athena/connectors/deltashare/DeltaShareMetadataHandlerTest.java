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

public class DeltaShareMetadataHandlerTest
{
    @Test
    public void testHandlerClassExists()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareMetadataHandler");
            assertNotNull("DeltaShareMetadataHandler class should exist", handlerClass);
            assertTrue("Should be a public class", java.lang.reflect.Modifier.isPublic(handlerClass.getModifiers()));
        } catch (ClassNotFoundException e) {
            fail("DeltaShareMetadataHandler class should exist: " + e.getMessage());
        }
    }
    
    @Test
    public void testHandlerExtendsCorrectSuperclass()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareMetadataHandler");
            Class<?> superclass = handlerClass.getSuperclass();
            assertNotNull("Should have a superclass", superclass);
            assertEquals("Should extend MetadataHandler", 
                "com.amazonaws.athena.connector.lambda.handlers.MetadataHandler", superclass.getName());
        } catch (ClassNotFoundException e) {
            fail("DeltaShareMetadataHandler class should exist");
        }
    }
    
    @Test
    public void testHandlerHasRequiredMethods()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareMetadataHandler");
            
            boolean hasListSchemas = false;
            boolean hasListTables = false;
            boolean hasGetTable = false;
            boolean hasGetSplits = false;
            
            for (java.lang.reflect.Method method : handlerClass.getDeclaredMethods()) {
                String methodName = method.getName();
                switch (methodName) {
                    case "doListSchemaNames":
                        hasListSchemas = true;
                        break;
                    case "doListTables":
                        hasListTables = true;
                        break;
                    case "doGetTable":
                        hasGetTable = true;
                        break;
                    case "doGetSplits":
                        hasGetSplits = true;
                        break;
                }
            }
            
            assertTrue("Should have doListSchemaNames method", hasListSchemas);
            assertTrue("Should have doListTables method", hasListTables);
            assertTrue("Should have doGetTable method", hasGetTable);
            assertTrue("Should have doGetSplits method", hasGetSplits);
            
        } catch (ClassNotFoundException e) {
            fail("DeltaShareMetadataHandler class should exist");
        }
    }
    
    @Test
    public void testHandlerConstructorExists()
    {
        try {
            Class<?> handlerClass = Class.forName("com.amazonaws.athena.connectors.deltashare.DeltaShareMetadataHandler");
            
            java.lang.reflect.Constructor<?>[] constructors = handlerClass.getConstructors();
            assertTrue("Should have at least one constructor", constructors.length > 0);
            
        } catch (ClassNotFoundException e) {
            fail("DeltaShareMetadataHandler class should exist");
        }
    }
}
