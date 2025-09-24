/*-
 * #%L
 * athena-deltashare
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.deltashare.util;

import org.junit.Test;
import java.lang.reflect.Method;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ParquetMetadataReaderTest
{
    @Test
    public void testGetParquetMetadataMethodSignature()
    {
        try {
            Method method = ParquetMetadataReader.class.getMethod("getParquetMetadata", String.class, long.class);
            assertNotNull(method);
            assertEquals("org.apache.parquet.hadoop.metadata.ParquetMetadata", method.getReturnType().getName());
            assertTrue(java.lang.reflect.Modifier.isStatic(method.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPublic(method.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("getParquetMetadata method should exist");
        }
    }
    
    @Test
    public void testMethodExceptionDeclaration()
    {
        try {
            Method method = ParquetMetadataReader.class.getMethod("getParquetMetadata", String.class, long.class);
            Class<?>[] exceptions = method.getExceptionTypes();
            assertEquals("Should declare IOException", 1, exceptions.length);
            assertEquals("Should declare IOException", java.io.IOException.class, exceptions[0]);
        } catch (NoSuchMethodException e) {
            fail("getParquetMetadata method should exist");
        }
    }
    
    @Test
    public void testClassStructure()
    {
        assertTrue("Should be a public class", java.lang.reflect.Modifier.isPublic(ParquetMetadataReader.class.getModifiers()));
        assertTrue("Should have methods", ParquetMetadataReader.class.getDeclaredMethods().length > 0);
    }
    
    @Test
    public void testLoggerField()
    {
        try {
            java.lang.reflect.Field loggerField = ParquetMetadataReader.class.getDeclaredField("logger");
            assertNotNull(loggerField);
            assertTrue(java.lang.reflect.Modifier.isStatic(loggerField.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isFinal(loggerField.getModifiers()));
            assertEquals("org.slf4j.Logger", loggerField.getType().getName());
        } catch (NoSuchFieldException e) {
            fail("Should have logger field");
        }
    }
    
    @Test
    public void testParameterTypes()
    {
        try {
            Method method = ParquetMetadataReader.class.getMethod("getParquetMetadata", String.class, long.class);
            Class<?>[] paramTypes = method.getParameterTypes();
            assertEquals("Should have 2 parameters", 2, paramTypes.length);
            assertEquals("First parameter should be String", String.class, paramTypes[0]);
            assertEquals("Second parameter should be long", long.class, paramTypes[1]);
        } catch (NoSuchMethodException e) {
            fail("getParquetMetadata method should exist");
        }
    }
}
