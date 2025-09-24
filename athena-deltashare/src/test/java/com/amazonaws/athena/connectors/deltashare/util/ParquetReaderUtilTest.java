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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ParquetReaderUtilTest
{
    @Test
    public void testStreamParquetFromUrlMethodSignature()
    {
        try {
            Method method = ParquetReaderUtil.class.getMethod("streamParquetFromUrl", 
                String.class, 
                com.amazonaws.athena.connector.lambda.data.BlockSpiller.class,
                org.apache.arrow.vector.types.pojo.Schema.class);
            assertNotNull(method);
            assertTrue(java.lang.reflect.Modifier.isStatic(method.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPublic(method.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("streamParquetFromUrl method should exist");
        }
    }
    
    @Test
    public void testStreamParquetFromUrlWithRowGroupMethodSignature()
    {
        try {
            Method method = ParquetReaderUtil.class.getMethod("streamParquetFromUrlWithRowGroup", 
                String.class, 
                com.amazonaws.athena.connector.lambda.data.BlockSpiller.class,
                org.apache.arrow.vector.types.pojo.Schema.class,
                int.class);
            assertNotNull(method);
            assertTrue(java.lang.reflect.Modifier.isStatic(method.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPublic(method.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("streamParquetFromUrlWithRowGroup method should exist");
        }
    }
    
    @Test
    public void testGetParquetMetadataMethodSignature()
    {
        try {
            Method method = ParquetReaderUtil.class.getMethod("getParquetMetadata", 
                String.class, 
                long.class);
            assertNotNull(method);
            assertTrue(java.lang.reflect.Modifier.isStatic(method.getModifiers()));
            assertTrue(java.lang.reflect.Modifier.isPublic(method.getModifiers()));
            assertEquals("org.apache.parquet.hadoop.metadata.ParquetMetadata", method.getReturnType().getName());
        } catch (NoSuchMethodException e) {
            fail("getParquetMetadata method should exist");
        }
    }
    
    @Test
    public void testDeprecatedMethodsExist()
    {
        try {
            Method cleanupMethod = ParquetReaderUtil.class.getMethod("cleanupTempFile", String.class);
            assertNotNull(cleanupMethod);
            assertTrue(cleanupMethod.isAnnotationPresent(Deprecated.class));
            
            Method downloadMethod = ParquetReaderUtil.class.getMethod("downloadParquetFile", String.class);
            assertNotNull(downloadMethod);
            assertTrue(downloadMethod.isAnnotationPresent(Deprecated.class));
            
            Method streamMethod = ParquetReaderUtil.class.getMethod("streamParquetToSpiller", 
                String.class, 
                com.amazonaws.athena.connector.lambda.data.BlockSpiller.class,
                org.apache.arrow.vector.types.pojo.Schema.class);
            assertNotNull(streamMethod);
            assertTrue(streamMethod.isAnnotationPresent(Deprecated.class));
        } catch (NoSuchMethodException e) {
            fail("Deprecated methods should exist: " + e.getMessage());
        }
    }
    
    @Test
    public void testInnerClassesExist()
    {
        Class<?>[] innerClasses = ParquetReaderUtil.class.getDeclaredClasses();
        assertTrue("Should have inner classes", innerClasses.length > 0);
        
        boolean hasPresignedRangeInputFile = false;
        boolean hasHttpRangeSeekableInputStream = false;
        
        for (Class<?> innerClass : innerClasses) {
            String simpleName = innerClass.getSimpleName();
            if ("PresignedRangeInputFile".equals(simpleName)) {
                hasPresignedRangeInputFile = true;
            } else if ("HttpRangeSeekableInputStream".equals(simpleName)) {
                hasHttpRangeSeekableInputStream = true;
            }
        }
        
        assertTrue("Should have PresignedRangeInputFile inner class", hasPresignedRangeInputFile);
        assertTrue("Should have HttpRangeSeekableInputStream inner class", hasHttpRangeSeekableInputStream);
    }
    
    @Test
    public void testStaticFieldsExist()
    {
        try {
            java.lang.reflect.Field[] fields = ParquetReaderUtil.class.getDeclaredFields();
            boolean hasLogger = false;
            boolean hasHadoopConf = false;
            boolean hasConstants = false;
            
            for (java.lang.reflect.Field field : fields) {
                String fieldName = field.getName();
                if ("logger".equals(fieldName)) {
                    hasLogger = true;
                } else if ("HADOOP_CONF".equals(fieldName)) {
                    hasHadoopConf = true;
                } else if (fieldName.contains("FORMATTER") || fieldName.contains("SIZE") || fieldName.contains("BATCH")) {
                    hasConstants = true;
                }
            }
            
            assertTrue("Should have logger field", hasLogger);
            assertTrue("Should have HADOOP_CONF field", hasHadoopConf);
            assertTrue("Should have constant fields", hasConstants);
        } catch (Exception e) {
            fail("Should be able to access class fields");
        }
    }
}
