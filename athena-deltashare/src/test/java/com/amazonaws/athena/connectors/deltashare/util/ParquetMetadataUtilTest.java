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
import static org.junit.Assert.fail;

public class ParquetMetadataUtilTest
{
    @Test
    public void testParquetFileMetadataInnerClass()
    {
        Class<?> metadataClass = ParquetMetadataUtil.ParquetFileMetadata.class;
        
        try {
            assertNotNull(metadataClass.getMethod("getTotalRows"));
            assertNotNull(metadataClass.getMethod("getRowGroups"));
            assertNotNull(metadataClass.getMethod("getRowGroupCount"));
            assertNotNull(metadataClass.getMethod("getCreatedBy"));
        } catch (NoSuchMethodException e) {
            fail("ParquetFileMetadata should have expected methods: " + e.getMessage());
        }
    }
    
    @Test
    public void testRowGroupMetadataInnerClass()
    {
        Class<?> rowGroupClass = ParquetMetadataUtil.RowGroupMetadata.class;
        
        try {
            assertNotNull(rowGroupClass.getMethod("getNumRows"));
            assertNotNull(rowGroupClass.getMethod("getTotalByteSize"));
            assertNotNull(rowGroupClass.getMethod("getTotalCompressedSize"));
        } catch (NoSuchMethodException e) {
            fail("RowGroupMetadata should have expected methods: " + e.getMessage());
        }
    }
    
    @Test
    public void testNestedClassModifiers()
    {
        assertTrue(java.lang.reflect.Modifier.isPublic(ParquetMetadataUtil.ParquetFileMetadata.class.getModifiers()));
        assertTrue(java.lang.reflect.Modifier.isStatic(ParquetMetadataUtil.ParquetFileMetadata.class.getModifiers()));
        assertTrue(java.lang.reflect.Modifier.isPublic(ParquetMetadataUtil.RowGroupMetadata.class.getModifiers()));
        assertTrue(java.lang.reflect.Modifier.isStatic(ParquetMetadataUtil.RowGroupMetadata.class.getModifiers()));
    }
}
