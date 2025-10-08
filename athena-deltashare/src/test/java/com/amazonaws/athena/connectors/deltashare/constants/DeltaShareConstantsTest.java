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
package com.amazonaws.athena.connectors.deltashare.constants;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class DeltaShareConstantsTest
{
    @Test
    public void testSourceTypeConstant()
    {
        assertEquals("deltashare", DeltaShareConstants.SOURCE_TYPE);
    }
    
    @Test
    public void testPropertyConstants()
    {
        assertEquals("endpoint", DeltaShareConstants.ENDPOINT_PROPERTY);
        assertEquals("token", DeltaShareConstants.TOKEN_PROPERTY);
        assertEquals("share_name", DeltaShareConstants.SHARE_NAME_PROPERTY);
    }
    
    @Test
    public void testEndpointConstants()
    {
        assertEquals("shares", DeltaShareConstants.SHARES_ENDPOINT);
        assertEquals("shares/%s/schemas", DeltaShareConstants.SCHEMAS_ENDPOINT);
        assertEquals("shares/%s/schemas/%s/tables", DeltaShareConstants.TABLES_ENDPOINT);
        assertEquals("shares/%s/schemas/%s/tables/%s/metadata", DeltaShareConstants.TABLE_METADATA_ENDPOINT);
        assertEquals("shares/%s/schemas/%s/tables/%s/query", DeltaShareConstants.QUERY_TABLE_ENDPOINT);
    }
    
    @Test
    public void testHttpHeaderConstants()
    {
        assertEquals("Authorization", DeltaShareConstants.AUTHORIZATION_HEADER);
        assertEquals("Content-Type", DeltaShareConstants.CONTENT_TYPE_HEADER);
        assertEquals("Bearer ", DeltaShareConstants.BEARER_PREFIX);
        assertEquals("application/json", DeltaShareConstants.APPLICATION_JSON);
    }
    
    @Test
    public void testMetadataPropertyConstants()
    {
        assertEquals("share", DeltaShareConstants.SHARE_PROPERTY);
        assertEquals("schema", DeltaShareConstants.SCHEMA_PROPERTY);
        assertEquals("table", DeltaShareConstants.TABLE_PROPERTY);
        assertEquals("partition_info", DeltaShareConstants.PARTITION_INFO_PROPERTY);
    }
    
    @Test
    public void testSizeThresholdConstants()
    {
        assertEquals(480L * 1024 * 1024, DeltaShareConstants.LAMBDA_TEMP_LIMIT);
        assertEquals(536870912L, DeltaShareConstants.FILE_SIZE_THRESHOLD);
        assertEquals(100L * 1024 * 1024, DeltaShareConstants.ROW_GROUP_SIZE_THRESHOLD);
    }
    
    @Test
    public void testConstantValuesAreReasonable()
    {
        assertTrue("Lambda temp limit should be positive", DeltaShareConstants.LAMBDA_TEMP_LIMIT > 0);
        assertTrue("File size threshold should be positive", DeltaShareConstants.FILE_SIZE_THRESHOLD > 0);
        assertTrue("Row group threshold should be positive", DeltaShareConstants.ROW_GROUP_SIZE_THRESHOLD > 0);
        
        assertTrue("Lambda temp limit should be less than 512MB", DeltaShareConstants.LAMBDA_TEMP_LIMIT < 512L * 1024 * 1024);
        assertTrue("File size threshold should be reasonable", DeltaShareConstants.FILE_SIZE_THRESHOLD < 1024L * 1024 * 1024);
        assertTrue("Row group threshold should be reasonable", DeltaShareConstants.ROW_GROUP_SIZE_THRESHOLD < 512L * 1024 * 1024);
    }
    
    @Test
    public void testEndpointFormats()
    {
        assertNotNull("Shares endpoint should not be null", DeltaShareConstants.SHARES_ENDPOINT);
        
        assertTrue("Schemas endpoint should contain format placeholder", 
            DeltaShareConstants.SCHEMAS_ENDPOINT.contains("%s"));
        assertTrue("Tables endpoint should contain format placeholders", 
            DeltaShareConstants.TABLES_ENDPOINT.contains("%s"));
        assertTrue("Table metadata endpoint should contain format placeholders", 
            DeltaShareConstants.TABLE_METADATA_ENDPOINT.contains("%s"));
        assertTrue("Query table endpoint should contain format placeholders", 
            DeltaShareConstants.QUERY_TABLE_ENDPOINT.contains("%s"));
    }
    
    @Test
    public void testBearerTokenFormat()
    {
        assertTrue("Bearer prefix should end with space", DeltaShareConstants.BEARER_PREFIX.endsWith(" "));
        assertEquals("Bearer ", DeltaShareConstants.BEARER_PREFIX);
    }
    
    @Test
    public void testConstantImmutability()
    {
        try {
            java.lang.reflect.Field[] fields = DeltaShareConstants.class.getDeclaredFields();
            
            for (java.lang.reflect.Field field : fields) {
                assertTrue("All fields should be static", java.lang.reflect.Modifier.isStatic(field.getModifiers()));
                assertTrue("All fields should be final", java.lang.reflect.Modifier.isFinal(field.getModifiers()));
                assertTrue("All fields should be public", java.lang.reflect.Modifier.isPublic(field.getModifiers()));
            }
        } catch (Exception e) {
            assertNotNull("Should be able to access class fields", e);
        }
    }
}
