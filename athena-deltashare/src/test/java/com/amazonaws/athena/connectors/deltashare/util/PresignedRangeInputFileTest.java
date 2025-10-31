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
package com.amazonaws.athena.connectors.deltashare.util;

import org.junit.Test;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PresignedRangeInputFileTest
{
    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullUrl()
    {
        new PresignedRangeInputFile(null, 1024);
    }
    
    @Test
    public void testConstructorWithZeroFileSize()
    {
        try {
            new PresignedRangeInputFile("https://example.com/file.parquet", 0);
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testConstructorWithNegativeFileSize()
    {
        try {
            new PresignedRangeInputFile("https://example.com/file.parquet", -1);
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testGetSignedUrl()
    {
        String url = "https://example.com/test-file.parquet";
        PresignedRangeInputFile inputFile = new PresignedRangeInputFile(url, 1024);
        assertEquals(url, inputFile.getSignedUrl());
    }
    
    @Test
    public void testGetLength() throws IOException
    {
        long expectedLength = 1024 * 1024;
        PresignedRangeInputFile inputFile = new PresignedRangeInputFile("https://example.com/file.parquet", expectedLength);
        assertEquals(expectedLength, inputFile.getLength());
    }
    
    @Test
    public void testToString()
    {
        String url = "https://example.com/test-file.parquet";
        PresignedRangeInputFile inputFile = new PresignedRangeInputFile(url, 1024);
        String result = inputFile.toString();
        
        assertNotNull(result);
        assertTrue(result.contains(url));
        assertTrue(result.contains("1024"));
    }
}
