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

public class HttpRangeSeekableInputStreamTest
{
    @Test
    public void testConstructorWithNullUrl()
    {
        try {
            new HttpRangeSeekableInputStream(null, 1024);
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testConstructorWithZeroFileSize()
    {
        try {
            new HttpRangeSeekableInputStream("https://example.com/file.parquet", 0);
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testConstructorWithNegativeFileSize()
    {
        try {
            new HttpRangeSeekableInputStream("https://example.com/file.parquet", -1);
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testGetPosInitialPosition() throws IOException
    {
        HttpRangeSeekableInputStream stream = new HttpRangeSeekableInputStream("https://example.com/file.parquet", 1024);
        assertEquals(0, stream.getPos());
        stream.close();
    }
    
    @Test
    public void testCloseOperation() throws IOException
    {
        HttpRangeSeekableInputStream stream = new HttpRangeSeekableInputStream("https://example.com/file.parquet", 1024);
        stream.close();
        stream.close();
    }
}
