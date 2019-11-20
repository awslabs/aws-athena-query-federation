/*-
 * #%L
 * athena-udfs
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
package com.amazonaws.athena.connectors.udfs;

import org.junit.Test;

import java.util.zip.DataFormatException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AthenaUDFHandlerTest
{
    private AthenaUDFHandler athenaUDFHandler = new AthenaUDFHandler();

    @Test
    public void testCompressAndDecompressHappyCase()
    {
        String input = "StringToBeCompressed";

        String compressed = athenaUDFHandler.compress(input);
        assertEquals("eJwLLinKzEsPyXdKdc7PLShKLS5OTQEAUrEH9w==", compressed);

        String decompressed = athenaUDFHandler.decompress(compressed);
        assertEquals(input, decompressed);
    }

    @Test(expected = NullPointerException.class)
    public void testCompressNull()
    {
        athenaUDFHandler.compress(null);
    }

    @Test(expected = NullPointerException.class)
    public void testDecompressNull()
    {
        athenaUDFHandler.decompress(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressNonCompressedInput()
    {
        athenaUDFHandler.decompress("jklasdfkljsadflkafdsjklsdfakljadsfkjldaadfasdffsa");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressBadInputEncoding()
    {
        athenaUDFHandler.decompress("78 da 0b c9 cf ab 54 70 cd 49 2d 4b 2c");
    }

    @Test
    public void testDecompressTruncatedInput()
    {
        try {
            athenaUDFHandler.decompress("");
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof DataFormatException);
            assertEquals("Input is truncated", e.getCause().getMessage());
        }
    }

}
