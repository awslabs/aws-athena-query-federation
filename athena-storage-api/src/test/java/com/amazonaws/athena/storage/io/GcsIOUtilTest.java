/*-
 * #%L
 * athena-storage-api
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
package com.amazonaws.athena.storage.io;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GcsIOUtilTest
{
    @Test
    public void testCheckPartitionLocationIsSuccessful()
    {
        String baseLocation = "/abc/state='UP'/";
        String actualPartitionedFolder = "state='UP'";
        String expectedPartitionedFolder = GcsIOUtil.getFolderName(baseLocation);
        assertEquals("Folders didn't match with expected", expectedPartitionedFolder, actualPartitionedFolder);
    }

    @Test
    public void testCheckFileHasExtension()
    {
        String filePath = "/abc/state='UP'/datafile.parquet";
        assertTrue("File didn't have an extension", GcsIOUtil.containsExtension(filePath));
    }

    @Test
    public void testCheckFileHasNotExtension()
    {
        String filePath = "/abc/state='UP'/datafile.parquet";
        assertTrue("File did have an extension", GcsIOUtil.containsExtension(filePath));
    }
}
