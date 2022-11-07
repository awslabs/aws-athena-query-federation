/*-
 * #%L
 * Amazon Athena Storage API
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

import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.gcs.io.GcsOfflineStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GoogleCredentials.class, StorageOptions.class})
public class FileCacheFactoryTest extends GcsTestBase
{
    static File csvFile;
    Storage storage;


    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeClass
    public static void setUpBeforeAllTests() throws URISyntaxException
    {
        setUpBeforeClass();
        String tempFileName = String.format("%s_%s.cache", BUCKET, CSV_FILE);
        File tempDir = FileUtils.getTempDirectory();
        String tempFilePath = tempDir.getPath() + Path.SEPARATOR + tempFileName;
        File tempFile = new File(tempFilePath);
        if (tempFile.exists()) {
            tempFile.delete();
        }
        URL csvFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        csvFile = new File(csvFileResourceUri.toURI());
    }

    @Before
    public void setUp() throws Exception
    {
        ReadChannel channel = mock(ReadChannel.class);
        storage = mockStorageWithBlobIterator(BUCKET, csvFile.length(), CSV_FILE);
        PowerMockito.when(storage.reader(Mockito.any())).thenReturn(channel);
        PowerMockito.when(channel.read(Mockito.any())).thenReturn(12);
    }

    @Test
    public void testCreateOfflineGcsStream() throws IOException
    {
        GcsOfflineStream gcsOfflineStream = FileCacheFactory.createOfflineGcsStream(storage, BUCKET, CSV_FILE);
        assertNotNull(gcsOfflineStream);
    }

    @Test
    public void testCreateOfflineGcsStreamFromCache() throws IOException
    {

        GcsOfflineStream gcsOfflineStream = FileCacheFactory.createOfflineGcsStream(storage, BUCKET, CSV_FILE);
        assertNotNull(gcsOfflineStream);
    }
}
