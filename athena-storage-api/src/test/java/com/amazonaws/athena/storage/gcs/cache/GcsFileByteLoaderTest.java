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
package com.amazonaws.athena.storage.gcs.cache;

import com.amazonaws.athena.storage.gcs.GcsTestBase;
import com.amazonaws.athena.storage.gcs.SeekableGcsInputStream;
import com.amazonaws.athena.storage.gcs.io.GcsInputFile;
import com.amazonaws.athena.storage.gcs.io.GcsFileByteLoader;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GcsFileByteLoader.class, GcsInputFile.class})
public class GcsFileByteLoaderTest extends GcsTestBase
{

    @BeforeClass
    public static void setUp()
    {
    }

    @Test(expected = Exception.class)
    public void testGcsFileByteLoader() throws Exception
    {
        Storage storage1 = mock(Storage.class);
        Blob blob1 = mock(Blob.class);
        ReadChannel channel = mock(ReadChannel.class);
        PowerMockito.when(storage1.get((BlobId) Mockito.any())).thenReturn(blob1);
        PowerMockito.when(blob1.getSize()).thenReturn(10L);
        PowerMockito.when(storage1.reader(Mockito.any())).thenReturn(channel);
        PowerMockito.whenNew(SeekableGcsInputStream.class).withAnyArguments().thenReturn(mock(SeekableGcsInputStream.class));
        String fileName = RandomStringUtils.random(10, true, false) + ".parquet";
        GcsFileByteLoader file = new GcsFileByteLoader(storage1, "bucketName", fileName);
        assertNotNull(file);
    }

}
