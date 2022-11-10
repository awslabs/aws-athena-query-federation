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
package com.amazonaws.athena.storage.gcs;

import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.gcs.io.GcsInputFile;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GcsInputFile.class})
public class GcsCsvSplitUtilTest extends GcsTestBase
{

    File csvFile;

    @Test
    public void testGetStorageSplitList() throws URISyntaxException
    {
        URL csvFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        csvFile = new File(csvFileResourceUri.toURI());
        List<StorageSplit> storageSplitList = GcsCsvSplitUtil.getStorageSplitList(99, "Consumer_Complaints.csv", 10);
        assertNotNull(storageSplitList);
    }

}
