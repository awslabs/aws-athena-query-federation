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

import com.amazonaws.athena.storage.gcs.io.GcsInputFile;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import java.util.List;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GcsInputFile.class})
public class GcsParquetSplitUtilTest
{

    @Rule
    public PowerMockRule rule = new PowerMockRule();

    static ParquetFileReader reader;

    @BeforeClass
    public static void setUp()
    {
        BlockMetaData block = new BlockMetaData();
        block.setRowCount(12);
        BlockMetaData block1 = new BlockMetaData();
        block.setRowCount(12);
        reader = mock(ParquetFileReader.class);
        PowerMockito.when(reader.getRecordCount()).thenReturn(100L);
        PowerMockito.when(reader.getRowGroups()).thenReturn(List.of(block, block1));
    }

    @Test
    public void testGetStorageSplitList()
    {
        List<StorageSplit> storageSplitList = GcsParquetSplitUtil.getStorageSplitList("FILE_NAME",
                reader, 200);
        assertNotNull(storageSplitList);
    }
}
