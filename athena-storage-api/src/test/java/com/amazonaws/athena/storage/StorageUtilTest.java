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
package com.amazonaws.athena.storage;

import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import static com.amazonaws.athena.storage.gcs.GcsTestBase.CSV_FILE;
import static com.amazonaws.athena.storage.StorageUtil.getCsvRecordCount;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityName;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityNameFromFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class StorageUtilTest
{
    @Test
    public void testCsvRecordCount() throws URISyntaxException
    {
        URL parquetFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        File csvFile = new File(parquetFileResourceUri.toURI());
        long totalRecords = getCsvRecordCount(csvFile);
        assertTrue(totalRecords > 0, "No records found");
    }

    @Test
    public void testSQLEntityNameWithFileExtensionEscaping()
    {
        String validEntityName = getValidEntityNameFromFile("919table1.parquet", ".parquet");
        assertEquals(validEntityName, "table1");

        validEntityName = getValidEntityNameFromFile("tabl/e2.csv", ".csv");
        assertTrue(validEntityName.endsWith("tabl_e2"));

        validEntityName = getValidEntityNameFromFile("tabl_e3.csv", ".csv");
        assertTrue(validEntityName.endsWith("tabl_e3"));

        validEntityName = getValidEntityNameFromFile("t\\\\abl_e#4.parquet", ".parquet");
        assertTrue(validEntityName.endsWith("t_abl_e_4"));

        validEntityName = getValidEntityNameFromFile("tabl_e!!5.csv", ".csv");
        assertTrue(validEntityName.endsWith("tabl_e_5"));

        validEntityName = getValidEntityNameFromFile("tabl_e@6.csv", ".csv");
        assertTrue(validEntityName.endsWith("tabl_e_6"));

        validEntityName = getValidEntityNameFromFile("tabl_e.7.csv", ".csv");
        assertTrue(validEntityName.endsWith("tabl_e_7"));

        validEntityName = getValidEntityNameFromFile("tabl_e$8.csv", ".csv");
        assertTrue(validEntityName.endsWith("tabl_e_8"));
    }

    @Test
    public void testSQLEntityNameEscaping()
    {
        String validEntityName = getValidEntityName("919table1");
        assertEquals(validEntityName, "table1");

        validEntityName = getValidEntityName("tabl/e2");
        assertTrue(validEntityName.endsWith("tabl_e2"));

        validEntityName = getValidEntityName("tabl_e3");
        assertTrue(validEntityName.endsWith("tabl_e3"));

        validEntityName = getValidEntityName("t\\\\abl_e#4");
        assertTrue(validEntityName.endsWith("t_abl_e_4"));

        validEntityName = getValidEntityName("tabl_e!!5");
        assertTrue(validEntityName.endsWith("tabl_e_5"));

        validEntityName = getValidEntityName("tabl_e@6");
        assertTrue(validEntityName.endsWith("tabl_e_6"));

        validEntityName = getValidEntityName("tabl_e.7");
        assertTrue(validEntityName.endsWith("tabl_e_7"));

        validEntityName = getValidEntityName("tabl_e$8");
        assertTrue(validEntityName.endsWith("tabl_e_8"));
    }
}
