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

import com.amazonaws.athena.storage.common.StorageObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({})
public class TableListResultTest
{

    static TableListResult tableListResult;
    static List<String> tableList = List.of("table1", "table2");
    static String nextToken = "testToken";

    @BeforeClass
    public static void setUp()
    {
        List<StorageObject> storageObjects = tableList.stream()
                .map(tableName -> StorageObject.builder().setTabletName(tableName).build())
                .collect(Collectors.toList());
        tableListResult = new TableListResult(storageObjects, nextToken);
    }

    @Test
    public void testGetTables()
    {
        assertNotNull(tableListResult.getTables());
    }

    @Test
    public void testGetNextToken()
    {
        assertEquals(tableListResult.getNextToken(), nextToken);
    }

    @Test
    public void testSetNextToken()
    {
        TableListResult tableListResult1 = Mockito.spy(tableListResult);
        tableListResult1.setNextToken(nextToken);
        verify(tableListResult1, times(1)).setNextToken(nextToken);
    }

    @Test
    public void testSetTables()
    {
        TableListResult tableListResult1 = Mockito.spy(tableListResult);
        List<StorageObject> storageObjects = tableList.stream()
                .map(tableName -> StorageObject.builder().setTabletName(tableName).build())
                .collect(Collectors.toList());
        tableListResult1.setTables(storageObjects);
        verify(tableListResult1, times(1)).setTables(storageObjects);
    }
}
