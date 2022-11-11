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

import com.amazonaws.athena.storage.common.PagedObject;
import com.amazonaws.athena.storage.common.StorageObject;
import com.amazonaws.athena.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.storage.datasource.exception.DatabaseNotFoundException;
import com.amazonaws.athena.storage.gcs.SeekableGcsInputStream;
import com.amazonaws.athena.storage.gcs.io.GcsStorageProvider;
import com.amazonaws.athena.storage.gcs.io.StorageFile;
import com.google.cloud.storage.StorageOptions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({SeekableGcsInputStream.class, StorageFile.class, StorageOptions.class})
public class AbstractStorageDatasourceTest extends GcsTestBase
{
    public static final String TABLE_OBJECTS = "tableObjects";
    public static final String DATABASE_BUCKETS = "databaseBuckets";
    public static final String STORAGE_PROVIDER = "storageProvider";
    public static final String LOADED_ENTITIES_LIST = "loadedEntitiesList";
    public static final String EXTENSION = "extension";
    private static Map<String, String> csvProps;

    @Mock
    GcsStorageProvider storageProvider;

    AbstractStorageDatasource abstractStorageDatasource;
    static List<String> bucketList = new ArrayList<>();

    @BeforeClass
    public static void setUp()
    {
        csvProps = new HashMap<>();
        csvProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        csvProps.putAll(properties);
        bucketList.add("test");
    }

    @Before
    public void setUpTest()
    {
        abstractStorageDatasource = PowerMockito.mock(AbstractStorageDatasource.class);
    }

    @Test
    public void testGetAllDatabases()
    {
        PowerMockito.when(storageProvider.getAllBuckets()).thenReturn(bucketList);
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .getAllDatabases();
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE_PROVIDER, storageProvider);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, new ArrayList<>());

        List<String> bList = abstractStorageDatasource.getAllDatabases();
        assertEquals(bList.size(), bucketList.size());
    }

    @Test
    public void testGetAllTables() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .getAllTables("test", null, 2);
        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));

        TableListResult bList = abstractStorageDatasource.getAllTables("test", null, 2);
        assertNotNull(bList);
    }

    @Test
    public void testCheckDatastoreForPagination() throws IOException
    {
        PowerMockito.when(abstractStorageDatasource.loadTablesInternal(anyString(), anyString(), anyInt())).thenReturn("token");
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesWithContinuationToken("test", null, 2);
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, "datasourceConfig", new StorageDatasourceConfig().credentialsJson(gcsCredentialsJson).properties(properties));
        String token = abstractStorageDatasource.loadTablesWithContinuationToken("test", null, 2);
        assertNull(token);

    }

    @Test
    public void testLoadAllTables() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadAllTables("test");
        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));

        List<StorageObject> bList = abstractStorageDatasource.loadAllTables("test");
        assertNotNull(bList);
    }

    @Test
    public void testCheckDatastoreForAll() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .checkDatastoreForDatabase("test");
        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));

        abstractStorageDatasource.checkDatastoreForDatabase("test");
        verify(abstractStorageDatasource, times(3)).checkDatastoreForDatabase("test");
    }

    @Test
    public void testGetStorageTable() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .getStorageTable("test", "test");
        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));

        Optional<StorageTable> obj = abstractStorageDatasource.getStorageTable("test", "test");
        assertNotNull(obj);
    }

    @Test
    public void testLoadTablesInternal() throws IOException
    {
        PowerMockito.when(storageProvider.getObjectNames(anyString(), anyString(), anyInt())).thenReturn(PagedObject.builder().fileNames(bucketList).nextToken(null).build());

        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test", "null", 2);

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE_PROVIDER, storageProvider);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        String token = abstractStorageDatasource.loadTablesInternal("test", "null", 2);
        assertNull(token);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void testLoadTablesInternalException() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test", null, 2);
        PowerMockito.when(abstractStorageDatasource.convertBlobsToTableObjectsMap(Mockito.any(), Mockito.any())).thenReturn(Map.of(new StorageObject("test", "test.csv", false), List.of("test")));

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of());
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE_PROVIDER, storageProvider);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        abstractStorageDatasource.loadTablesInternal("test", null, 2);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void testLoadTablesInternalWithoutTokenException() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test");
        PowerMockito.when(abstractStorageDatasource.convertBlobsToTableObjectsMap(Mockito.any(), Mockito.any())).thenReturn(Map.of(new StorageObject("test", "test.csv", false), List.of("test")));

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test1", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE_PROVIDER, storageProvider);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        abstractStorageDatasource.loadTablesInternal("test");
    }

    @Test
    public void testLoadTablesInternalWithoutToken() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test");
        PowerMockito.when(abstractStorageDatasource.convertBlobsToTableObjectsMap(Mockito.any(), Mockito.any())).thenReturn(Map.of(new StorageObject("test", "test.csv", false), List.of("test")));

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE_PROVIDER, storageProvider);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        abstractStorageDatasource.loadTablesInternal("test");
        verify(abstractStorageDatasource, times(3)).loadTablesInternal("test");
    }

    @Test
    public void testConvertBlobsToTableObjectsMap() throws IOException
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .convertBlobsToTableObjectsMap(BUCKET, bucketList);


        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, EXTENSION, "csv");
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));

        Map<StorageObject, List<String>> obj = abstractStorageDatasource.convertBlobsToTableObjectsMap(BUCKET, bucketList);
        assertNotNull(obj);
    }

    @Test
    public void testAddTable() throws IOException
    {
        HashMap<StorageObject, List<String>> map = new HashMap<>();
        List<String> sList = new ArrayList<>();
        sList.add("test");
        map.put(new StorageObject("test", "test.csv", false), sList);
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .addTable("test", "test.csv", map);

        PowerMockito.when(storageProvider.isPartitionedDirectory(anyString(), anyString())).thenReturn(true);
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE_PROVIDER, storageProvider);
        Whitebox.setInternalState(abstractStorageDatasource, "datasourceConfig", new StorageDatasourceConfig().credentialsJson(gcsCredentialsJson).properties(csvProps));
        Whitebox.setInternalState(abstractStorageDatasource, EXTENSION, "csv");
        abstractStorageDatasource.addTable("test", "test.csv", map);
        verify(abstractStorageDatasource, times(3)).addTable("test","test.csv", map);
    }

    @Test
    public void testTablesLoadedForDatabase()
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .tablesLoadedForDatabase("test");

        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        boolean st = abstractStorageDatasource.tablesLoadedForDatabase("test");
        assertFalse(st);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void testTablesLoadedForDatabaseException()
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .tablesLoadedForDatabase("test");

        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of());
        boolean st = abstractStorageDatasource.tablesLoadedForDatabase("test");
        assertFalse(st);
    }


}
