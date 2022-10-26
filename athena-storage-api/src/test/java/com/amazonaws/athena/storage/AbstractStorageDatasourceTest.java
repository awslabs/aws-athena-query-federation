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


import com.amazonaws.athena.storage.datasource.GcsDatasourceConfig;
import com.amazonaws.athena.storage.datasource.exception.DatabaseNotFoundException;
import com.amazonaws.athena.storage.gcs.SeekableGcsInputStream;
import com.amazonaws.athena.storage.gcs.io.StorageFile;
import com.google.api.gax.paging.Page;
import com.google.cloud.PageImpl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static com.amazonaws.athena.storage.StorageConstants.FILE_NAME_PATTERN_ENV_VAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({SeekableGcsInputStream.class, StorageFile.class, StorageOptions.class})
public class AbstractStorageDatasourceTest extends GcsTestBase
{
    public static final String TABLE_OBJECTS = "tableObjects";
    public static final String DATABASE_BUCKETS = "databaseBuckets";
    public static final String STORAGE = "storage";
    public static final String LOADED_ENTITIES_LIST = "loadedEntitiesList";
    public static final String EXTENSION = "extension";
    private static Map<String, String> csvProps;

    @Mock
    private PageImpl<Bucket> blob;
    AbstractStorageDatasource abstractStorageDatasource;

    @BeforeClass
    public static void setUp()
    {
        csvProps = new HashMap<>();
        csvProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        csvProps.put(FILE_NAME_PATTERN_ENV_VAR, "csv");
        csvProps.putAll(properties);
    }

    @Before
    public void setUpTest()
    {
        abstractStorageDatasource = PowerMockito.mock(AbstractStorageDatasource.class);
    }

    @Test
    public void testGetAllDatabases()
    {
        Storage storage = mockStorageWithBlobIterator("test");

        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .getAllDatabases();
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE, storage);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, new ArrayList<>());

        List<String> bList = abstractStorageDatasource.getAllDatabases();
        assertNotNull(bList);
    }

    @Test
    public void testGetAllTables()
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .getAllTables("test", null, 2);
        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));

        TableListResult bList = abstractStorageDatasource.getAllTables("test", null, 2);
        assertNotNull(bList);

    }

    @Test
    public void testCheckMetastoreForPagination()
    {
        PowerMockito.when(abstractStorageDatasource.loadTablesInternal(anyString(), anyString(), anyInt())).thenReturn("token");
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .checkMetastoreForPagination("test", null, 2);
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, "metastoreConfig", new GcsDatasourceConfig().credentialsJson(gcsCredentialsJson).properties(properties));
        String token = abstractStorageDatasource.checkMetastoreForPagination("test", null, 2);
        assertNull(token);

    }

    @Test
    public void testLoadAllTables()
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadAllTables("test");
        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));

        List<String> bList = abstractStorageDatasource.loadAllTables("test");
        assertNotNull(bList);
    }

    @Test
    @Ignore
    public void testCheckMetastoreForAll()
    {
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .checkMetastoreForAll("test");
        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, Map.of("test", Map.of("test", List.of("test"))));
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));

        abstractStorageDatasource.checkMetastoreForAll("test");
        verify(abstractStorageDatasource, times(1)).checkMetastoreForAll("test");
    }

    @Test
    public void testGetStorageTable()
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
    public void testLoadTablesInternal()
    {
        Storage st = mock(Storage.class);
        PowerMockito.when(blob.getNextPageToken()).thenReturn(null);
        doReturn(blob).when(st).list(anyString(), Mockito.any());
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test", null, 2);
        PowerMockito.when(abstractStorageDatasource.addTables2FileMap(Mockito.any())).thenReturn(Map.of("test", List.of("test")));

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE, st);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        String token = abstractStorageDatasource.loadTablesInternal("test", null, 2);
        assertNull(token);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void testLoadTablesInternalException()
    {
        Storage st = mock(Storage.class);
        PowerMockito.when(blob.getNextPageToken()).thenReturn(null);
        doReturn(blob).when(st).list(anyString(), Mockito.any());
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test", null, 2);
        PowerMockito.when(abstractStorageDatasource.addTables2FileMap(Mockito.any())).thenReturn(Map.of("test", List.of("test")));

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of());
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE, st);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        abstractStorageDatasource.loadTablesInternal("test", null, 2);
    }

    @Test(expected = DatabaseNotFoundException.class)
    public void testLoadTablesInternalWithoutTokenException()
    {
        Storage st = mock(Storage.class);
        PowerMockito.when(blob.getNextPageToken()).thenReturn(null);
        doReturn(blob).when(st).list(anyString());
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test");
        PowerMockito.when(abstractStorageDatasource.addTables2FileMap(Mockito.any())).thenReturn(Map.of("test", List.of("test")));

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test1", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE, st);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        abstractStorageDatasource.loadTablesInternal("test");
    }

    @Test
    @Ignore
    public void testLoadTablesInternalWithoutToken()
    {
        Storage st = mock(Storage.class);
        PowerMockito.when(blob.getNextPageToken()).thenReturn(null);
        doReturn(blob).when(st).list(anyString());
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .loadTablesInternal("test");
        PowerMockito.when(abstractStorageDatasource.addTables2FileMap(Mockito.any())).thenReturn(Map.of("test", List.of("test")));

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, STORAGE, st);
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        abstractStorageDatasource.loadTablesInternal("test");
        verify(abstractStorageDatasource, times(1)).loadTablesInternal("test");
    }

    @Test
    public void testAddTables2FileMap() throws IOException
    {
        Storage storage = mockStorageWithBlobIterator("test");
        String contents = "hello";
        String bucketName = RemoteStorageHelper.generateBucketName();
        BlobId blobId = BlobId.of(bucketName, "hello");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        byte[] content = contents.getBytes(StandardCharsets.UTF_8);
        storage.create(BucketInfo.of(bucketName));
        storage.createFrom(blobInfo, new ByteArrayInputStream(content));

        Page<Blob> buck = storage.list(bucketName);
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .addTables2FileMap(buck);

        Whitebox.setInternalState(abstractStorageDatasource, TABLE_OBJECTS, new HashMap<>());
        Whitebox.setInternalState(abstractStorageDatasource, DATABASE_BUCKETS, Map.of("test", "test"));
        Whitebox.setInternalState(abstractStorageDatasource, EXTENSION, "csv");
        Whitebox.setInternalState(abstractStorageDatasource, LOADED_ENTITIES_LIST, List.of(new AbstractStorageDatasource.LoadedEntities("test")));
        Map<String, List<String>> obj = abstractStorageDatasource.addTables2FileMap(buck);
        assertNotNull(obj);
    }

    @Test
    @Ignore
    public void testAddTable()
    {
        HashMap<String, List<String>> map = new HashMap<>();
        List<String> sList = new ArrayList<>();
        sList.add("test");
        map.put("test", sList);
        PowerMockito.doCallRealMethod()
                .when(abstractStorageDatasource)
                .addTable("test.csv", map);

        Whitebox.setInternalState(abstractStorageDatasource, "metastoreConfig", new GcsDatasourceConfig()
                .credentialsJson(gcsCredentialsJson)
                .properties(csvProps));
        Whitebox.setInternalState(abstractStorageDatasource, EXTENSION, "csv");
        abstractStorageDatasource.addTable("test.csv", map);
        verify(abstractStorageDatasource, times(1)).addTable("test.csv", Map.of("test", List.of("test")));
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
