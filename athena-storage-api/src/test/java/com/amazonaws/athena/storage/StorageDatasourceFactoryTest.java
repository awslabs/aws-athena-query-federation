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

import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PowerMockIgnore;

import java.lang.reflect.InvocationTargetException;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*",
        "com.amazonaws.athena.storage.datasource.StorageDatasourceFactory"})
public class StorageDatasourceFactoryTest extends GcsTestBase
{

    @BeforeClass
    public static void setUpBeforeAllTests()
    {
        setUpBeforeClass();
    }

    @Test
    public void testGetParquetDataSource()
    {
        Throwable throwable = null;
        try {
            StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
            assertNull("PARQUET StorageDatasource was not null", parquetDatasource);
        }
        catch (Exception exception) {
            throwable = exception;
        }
        assertTrue(throwable instanceof InvocationTargetException);
    }

    @Test
    public void testGetCsvDataSource()
    {
        Throwable throwable = null;
        try {
            parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
            StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
            assertNull("CSV StorageDatasource was not null", parquetDatasource);
        }
        catch (Exception exception) {
            throwable = exception;
        }
        assertTrue(throwable instanceof InvocationTargetException);
    }
}
