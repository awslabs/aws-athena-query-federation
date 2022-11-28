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
package com.amazonaws.athena.connectors.gcs.storage.datasource;

import com.amazonaws.athena.connectors.gcs.storage.StorageDatasource;
import com.amazonaws.athena.connectors.gcs.storage.datasource.exception.UncheckedStorageDatasourceException;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static java.util.Objects.requireNonNull;

public class StorageDatasourceFactory
{
    private StorageDatasourceFactory()
    {
    }

    /**
     * Creates a data source based on properties. It highly depends on an environment variable named file_extension
     * Currently, file_extension only supports either PARQUET or CSV. Based on this value, this factory method will
     * create either ParquetDataSource or CsvDatasource which is a subclass of {@link com.amazonaws.athena.connectors.gcs.storage.AbstractStorageDatasource},
     * which in turn an implementation of {@link StorageDatasource}
     *
     * @param credentialJsonString Credential JSON to access target storage service for example Google's GCS (Google Cloud Storage)
     * @param properties              Map of property/value pairs from the lambda environment
     * @return An instance of StorageDatasource
     * @see StorageDatasource
     */
    public static StorageDatasource createDatasource(String credentialJsonString,
                                                     Map<String, String> properties) throws InvocationTargetException,
            NoSuchMethodException, InstantiationException, IllegalAccessException
    {
        String fileFormat = properties.get(FILE_EXTENSION_ENV_VAR);
        fileFormat = requireNonNull(fileFormat, "File extension was not specified, please specify any of parquet, or csv (cae insensitive");
        FileFormat.SupportedFileFormat supportedFileFormat = FileFormatSupport.getSupportedFormat(fileFormat);
        if (supportedFileFormat == null) {
            throw new UncheckedStorageDatasourceException("File extension " + fileFormat
                    + " not yet supported. Please specify any of parquet, or csv (case insensitive)");
        }
        return supportedFileFormat.createDatasource(credentialJsonString, properties);
    }
}
