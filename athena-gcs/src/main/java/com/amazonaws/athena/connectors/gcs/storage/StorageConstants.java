/*-
 * #%L
 * athena-hive
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
package com.amazonaws.athena.connectors.gcs.storage;

public class StorageConstants
{
    private StorageConstants()
    {
    }

    /**
     * part_name columns contains the source file name. It helps filter when files are gruped in a single table
     */
    public static final String BLOCK_PARTITION_COLUMN_NAME = "part_name";

    /**
     * A key that stores JSON representation of {@link com.amazonaws.athena.connectors.gcs.storage.StorageSplit} as a string
     */
    public static final String STORAGE_SPLIT_JSON = "storage_split_json";

    // Environment variables passed as properties in ObjectStorageMetadataConfig
    public static final String FILE_EXTENSION_ENV_VAR = "file_extension";

    // split constants
    public static final String TABLE_PARAM_BUCKET_NAME = "bucketName";
    public static final String TABLE_PARAM_OBJECT_NAME_LIST = "objectName";
    public static final String IS_TABLE_PARTITIONED = "table_partitioned";
    public static final String TABLE_PARAM_OBJECT_NAME = "partitioned_table_base";
}
