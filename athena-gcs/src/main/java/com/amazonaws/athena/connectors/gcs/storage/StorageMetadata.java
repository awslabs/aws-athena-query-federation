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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connectors.gcs.common.PartitionFolder;
import com.amazonaws.athena.connectors.gcs.common.PartitionLocation;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageTable;
import com.amazonaws.services.glue.AWSGlue;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface StorageMetadata
{
    List<Field> getTableFields(String bucketName, List<String> objectNames) throws IOException;

    /**
     * Returns a storage object (file) as a DB table with field names and associated file type
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @return An instance of {@link StorageTable} with column metadata
     */
    Optional<StorageTable> getStorageTable(String databaseName, String tableName) throws Exception;

    /**
     * Retrieves a list of StorageSplit that essentially contain the list of all files for a given table type in a storage location
     *
     * @param tableType Type of the table (e.g., PARQUET or CSV)
     * @param partitions List of {@link PartitionLocation} instances
     * @return A list of {@link StorageSplit} instances
     */
    List<StorageSplit> getStorageSplits(String tableType, PartitionLocation partitions);

    /**
     * Returns the Datasource specific file format to be used to read a file (for retrieving schema or fetching data)
     *
     * @return An instance of FileFormat
     */
    FileFormat getFileFormat();

    /**
     *  Used to test with test classes integrated directly with GCS bucket
     *  <p>
     *      Those test classes are not part of the artifact and not present in src/test. However, one may use it to
     *      test few methods that requires real-life debugging
     *  </p>
     * @return An instance of {@link Storage} from Google Storage SDK
     */
    @VisibleForTesting
    Storage getStorage();

    List<PartitionFolder> getPartitionFolders(MetadataRequest request, TableName tableName, AWSGlue glueClient);
}
