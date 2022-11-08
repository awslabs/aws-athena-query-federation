/*-
 * #%L
 * athena-gcs
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.StorageTable;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static com.amazonaws.athena.storage.StorageConstants.BLOCK_PARTITION_COLUMN_NAME;

public class GcsSchemaUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsSchemaUtils.class);

    /**
     * Builds the table schema based on the provided field by the retrieved instance of {@link StorageTable}
     *
     * @param datasource   An instance of {@link StorageDatasource}
     * @param databaseName Name of the bucket in GCS
     * @param tableName    Name of the storage object (file) from GCS
     * @return An instance of {@link Schema}
     */
    protected Schema buildTableSchema(StorageDatasource datasource, String databaseName, String tableName) throws IOException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        Optional<StorageTable> optionalStorageTable = datasource.getStorageTable(databaseName, tableName);
        if (optionalStorageTable.isPresent()) {
            StorageTable table = optionalStorageTable.get();
            LOGGER.info("Schema Fields\n{}", table.getFields());
            for (Field field : table.getFields()) {
                schemaBuilder.addField(field);
            }
            schemaBuilder.addStringField(BLOCK_PARTITION_COLUMN_NAME);
            return schemaBuilder.build();
        }
        else {
            LOGGER.error("Table '{}' was not found under schema '{}'", tableName, databaseName);
            throw new GcsConnectorException("Table '" + tableName + "' was not found under schema '" + databaseName + "'");
        }
    }
}
