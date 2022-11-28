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
import com.amazonaws.athena.connectors.gcs.storage.StorageDatasource;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageTable;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static com.amazonaws.athena.connectors.gcs.GcsUtil.isFieldTypeNull;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.createUri;

public class GcsSchemaUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsSchemaUtils.class);

    private static final ArrowType.ArrowTypeID TIMESTAMP_NANO_TYPE = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null).getTypeID();

    private GcsSchemaUtils()
    {
    }

    /**
     * Builds the table schema based on the provided field by the retrieved instance of {@link StorageTable}
     *
     * @param datasource   An instance of {@link StorageDatasource}
     * @param databaseName Name of the bucket in GCS
     * @param tableName    Name of the storage object (file) from GCS
     * @return An instance of {@link Schema}
     */
    public static Schema buildTableSchema(StorageDatasource datasource, String databaseName, String tableName) throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        Optional<StorageTable> optionalStorageTable = datasource.getStorageTable(databaseName, tableName);
        if (optionalStorageTable.isPresent()) {
            StorageTable table = optionalStorageTable.get();
            LOGGER.info("Schema Fields\n{}", table.getFields());
            for (Field field : table.getFields()) {
                if (isFieldTypeNull(field)) {
                    field = Field.nullable(field.getName(), Types.MinorType.VARCHAR.getType());
                }
                schemaBuilder.addField(getCompatibleField(field));
            }
//            schemaBuilder.addStringField(BLOCK_PARTITION_COLUMN_NAME);
            Schema schema = schemaBuilder.build();
            System.out.printf("Schema Fields %n%s%n", schema.getFields());
            return schema;
        }
        else {
            LOGGER.error("Table '{}' was not found under schema '{}'", tableName, databaseName);
            throw new GcsConnectorException("Table '" + tableName + "' was not found under schema '" + databaseName + "'");
        }
    }

    public static Field getCompatibleField(Field field)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        switch (fieldType) {
            case TIMESTAMPNANO:
            case TIMESTAMPMILLI:
            case TIMEMICRO:
            case TIMESTAMPMICRO:
            case TIMESTAMPMILLITZ:
            case TIMESTAMPMICROTZ:
            case TIMENANO:
                if (field.isNullable()) {
                    return new Field(field.getName(),
                            FieldType.nullable(Types.MinorType.DATEMILLI.getType()), List.of());
                }
                else {
                    return new Field(field.getName(),
                            FieldType.notNullable(Types.MinorType.DATEMILLI.getType()), List.of());
                }
            case FIXEDSIZEBINARY:
            case LARGEVARBINARY:
            case VARBINARY:
                if (field.isNullable()) {
                    return new Field(field.getName(),
                            FieldType.nullable(Types.MinorType.VARCHAR.getType()), List.of());
                }
                else {
                    return new Field(field.getName(),
                            FieldType.notNullable(Types.MinorType.VARCHAR.getType()), List.of());
                }
            default:
                return field;
        }
    }

    public static Optional<Schema> getSchemaFromGcsPrefix(String prefix, FileFormat fileFormat, StorageDatasourceConfig config) throws Exception
    {
        String uri = createUri(prefix);
        return getSchemaFromGcsUri(uri, fileFormat);
    }

    public static Optional<Schema> getSchemaFromGcsUri(String uri, FileFormat fileFormat) throws Exception
    {
        ScanOptions options = new ScanOptions(1);
        try (
                BufferAllocator allocator = new RootAllocator();
                DatasetFactory datasetFactory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), fileFormat, uri);
                Dataset dataset = datasetFactory.finish();
                Scanner scanner = dataset.newScan(options);
                ArrowReader reader = scanner.scanBatches()
        ) {
            return Optional.of(reader.getVectorSchemaRoot().getSchema());
        }
    }
}
