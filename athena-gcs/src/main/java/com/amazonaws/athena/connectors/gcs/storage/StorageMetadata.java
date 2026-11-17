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
package com.amazonaws.athena.connectors.gcs.storage;

import com.amazonaws.athena.connector.lambda.data.ArrowSchemaUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.GcsUtil;
import com.amazonaws.athena.connectors.gcs.common.PartitionUtil;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Table;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.createUri;
import static com.google.cloud.storage.Storage.BlobListOption.prefix;
import static java.util.Objects.requireNonNull;

public class StorageMetadata
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageMetadata.class);
    private final Storage storage;

    /**
     * Instantiate a storage data source object with provided config
     *
     * @param gcsCredentialJsonString An instance of GcsDatasourceConfig that contains necessary properties for instantiating an appropriate data source
     * @throws IOException If occurs during initializing input stream with GCS credential JSON
     */
    public StorageMetadata(String gcsCredentialJsonString) throws IOException
    {
        requireNonNull(gcsCredentialJsonString, "GCS credential JSON is null");
        GoogleCredentials credentials
                = GoogleCredentials.fromStream(new ByteArrayInputStream(gcsCredentialJsonString.getBytes(StandardCharsets.UTF_8)))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    /**
     * Returns a list of field names and associated file type
     *
     * @param bucketName Name of the bucket
     * @param prefix    Name of the folder
     * @param format   classification param form table
     * @return An instance of {@link List<Field>} with column metadata
     */
    private List<Field> getFields(String bucketName, String prefix, String format, BufferAllocator allocator)
    {
        LOGGER.info("Getting table fields for object {}.{}", bucketName, prefix);
        Optional<String> file = getAnyFilenameInPath(bucketName, prefix);
        // file.get() is purposefully being called without checking here to cause an exception to be thrown because we
        // don't handle the situation where the file is not present.
        return getFileSchema(bucketName, file.get(), FileFormat.valueOf(format.toUpperCase()), allocator).getFields();
    }

    /**
     * Retrieves a list of StorageSplit that essentially contain the list of all files for a given table type in a storage location
     *
     * @param locationUri location uri
     * @return A list of files
     */
    public List<String> getStorageSplits(URI locationUri)
    {
        String bucketName = locationUri.getAuthority();
        // Trim leading / and ensure trailing / for precise prefix matching
        String path = locationUri.getPath().replaceFirst("^/", "");
        path = path.endsWith("/") ? path : path + "/";
        Page<Blob> blobs = storage.list(bucketName, prefix(path));
        return StreamSupport.stream(blobs.iterateAll().spliterator(), false)
            .filter(blob -> isBlobFile(blob))
            .map(blob -> bucketName + "/" + blob.getName())
            .collect(Collectors.toList());
    }

    /**
     * Retrieves a list of partition folders from the GCS bucket based on partition.pattern Table parameter and partition keys set forth in Glue table.
     * Currently NOT filtering partitions (returning all partitions found in GCS path) because testing was not possbile as Athena doesnt work with 
     * partition cols in the WHERE clause. Need to implement partition filtering/pruning utilising substrait plan in the future.
     *
     * @param schema An instance of {@link Schema} that describes underlying Table's schema
     * @param tableInfo Name of the table
     * @param constraints An instance of {@link Constraints}, captured from where clauses
     * @param awsGlue An instance of {@link AWSGlue}
     * @return A list of {@link Map<String, String>} instances
     * @throws URISyntaxException Throws if any occurs during parsing Uri
     */
    public List<Map<String, String>> getPartitionFolders(Schema schema, TableName tableInfo, Constraints constraints, GlueClient awsGlue)
            throws URISyntaxException
    {
        LOGGER.info("Getting partition folder(s) for table {}.{}", tableInfo.getSchemaName(), tableInfo.getTableName());
        Table table = GcsUtil.getGlueTable(tableInfo, awsGlue);
        URI storageLocation = new URI(table.storageDescriptor().location());
        LOGGER.info("Table storage location: {}", storageLocation);
        // Trim leading / (blob names in GCS don't have a leading slash) and ensure trailing /
        String path = storageLocation.getPath().replaceFirst("^/", "");
        final String prefixPath = path.endsWith("/") ? path : path + "/";
        Page<Blob> blobPage = storage.list(storageLocation.getAuthority(), prefix(prefixPath));

        List<Map<String, String>> results = StreamSupport.stream(blobPage.iterateAll().spliterator(), false)
                .filter(blob -> isBlobFile(blob))
                .map(blob -> blob.getName().replaceFirst("^" + prefixPath, ""))
                .peek(name -> LOGGER.info("Name after prefix removal: {}", name))
                .map(name -> name.substring(0, name.lastIndexOf("/") + 1).trim())
                .distinct()
                .peek(folderPath -> LOGGER.info("Distinct folderPath: {}", folderPath))
                .map(folderPath -> PartitionUtil.getPartitionColumnData(table, folderPath))
                .peek(partition -> LOGGER.info("Extracted partition map: {}", partition))
                .filter(partition -> !partition.isEmpty())
                .collect(Collectors.<Map<String, String>>toList());

        LOGGER.info("getPartitionFolders results: {}", results);
        return results;
    }

    /**
     * Retrieves the filename of any file that has a non-zero size within the bucket/prefix
     *
     * @param bucket Name of the bucket
     * @param prefixPath Prefix (aka, folder in Storage service) of the bucket from where this method with retrieve files
     * @return A single file name under the prefix
     */
    protected Optional<String> getAnyFilenameInPath(String bucket, String prefixPath)
    {
        LOGGER.info("Listing nested files for prefix {} under the bucket {}", prefixPath, bucket);
        // Trim leading /
        prefixPath = prefixPath.replaceFirst("^/", "");
        Page<Blob> blobPage = storage.list(bucket, prefix(prefixPath));
        return StreamSupport.stream(blobPage.iterateAll().spliterator(), false)
            .filter(blob -> isBlobFile(blob))
            .map(blob -> blob.getName())
            .findAny();
    }

    // helpers
    /**
     * check whether it is file, It may return folder also
     */
    private boolean isBlobFile(Blob blob)
    {
        return blob.getSize() > 0;
    }

    @VisibleForTesting
    protected Schema getFileSchema(String bucketName, String path, FileFormat format, BufferAllocator allocator)
    {
        requireNonNull(bucketName, "bucketName was null");
        requireNonNull(path, "fileName was null");
        LOGGER.info("Retrieving field schema from file {}, under the bucket {}", path, bucketName);
        String uri = createUri(bucketName, path);
        DatasetFactory factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), format, uri);
        // inspect schema
        return factory.inspect();
    }

    private boolean partitionConstraintsSatisfied(Map<String, String> partitionMap, Map<String, Optional<Set<String>>> columnValueConstraintMap)
    {
        // For all constraints (column name -> set of valid values), validate that any partition column being
        // constrained has a value that is within the constrained value set.
        return columnValueConstraintMap.entrySet().stream()
            .filter(constraintEntry -> partitionMap.containsKey(constraintEntry.getKey()))
            .allMatch(constraintEntry ->
                constraintEntry.getValue()
                    .map(validValues -> validValues.contains(partitionMap.get(constraintEntry.getKey())))
                    .orElse(true) // If the optional constraint value set is not present, then this constraint is satisfied
            );
    }

    /**
     * Builds the table schema based on the provided field
     *
     * @param table      Glue table object
     * @return An instance of {@link Schema}
     */
    public Schema buildTableSchema(Table table, BufferAllocator allocator) throws URISyntaxException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        String locationUri = table.storageDescriptor().location();
        URI storageLocation = new URI(locationUri);
        List<Field> fieldList = getFields(storageLocation.getAuthority(), storageLocation.getPath(), table.parameters().get(CLASSIFICATION_GLUE_TABLE_PARAM), allocator);

        LOGGER.debug("Schema Fields\n{}", fieldList);
        for (Field field : fieldList) {
            if (isArrowTypeNull(field.getType())) {
                field = Field.nullable(field.getName().toLowerCase(), Types.MinorType.VARCHAR.getType());
            }
            else {
                // TODO: Need to check to see if nested field names need to be lowercased since this
                // method was not taking into account the casing of the struct fieldnames before.
                Field updatedField = ArrowSchemaUtils.remapArrowTypesWithinField(field, StorageMetadata::getCompatibleFieldType);
                field = new Field(updatedField.getName().toLowerCase(), updatedField.getFieldType(), updatedField.getChildren());
            }
            schemaBuilder.addField(field);
        }
        return schemaBuilder.build();
    }

    private static ArrowType getCompatibleFieldType(ArrowType arrowType)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(arrowType);
        switch (fieldType) {
            case TIMESTAMPNANO:
            case TIMESTAMPSEC:
            case TIMESTAMPMILLI:
            case TIMEMICRO:
            case TIMESTAMPMICRO:
            case TIMENANO:
                return Types.MinorType.DATEMILLI.getType();
            case TIMESTAMPMILLITZ:
            case TIMESTAMPMICROTZ: {
                return new ArrowType.Timestamp(
                    org.apache.arrow.vector.types.TimeUnit.MILLISECOND,
                    ((ArrowType.Timestamp) arrowType).getTimezone());
            }
            // NOTE: Not sure that both of these should go to Utf8,
            // but just keeping it in-line with how it was before.
            case FIXEDSIZEBINARY:
            case LARGEVARBINARY:
                return ArrowType.Utf8.INSTANCE;
        }
        return arrowType;
    }

    private static boolean isArrowTypeNull(ArrowType arrowType)
    {
        return arrowType == null || arrowType.equals(Types.MinorType.NULL.getType());
    }
}
