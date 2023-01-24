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

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.GcsUtil;
import com.amazonaws.athena.connectors.gcs.common.PartitionUtil;
import com.amazonaws.athena.connectors.gcs.filter.AbstractExpression;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilder;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Table;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.createUri;
import static com.amazonaws.athena.connectors.gcs.GcsUtil.isFieldTypeNull;
import static com.google.cloud.storage.Storage.BlobListOption.prefix;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

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
        Optional<String> file = getStorageFiles(bucketName, prefix);
        if (file.isPresent()) {
            return getFileSchema(bucketName, file.get(), FileFormat.valueOf(format.toUpperCase()), allocator).getFields();
        }

        throw new IllegalArgumentException("No object found for the table name '" + prefix + "' under bucket " + bucketName);
    }

    /**
     * Retrieves a list of StorageSplit that essentially contain the list of all files for a given table type in a storage location
     *
     * @param locationUri location uri
     * @return A list of files
     */
    public List<String> getStorageSplits(URI locationUri)
    {
        List<String> fileList = new ArrayList<>();
        String bucketName = locationUri.getAuthority();
        String path = locationUri.getPath().startsWith("/") ? locationUri.getPath().substring(1) : locationUri.getPath();
        Page<Blob> blobs = storage.list(bucketName, prefix(path));
        for (Blob blob : blobs.iterateAll()) {
            if (isBlobFile(blob)) {
                fileList.add(bucketName + "/" + blob.getName());
            }
        }
        return fileList;
    }

    /**
     * Retrieves a list of partition folders from the GCS bucket based on partition.pattern Table parameter and partition keys set forth in Glue table. If the summary from the
     * constraints is empty (no where clauses or unsupported clauses), it will essentially return all the partition folders from the GCS bucket. If there is any constraints to
     * apply, it will apply constraints to filter selected partition folder, to narrow down the data load
     *
     * @param schema An instance of {@link Schema} that describes underlying Table's schema
     * @param tableInfo Name of the table
     * @param constraints An instance of {@link Constraints}, captured from where clauses
     * @param awsGlue An instance of {@link AWSGlue}
     * @return A list of {@link Map<String, String>} instances
     * @throws URISyntaxException Throws if any occurs during parsing Uri
     */
    public List<Map<String, String>> getPartitionFolders(Schema schema, TableName tableInfo, Constraints constraints, AWSGlue awsGlue)
            throws URISyntaxException
    {
        LOGGER.info("Getting partition folder(s) for table {}.{}", tableInfo.getSchemaName(), tableInfo.getTableName());
        Table table = GcsUtil.getGlueTable(tableInfo, awsGlue);
        // TODO: I'm not sure if this filtering is even necessary.
        var partitionFieldNames = table.getPartitionKeys().stream().map(Column::getName).collect(Collectors.toSet());
        List<Field> partitionFields = schema.getFields().stream()
            .filter(field -> partitionFieldNames.contains(field.getName()))
            .collect(Collectors.toList());
        // Build expression only based on partition keys
        List<AbstractExpression> expressions = new FilterExpressionBuilder(partitionFields).getExpressions(constraints);
        LOGGER.info("Expressions for the request of {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), expressions);
        URI storageLocation = new URI(table.getStorageDescriptor().getLocation());
        // Generate a map from column name to the expressions that need to be evaluated for that column
        Map<String, List<AbstractExpression>> expressionMap = expressions.stream()
                .collect(Collectors.groupingBy(AbstractExpression::getColumnName,
                        () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER), toCollection(ArrayList::new)));
        LOGGER.info("Listing object in location {} under the bucket {}", storageLocation.getAuthority(), storageLocation.getPath());
        String path = storageLocation.getPath().substring(1);
        Page<Blob> blobPage = storage.list(storageLocation.getAuthority(), prefix(path));

        Map<Boolean, List<Map<String, String>>> results = StreamSupport.stream(blobPage.iterateAll().spliterator(), false)
                .filter(blob -> !isBlobFile(blob))
                .map(blob -> blob.getName().replaceFirst("^" + path, ""))
                 // remove the front-slash, because, the expression generated without it
                .map(folderPath -> folderPath.startsWith("/") ? folderPath.substring(1) : folderPath)
                .map(folderPath -> PartitionUtil.getPartitionColumnData(table, folderPath))
                .collect(Collectors.partitioningBy(partitionsMap -> checkPartitionWithConstrains(partitionsMap, expressionMap)));

        LOGGER.info("getPartitionFolders results: {}", results);

        return results.get(true);
    }

    /**
     * Retrieves a first of file has non-zero size
     *
     * @param bucket Name of the bucket
     * @param prefix Prefix (aka, folder in Storage service) of the bucket from where this method with retrieve files
     * @return A single file name under the prefix
     */
    protected Optional<String> getStorageFiles(String bucket, String prefix)
    {
        LOGGER.info("Listing nested files for prefix {} under the bucket {}", prefix, bucket);
        Page<Blob> blobPage = storage.list(bucket, prefix(prefix.substring(1)));
        for (Blob blob : blobPage.iterateAll()) {
            if (isBlobFile(blob)) {
               return Optional.of(blob.getName());
            }
        }
        return Optional.empty();
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
        DatasetFactory factory = new FileSystemDatasetFactory(allocator,
                NativeMemoryPool.getDefault(), format, uri);
        // inspect schema
        return factory.inspect();
    }

    private boolean checkPartitionWithConstrains(Map<String, String> partitionMap, Map<String, List<AbstractExpression>> expressionMap)
    {
        if (expressionMap.isEmpty()) {
            return true;
        }
        boolean expressionFailureExists = partitionMap.entrySet().stream().anyMatch(partitionEntry ->
            expressionMap.getOrDefault(partitionEntry.getKey(), List.of()).stream()
                .anyMatch(expr -> {
                    boolean result = expr.apply(partitionEntry.getValue());
                    LOGGER.debug("Partition entry: {}, expression: {}, result: {}", partitionEntry, expr, result);
                    return !result;
                })
        );
        return !expressionFailureExists;
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
        String locationUri = table.getStorageDescriptor().getLocation();
        URI storageLocation = new URI(locationUri);
        List<Field> fieldList = getFields(storageLocation.getAuthority(), storageLocation.getPath(), table.getParameters().get(CLASSIFICATION_GLUE_TABLE_PARAM), allocator);

        LOGGER.debug("Schema Fields\n{}", fieldList);
        for (Field field : fieldList) {
            if (isFieldTypeNull(field)) {
                field = Field.nullable(field.getName().toLowerCase(), Types.MinorType.VARCHAR.getType());
            }
            else {
                field = new Field(field.getName().toLowerCase(), new FieldType(field.isNullable(), field.getType(), field.getDictionary(), field.getMetadata()), field.getChildren());
            }
            schemaBuilder.addField(getCompatibleField(field));
        }
        return schemaBuilder.build();
    }

    private Field getCompatibleField(Field field)
    {
        String fieldName = field.getName().toLowerCase();
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        switch (fieldType) {
            case TIMESTAMPNANO:
            case TIMESTAMPSEC:
            case TIMESTAMPMILLI:
            case TIMEMICRO:
            case TIMESTAMPMICRO:
            case TIMENANO:
                    return new Field(fieldName,
                            new FieldType(field.isNullable(), Types.MinorType.DATEMILLI.getType(), field.getDictionary(),
                                    field.getMetadata()), field.getChildren());
            case TIMESTAMPMICROTZ:
                return new Field(fieldName,
                        new FieldType(field.isNullable(), Types.MinorType.TIMESTAMPMILLITZ.getType(), field.getDictionary(),
                                field.getMetadata()), field.getChildren());
            case FIXEDSIZEBINARY:
            case LARGEVARBINARY:
                    return new Field(fieldName,
                            new FieldType(field.isNullable(), Types.MinorType.VARCHAR.getType(), field.getDictionary(),
                                    field.getMetadata()), field.getChildren());
            default:
                return field;
        }
    }
}
