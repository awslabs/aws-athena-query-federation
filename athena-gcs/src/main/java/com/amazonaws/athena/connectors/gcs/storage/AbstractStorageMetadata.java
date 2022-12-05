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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.common.FieldValue;
import com.amazonaws.athena.connectors.gcs.common.StorageNode;
import com.amazonaws.athena.connectors.gcs.common.StorageObject;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.athena.connectors.gcs.common.TreeTraversalContext;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpression;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilder;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageMetadataConfig;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageTable;
import com.amazonaws.athena.connectors.gcs.storage.datasource.exception.UncheckedStorageDatasourceException;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.gcs.common.PartitionUtil.getPartitionFieldValue;
import static com.amazonaws.athena.connectors.gcs.common.PartitionUtil.isPartitionFolder;
import static com.amazonaws.athena.connectors.gcs.common.StorageIOUtil.containsExtension;
import static com.amazonaws.athena.connectors.gcs.common.StorageIOUtil.getFolderName;
import static com.amazonaws.athena.connectors.gcs.common.StorageTreeNodeBuilder.buildSchemaList;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.IS_TABLE_PARTITIONED;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME_LIST;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.createUri;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.getUniqueEntityName;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.getValidEntityNameFromFile;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.tableNameFromFile;
import static java.util.Objects.requireNonNull;

public abstract class AbstractStorageMetadata implements StorageMetadata
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStorageMetadata.class);

    /**
     * Extension for the metadata set via the environment variable
     * For example, PARQUET or CSV
     */
    protected final String extension;

    /**
     * Metadata config with environment variable
     */
    protected final StorageMetadataConfig metadataConfig;
    protected static Storage storage;
    private final Map<String, String> dbMap = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsInvalidExtension(String objectName)
    {
        return containsExtension(objectName)
                && !objectName.endsWith(metadataConfig.extension());
    }

    /**
     * Return a list of Field instances with field name and field type (Arrow type)
     *
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file in the specified bucket
     * @return List of field instances
     */
    @Override
    public List<Field> getTableFields(String bucketName, List<String> objectNames)
    {
<<<<<<< HEAD
<<<<<<< HEAD
        LOGGER.debug("Retrieving field schema for file(s) {}, under the bucket {}", objectNames, bucketName);
=======
        LOGGER.info("Retrieving field schema for file(s) {}, under the bucket {}", objectNames, bucketName);
>>>>>>> 3a864c14 (Rename all instances with datasource to metadata)
        requireNonNull(objectNames, "List of tables in bucket " + bucketName + " was null");
=======
>>>>>>> 3d441084 (Apply constraints on the partition folder(s)  to optimize performance)
        if (objectNames.isEmpty()) {
            throw new UncheckedStorageDatasourceException("List of tables in bucket " + bucketName + " was empty");
        }
        return getFileSchema(bucketName, objectNames.get(0)).getFields();
    }

    /**
     * Returns a list of all buckets from a cloud storage as databases
     *
     * @return List of database names
     */
    @Override
    public List<String> getAllDatabases()
    {
        TreeTraversalContext traversalContext = TreeTraversalContext.builder()
                .storage(storage)
                .build();
        Optional<StorageNode<String>> optionalRoot = buildSchemaList(traversalContext, null);
        if (optionalRoot.isPresent()) {
            dbMap.clear();
            optionalRoot.get().getChildren().forEach(node -> dbMap.put(node.getData(), node.getPath()));
            return ImmutableList.copyOf(dbMap.keySet());
        }
        return List.of();
    }

    /**
     * List all tables in a database
     *
     * @param databaseName Name of the database
     * @param nextToken    Token for the next page token, may be null
     * @param pageSize     Size of the page (number of tables per table)
     * @return List of all tables under the database
     */
    @Override
    public TableListResult getAllTables(String databaseName, String nextToken, int pageSize)
    {
        String bucket = getBucketByDatabase(databaseName);
        if (bucket == null) {
            throw new UncheckedStorageDatasourceException("No bucket found for database '" + databaseName + "'");
        }
        Storage.BlobListOption maxTableCountOption = Storage.BlobListOption.pageSize(pageSize);
        Page<Blob> blobs;
        if (nextToken != null) {
            blobs = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                    Storage.BlobListOption.pageToken(nextToken), maxTableCountOption);
        }
        else {
            blobs = storage.list(bucket, Storage.BlobListOption.currentDirectory(), maxTableCountOption);
        }
        Set<String> partitionedTables = new HashSet<>();
        Map<String, String> tableObjectMap = new HashMap<>();
        for (Blob blob : blobs.iterateAll()) {
            String storageObjectName = blob.getName();
            LOGGER.info("Loading table for object {}, under the bucket {}", storageObjectName, bucket);
            String tableName;
            if (storageObjectName.endsWith("/")) {
                LOGGER.info("Loading table for object {} is a folder", storageObjectName);
                if (isContainingDirectoryPartitioned(getStorageFiles(bucket, storageObjectName))) {
                    LOGGER.info("Loading table for object {} is a partitioned folder", storageObjectName);
                    partitionedTables.add(storageObjectName);
                    tableName = getValidEntityNameFromFile(getFolderName(storageObjectName), extension);
                }
                else {
                    LOGGER.info("Loading table for object {} is NOT a partitioned folder", storageObjectName);
                    continue;
                }
            }
            else if (!storageObjectName.toLowerCase().endsWith(metadataConfig.extension())) {
                LOGGER.info("Loading table for object {} is NOT with valid extension", storageObjectName);
                continue;
            }
            else {
                tableName = getValidEntityNameFromFile(tableNameFromFile(storageObjectName, extension), extension);
            }
            if (tableObjectMap.containsKey(tableName)) {
                tableName = getUniqueEntityName(tableName, tableObjectMap);
            }
            tableObjectMap.put(tableName, storageObjectName);
        }
        List<StorageObject> storageObjects = tableObjectMap.entrySet().stream()
                .map(entry -> StorageObject.builder()
                        .setTabletName(entry.getKey())
                        .setObjectName(entry.getValue())
                        .setPartitioned(partitionedTables.contains(entry.getValue()))
                        .build())
                .collect(Collectors.toList());
        return new TableListResult(new ArrayList<>(storageObjects), blobs.getNextPageToken());
    }

    /**
     * Returns a storage object (file) as a DB table with field names and associated file type
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @return An instance of {@link StorageTable} with column metadata
     */
    @Override
    public synchronized Optional<StorageTable> getStorageTable(String databaseName, String tableName)
    {
        LOGGER.info("Getting storage table for object {}.{}", databaseName, tableName);
        String bucketName = getBucketByDatabase(databaseName);
        if (bucketName == null) {
            throw new UncheckedStorageDatasourceException("No bucket found for database '" + databaseName + "'");
        }
        Optional<String> optionalObjectName = getTableObjectName(bucketName, tableName);
        if (optionalObjectName.isPresent()) {
            String objectName = optionalObjectName.get();
            LOGGER.info("Object name for entity {}.{} is {}", databaseName, tableName, objectName);
            if (objectName.endsWith("/")) {
                List<String> files = getStorageFiles(bucketName, objectName);
                if (isContainingDirectoryPartitioned(files)) {
                    StorageTable table = StorageTable.builder()
                            .setDatabaseName(databaseName)
                            .setTableName(tableName)
                            .partitioned(true)
                            .setParameter(TABLE_PARAM_BUCKET_NAME, bucketName)
                            .setParameter(TABLE_PARAM_OBJECT_NAME, objectName)
                            .setParameter(IS_TABLE_PARTITIONED, "true")
                            .setParameter(TABLE_PARAM_OBJECT_NAME_LIST, String.join(",", files))
                            .setFieldList(getTableFields(bucketName, files))
                            .build();
                    return Optional.of(table);
                }
            }
            else {
                StorageTable table = StorageTable.builder()
                        .setDatabaseName(databaseName)
                        .setTableName(tableName)
                        .setParameter(TABLE_PARAM_BUCKET_NAME, bucketName)
                        .setParameter(TABLE_PARAM_OBJECT_NAME, objectName)
                        .setParameter(IS_TABLE_PARTITIONED, "false")
                        .setParameter(TABLE_PARAM_OBJECT_NAME_LIST, objectName)
                        .setFieldList(getTableFields(bucketName, List.of(objectName)))
                        .build();
                return Optional.of(table);
            }
        }
        throw new UncheckedStorageDatasourceException("No object found for the table name '" + tableName + "' under bucket " + bucketName);
    }

    /**
     * Retrieves all the partitions from a given object. When the object is a:
     * <ol>
     *     <li>single file, it contains only a single partition</li>
     *     <li>director and partitioned, that is, it contains sub-folder in form of FIELD_NAME=FIELD_VALUE, it may contains one or more partitions</li>
     *     <li>director and not partitioned, this method ignores the directory</li>
     * </ol>
     * @param schema An instance of {@link Schema}
     * @param tableInfo An instance of {@link TableName} that contains schema name and table name
     * @param constraints An instance of {@link Constraints} that contains predicate information
     * @param bucketName Name of the bucket
     * @param objectName Name of the object
     * @return A list of {@link StoragePartition} instances
     */
    @Override
    public List<StoragePartition> getStoragePartitions(Schema schema, TableName tableInfo, Constraints constraints,
                                                       String bucketName, String objectName)
    {
        LOGGER.info("Getting partitions for object {} in bucket {}", objectName, bucketName);
        if (objectName.endsWith("/")) { // a folder
            List<String> files = getStorageFiles(bucketName, objectName);
            if (!files.isEmpty()) { // We fot a list of FieldValue for partition folder
                Schema fileSchema = getFileSchema(bucketName, files.get(0));
                Set<FieldValue> fieldValueList = getPartitionedFieldValue(files);
                if (!fieldValueList.isEmpty()) {
                    LOGGER.info("AbstractStorageMetadata::getStoragePartitions ->  field values for partition folder(s)\n{}", fieldValueList);
                    List<FilterExpression> expressions = new FilterExpressionBuilder(fileSchema).getExpressions(constraints, Map.of());
                    LOGGER.info("AbstractStorageMetadata::getStoragePartitions -> List of expressions:\n{}", expressions);
                    List<StoragePartition> partitions = new ArrayList<>();
                    for (String file : files) {
                        if (!file.toLowerCase().endsWith(metadataConfig.extension())) {
                            continue;
                        }
                        if (!partitionSelected(file, expressions, fieldValueList)) {
                            continue;
                        }
                        partitions.add(StoragePartition.builder()
                                .objectNames(List.of())
                                .location(file)
                                .bucketName(bucketName)
                                .recordCount(0L)
                                .children(List.of())
                                .build());
                    }
                    return partitions;
                }
            }
        }
        else {
            return List.of(StoragePartition.builder()
                    .objectNames(List.of())
                    .location(objectName)
                    .bucketName(bucketName)
                    .recordCount(0L)
                    .children(List.of())
                    .build());
        }
        return List.of();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Storage getStorage()
    {
        return storage;
    }

    /**
     * Instantiate a storage data source object with provided config
     *
     * @param config An instance of GcsDatasourceConfig that contains necessary properties for instantiating an appropriate data source
     * @throws IOException If occurs during initializing input stream with GCS credential JSON
     */
    protected AbstractStorageMetadata(StorageMetadataConfig config) throws IOException
    {
        this.metadataConfig = requireNonNull(config, "StorageDatastoreConfig is null");
        requireNonNull(config.credentialsJson(), "GCS credential JSON is null");
        requireNonNull(config.properties(), "Environment variables were null");
        this.extension = requireNonNull(config.extension(), "File extension is null");
        GoogleCredentials credentials
                = GoogleCredentials.fromStream(new ByteArrayInputStream(config.credentialsJson().getBytes(StandardCharsets.UTF_8)))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    /**
     * Retrieves a list of files that match the extension as per the metadata config
     * @param bucket Name of the bucket
     * @param prefix Prefix (aka, folder in Storage service) of the bucket from where this method with retrieve files
     * @return A list file names under the prefix
     */
    protected List<String> getStorageFiles(String bucket, String prefix)
    {
        LOGGER.info("Listing nested files for prefix {} under the bucket {}", prefix, bucket);
        List<String> fileNames = new ArrayList<>();
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.prefix(prefix));
        for (Blob blob : blobPage.iterateAll()) {
            if (blob.getName().toLowerCase(Locale.ROOT).endsWith(extension.toLowerCase(Locale.ROOT))) {
                fileNames.add(blob.getName());
            }
        }
        LOGGER.info("Files is prefix {} under the bucket {} are {}", prefix, bucket, fileNames);
        return fileNames;
    }

    // helpers

    /**
     * Retrieves a table's actual file name (object name) if it exists under the bucket.
     * Usually table name are compatible with ANSI-SQL, so the actual
     * table name vs. actual file name may differ. This method with resolve this and returns the correct table name if found under the bucket
     * @param bucketName Name of the bucket
     * @param tableName Name of the table in
     * @return Optional table. If found the get method will return the actual file name, otherwise it'll be empty
     */
    private Optional<String> getTableObjectName(String bucketName, String tableName)
    {
        requireNonNull(bucketName, "Bucket name was null");
        Map<String, String> tableObjectMap = new HashMap<>();
        Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.currentDirectory());
        for (Blob blob : blobs.iterateAll()) {
            String storageObjectName = blob.getName();
            LOGGER.info("AbstractStorageMetadata::getTableObjectName - Searching table {} with object {} under the bucket {}", tableObjectMap,
                    storageObjectName, bucketName);
            String validName;
            if (storageObjectName.endsWith("/")) {
                validName = getValidEntityNameFromFile(getFolderName(storageObjectName), extension);
            }
            else if (!storageObjectName.toLowerCase().endsWith(metadataConfig.extension())) {
                continue;
            }
            else {
                validName = getValidEntityNameFromFile(tableNameFromFile(storageObjectName, extension), extension);
            }
            if (tableObjectMap.containsKey(validName)) {
                validName = getUniqueEntityName(validName, tableObjectMap);
            }
            if (validName.equals(tableName)) {
                return Optional.of(storageObjectName);
            }
            tableObjectMap.put(validName, tableName);
        }
        return Optional.empty();
    }

    /**
     * Checks to see if the prefix contianing a list of paths is actually a partition directlry
     * @param paths A list of paths under the containing directory
     * @return True if the containing directory is partitioned, false otherwise
     */
    private boolean isContainingDirectoryPartitioned(List<String> paths)
    {
        LOGGER.info("Checking following paths to see if any is partitioned\n{}", paths);
        for (String path : paths) {
            String[] folders = path.split("/");
            for (String folder : folders) {
                if (isPartitionFolder(folder)) {
                    return true;
                }
            }
        }
        LOGGER.warn("None of the {} is a partitioned folder", paths);
        return false;
    }

    private Set<FieldValue> getPartitionedFieldValue(List<String> paths)
    {
        Set<FieldValue> fieldValues = new HashSet<>();
        LOGGER.info("Getting FieldValue from the following path if any are partitioned\n{}", paths);
        for (String path : paths) {
            String[] folders = path.split("/");
            for (String folder : folders) {
                if (isPartitionFolder(folder)) {
                    Optional<FieldValue> optionalFieldValue = getPartitionFieldValue(folder);
                    optionalFieldValue.ifPresent(fieldValues::add);
                }
            }
        }
        LOGGER.info("Field values for partitioned folder(s) {}", fieldValues);
        return fieldValues;
    }

    private String getBucketByDatabase(String databaseName)
    {
        if (dbMap.containsKey(databaseName)) {
            return dbMap.get(databaseName);
        }
        TreeTraversalContext traversalContext = TreeTraversalContext.builder()
                .storage(storage)
                .build();
        Optional<StorageNode<String>> optionalRoot = buildSchemaList(traversalContext, databaseName);
        if (optionalRoot.isPresent()) {
            Optional<StorageNode<String>> optionalSchema = optionalRoot.get().findChildByData(databaseName);
            if (optionalSchema.isPresent()) {
                LOGGER.info("AbstractStorageMetadata::getBucketByDatabase node for database {} is {}", databaseName, optionalSchema.get());
                return optionalSchema.get().getPath();
            }
        }
        return null;
    }

    private boolean partitionSelected(String file, List<FilterExpression> expressions, Set<FieldValue> fieldValueList)
    {
        if (expressions.isEmpty() || fieldValueList.isEmpty()) {
            LOGGER.info("All partition folder selected for for file {}", file);
            return true;
        }

        for (FieldValue fieldValue : fieldValueList) {
            if (file.contains(fieldValue.getOriginalValue())) {
                for (FilterExpression expression : expressions) {
                    if (expression.apply(fieldValue.getValue())) {
                        LOGGER.info("Partition folder {} for file {} is selected", fieldValue.getOriginalValue(), file);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public Schema getFileSchema(String bucketName, String fileName)
    {
        requireNonNull(bucketName, "bucketName was null");
        requireNonNull(fileName, "fileName was null");
        LOGGER.info("Retrieving field schema from file {}, under the bucket {}", fileName, bucketName);
        String uri = createUri(bucketName, fileName);
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory factory = new FileSystemDatasetFactory(allocator,
                NativeMemoryPool.getDefault(), getFileFormat(), uri);
        // inspect schema
        return factory.inspect();
    }
}
