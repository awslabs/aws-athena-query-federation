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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connectors.gcs.UncheckedGcsConnectorException;
import com.amazonaws.athena.connectors.gcs.common.FieldValue;
import com.amazonaws.athena.connectors.gcs.common.PartitionFolder;
import com.amazonaws.athena.connectors.gcs.common.PartitionLocation;
import com.amazonaws.athena.connectors.gcs.common.PartitionUtil;
import com.amazonaws.athena.connectors.gcs.common.StorageLocation;
import com.amazonaws.athena.connectors.gcs.common.StorageNode;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.athena.connectors.gcs.common.TreeTraversalContext;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpression;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilder;
import com.amazonaws.athena.connectors.gcs.glue.GlueUtil;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageMetadataConfig;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageTable;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Table;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.amazonaws.athena.connectors.gcs.common.PartitionUtil.isPartitionFolder;
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
import static com.google.cloud.storage.Storage.BlobListOption.prefix;
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
            throw new UncheckedGcsConnectorException("List of tables in bucket " + bucketName + " was empty");
        }
        return getFileSchema(bucketName, objectNames.get(0)).getFields();
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
            throw new UncheckedGcsConnectorException("No bucket found for database '" + databaseName + "'");
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
        throw new UncheckedGcsConnectorException("No object found for the table name '" + tableName + "' under bucket " + bucketName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StorageSplit> getStorageSplits(String tableType, PartitionLocation partition)
    {
        String extension = "." + tableType.toLowerCase();
        List<StorageSplit> splits = new ArrayList<>();
        String bucketName = partition.getBucketName();
        Page<Blob> blobs = storage.list(bucketName, prefix(partition.getLocation()));
        for (Blob blob : blobs.iterateAll()) {
            if (blob.getName().toLowerCase().endsWith(extension)) {
                splits.add(StorageSplit.builder().fileName(bucketName + "/" + blob.getName()).build());
            }
        }
        return splits;
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

    @Override
    public List<PartitionFolder> getPartitionFolders(MetadataRequest request, Schema schema, TableName tableInfo, Constraints constraints, AWSGlue awsGlue)
            throws ParseException
    {
<<<<<<< HEAD
        LOGGER.info("Getting partition folder(s) for table {}.{}", tableInfo.getSchemaName(), tableInfo.getTableName());
        List<PartitionFolder> partitionFolders = new ArrayList<>();
        Table table = GlueUtil.getGlueTable(request, tableInfo, awsGlue);
        if (table != null) {
            Optional<String> optionalFolderRegEx = PartitionUtil.getRegExExpression(table);
            if (optionalFolderRegEx.isPresent()) {
                String locationUri = table.getStorageDescriptor().getLocation();
                LOGGER.info("Location URI for table {}.{} is {}", tableInfo.getSchemaName(), tableInfo.getTableName(), locationUri);
                StorageLocation storageLocation = StorageLocation.fromUri(locationUri);
                LOGGER.info("Listing object in location {} under the bucket {}", storageLocation.getLocation(), storageLocation.getBucketName());
                Page<Blob> blobPage = storage.list(storageLocation.getBucketName(), prefix(storageLocation.getLocation()));
                String folderRegEx = optionalFolderRegEx.get();
                Pattern folderRegExPattern = Pattern.compile(folderRegEx);
                List<FilterExpression> expressions = new FilterExpressionBuilder(schema).getExpressions(constraints);
                LOGGER.info("Expressions for the request of {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), expressions);
                for (Blob blob : blobPage.iterateAll()) {
                    String blobName = blob.getName();
                    String folderPath = blobName.startsWith(storageLocation.getLocation())
                            ? blobName.replace(storageLocation.getLocation(), "")
                            : blobName;
                    LOGGER.info("Examining folder {} against regex {}", folderPath, folderRegEx);
                    if (folderRegExPattern.matcher(folderPath).matches()) {
                        LOGGER.info("Examining folder {} against regex {} matches", folderPath, folderRegEx);
                        if (!canIncludePath(folderPath, expressions)) {
                            LOGGER.info("Folder " + folderPath + " has NOT been selected against the expression");
                            continue;
                        }
                        LOGGER.info("Folder " + folderPath + " has been selected against the expression");
                        List<StoragePartition> partitions = PartitionUtil.getStoragePartitions(folderPath, folderRegEx,
                                table.getPartitionKeys(), table.getParameters());
                        if (!partitions.isEmpty()) {
                            partitionFolders.add(new PartitionFolder(folderPath, partitions));
                        }
                        else {
                            LOGGER.info("No partitions found for the folder {}", blob.getName());
                        }
                    }
                }
            }
        }
        else {
            LOGGER.info("Table {}.{} not found", tableInfo.getSchemaName(), tableInfo.getTableName());
        }
        return partitionFolders;
=======
        return List.of(new PartitionFolder("gs://mydatalake4/full_data/", List.of(new StoragePartition("ss_sold_date_sk", "integer", 2451051L))));
>>>>>>> 8913f0f9 (GcsMetadataHandler changes for doGetSplits)
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
        Page<Blob> blobPage = storage.list(bucket, prefix(prefix));
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
     * Checks to see if the prefix containing a list of paths is actually a partition directory
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

    private boolean canIncludePath(String folderPath, List<FilterExpression> expressions)
    {
        if (expressions.isEmpty()) {
            return true;
        }
        String[] folderPaths = folderPath.split("/");
        for (String path : folderPaths) {
            Optional<FieldValue> optionalFieldValue = FieldValue.from(path);
            if (optionalFieldValue.isEmpty()) {
                continue;
            }
            FieldValue fieldValue = optionalFieldValue.get();
            Optional<FilterExpression> optionalExpression = expressions.stream()
                    .filter(expr -> expr.columnName().equalsIgnoreCase(fieldValue.getField()))
                    .findFirst();
            if (optionalExpression.isPresent()) {
                LOGGER.info("Evaluating field value {} against the expression {}", fieldValue, expressions);
                FilterExpression expression = optionalExpression.get();
                if (!expression.apply(fieldValue.getValue())) {
                    return false;
                }
            }
            else {
                LOGGER.info("No expression found for field {}", fieldValue.getField());
            }
        }
        return true;
    }
}
