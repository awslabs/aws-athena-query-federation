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
import com.amazonaws.athena.connectors.gcs.GcsSchemaUtils;
import com.amazonaws.athena.connectors.gcs.common.PagedObject;
import com.amazonaws.athena.connectors.gcs.common.StorageNode;
import com.amazonaws.athena.connectors.gcs.common.StorageObject;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.athena.connectors.gcs.common.StorageTreeNodeBuilder;
import com.amazonaws.athena.connectors.gcs.common.TreeTraversalContext;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpression;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilder;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageTable;
import com.amazonaws.athena.connectors.gcs.storage.datasource.exception.DatabaseNotFoundException;
import com.amazonaws.athena.connectors.gcs.storage.datasource.exception.UncheckedStorageDatasourceException;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
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
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.amazonaws.athena.connectors.gcs.common.PartitionUtil.isPartitionFolder;
import static com.amazonaws.athena.connectors.gcs.common.StorageIOUtil.containsExtension;
import static com.amazonaws.athena.connectors.gcs.common.StorageIOUtil.getFolderName;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.IS_TABLE_PARTITIONED;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.connectors.gcs.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME_LIST;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.createUri;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.getValidEntityName;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.getValidEntityNameFromFile;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.tableNameFromFile;
import static java.util.Objects.requireNonNull;

public abstract class AbstractStorageDatasource implements StorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStorageDatasource.class);

    protected final String extension;
    protected final StorageDatasourceConfig datasourceConfig;
    protected final Map<String, String> databaseBuckets = new HashMap<>();
    protected final Map<String, Map<StorageObject, List<String>>> tableObjects = new HashMap<>();
    protected boolean storeCheckingComplete = false;
    protected List<LoadedEntities> loadedEntitiesList = new ArrayList<>();
    protected static Storage storage;

    /**
     * Instantiate a storage data source object with provided config
     *
     * @param config An instance of GcsDatasourceConfig that contains necessary properties for instantiating an appropriate data source
     * @throws IOException If occurs during initializing input stream with GCS credential JSON
     */
    protected AbstractStorageDatasource(StorageDatasourceConfig config) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        this.datasourceConfig = requireNonNull(config, "StorageDatastoreConfig is null");
        requireNonNull(config.credentialsJson(), "GCS credential JSON is null");
        requireNonNull(config.properties(), "Environment variables were null");
        this.extension = requireNonNull(config.extension(), "File extension is null");
        GoogleCredentials credentials
                = GoogleCredentials.fromStream(new ByteArrayInputStream(config.credentialsJson().getBytes(StandardCharsets.UTF_8)))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsInvalidExtension(String objectName)
    {
        return containsExtension(objectName)
                && !objectName.endsWith(datasourceConfig.extension());
    }

    /**
     * Return a list of Field instances with field name and field type (Arrow type)
     *
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file in the specified bucket
     * @return List of field instances
     */
    @Override
    public List<Field> getTableFields(String bucketName, List<String> objectNames) throws IOException
    {
        System.out.printf("Retrieving field schema for file(s) %s, under the bucket %s%n", objectNames, bucketName);
        LOGGER.info("Retrieving field schema for file(s) {}, under the bucket {}", objectNames, bucketName);
        requireNonNull(objectNames, "List of tables in bucket " + bucketName + " was null");
        if (objectNames.isEmpty()) {
            throw new UncheckedStorageDatasourceException("List of tables in bucket " + bucketName + " was empty");
        }
        System.out.printf("Inferring field schema based on file %s%n", objectNames.get(0));
        LOGGER.debug("Inferring field schema based on file {}", objectNames.get(0));
        String uri = createUri(bucketName, objectNames.get(0));
        System.out.printf("Retrieving fields for URI %s%n", uri);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        DatasetFactory factory = new FileSystemDatasetFactory(allocator,
                NativeMemoryPool.getDefault(), getFileFormat(), uri);
        // inspect schema
        return factory.inspect().getFields();
    }
    /**
     * Returns a list of all buckets from a cloud storage as databases
     *
     * @return List of database names
     */
    @Override
    public List<String> getAllDatabases()
    {
        Page<Bucket> buckets = storage.list();
        for (Bucket bucket : buckets.iterateAll()) {
            String bucketName = bucket.getName();
            String validName = getValidEntityName(bucketName);
            if (!databaseBuckets.containsKey(validName)) {
                loadedEntitiesList.add(new LoadedEntities(validName));
                databaseBuckets.put(validName, bucketName);
            }
        }
        return ImmutableList.copyOf(databaseBuckets.keySet());
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
    public synchronized TableListResult getAllTables(String databaseName, String nextToken, int pageSize) throws Exception
    {
        String currentNextToken = null;
        System.out.println("checking complete? " + storeCheckingComplete + ", tables are loaded for database? " + databaseName + ": " + tablesLoadedForDatabase(databaseName));
        if (!storeCheckingComplete
                || !tablesLoadedForDatabase(databaseName)) {
            currentNextToken = this.loadTablesWithContinuationToken(databaseName, nextToken, pageSize);
        }
        LOGGER.debug("tableObjects:\n{}", tableObjects);
        List<StorageObject> tables = List.copyOf(tableObjects.getOrDefault(databaseName, Map.of()).keySet());
        return new TableListResult(tables, currentNextToken);
    }

    /**
     * Loads Tables with continuation token from a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables. It loads tables from the underlying storage provider with a
     * token and page size (e.g., 10) until all tables (files) are loaded.
     *
     * @param databaseName For which datastore will be checked
     * @param nextToken    Next token for retrieve next page of table list, may be null
     * @param pageSize     Size of the page in each load with token
     */

    public synchronized String loadTablesWithContinuationToken(String databaseName, String nextToken, int pageSize) throws Exception
    {
        if (!checkBucketExists(databaseName)) {
            System.out.println("Not bucket exists for database " + databaseName);
            return null;
        }
        String currentNextToken = loadTablesInternal(databaseName, nextToken, pageSize);
        storeCheckingComplete = currentNextToken == null;
        return currentNextToken;
    }

    /**
     * List all tables in a database
     *
     * @param databaseName Name of the database
     * @return List of all tables under the database
     */
    @Override
    public List<StorageObject> loadAllTables(String databaseName) throws Exception
    {
        checkDatastoreForDatabase(databaseName);
        return List.copyOf(tableObjects.getOrDefault(databaseName, Map.of()).keySet());
    }

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param database For which datastore will be checked
     */
    @Override
    public void checkDatastoreForDatabase(String database) throws Exception
    {
        if (checkBucketExists(database)) {
            loadTablesInternal(database);
            storeCheckingComplete = true;
        }
    }

    /**
     * Returns a storage object (file) as a DB table with field names and associated file type
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @return An instance of {@link StorageTable} with column metadata
     */
    @Override
    public synchronized Optional<StorageTable> getStorageTable(String databaseName, String tableName) throws Exception
    {
        if (!storeCheckingComplete) {
            this.checkDatastoreForDatabase(databaseName);
        }
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new RuntimeException("StorageHiveDatastore.getTable: bucket null does not exist");
        }
        System.out.printf("Resolving Table %s under the schema %s%n", tableObjects, databaseName);
        LOGGER.debug("Resolving Table {} under the schema {}", tableObjects, databaseName);
        Map<StorageObject, List<String>> objectNameMap = tableObjects.get(databaseName);
        if (objectNameMap != null && !objectNameMap.isEmpty()) {
            System.out.printf("Searching storage key for table %s in %s database%n", tableName, databaseName);
            Optional<StorageObject> optionalStorageObjectKey = findStorageObjectKey(tableName, databaseName);
            if (optionalStorageObjectKey.isPresent()) {
                System.out.printf("Searching storage key for table %s in %s database found %s%n", tableName, databaseName, optionalStorageObjectKey.get());
                StorageObject key = optionalStorageObjectKey.get();
                List<String> objectNames = objectNameMap.get(key);
                System.out.printf("Object names for for key %s are %s%n", key, objectNames);
                if (objectNames != null) {
                    System.out.printf("Getting storage table for object %s in %s bucket%n", key.getObjectName(), bucketName);
                    StorageTable table = StorageTable.builder()
                            .setDatabaseName(databaseName)
                            .setTableName(tableName)
                            .partitioned(key.isPartitioned())
                            .setParameter(TABLE_PARAM_BUCKET_NAME, bucketName)
                            .setParameter(TABLE_PARAM_OBJECT_NAME, key.getObjectName())
                            .setParameter(IS_TABLE_PARTITIONED, key.isPartitioned() ? "true" : "false")
                            .setParameter(TABLE_PARAM_OBJECT_NAME_LIST, String.join(",", objectNames))
                            .setFieldList(getTableFields(bucketName, objectNames))
                            .build();
                    return Optional.of(table);
                }
            }
            LOGGER.info("No file(s) found for table {} in the schema {}", tableName, databaseName);
        }
        return Optional.empty();
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
     * @return An list of {@link StoragePartition} instances
     * @throws IOException Occurs if any during walk-through the buckets/files within the underlying storage provider
     */
    @Override
    public List<StoragePartition> getStoragePartitions(Schema schema, TableName tableInfo, Constraints constraints,
                                                       String bucketName, String objectName) throws Exception
    {
        LOGGER.info("Retrieving partitions for object {}, under bucket {}", objectName, bucketName);
        requireNonNull(bucketName, "Bucket name was null");
        requireNonNull(objectName, "objectName name was null");
        List<StoragePartition> storagePartitions = new ArrayList<>();
        if (isDirectory(bucketName, objectName)) {
//            if (true) {
//                // TODO: as of now no partition support
//                return List.of();
//            }
            if (isPartitionedDirectory(bucketName, objectName)) {
                TreeTraversalContext context = TreeTraversalContext.builder()
                        .hasParent(true)
                        .includeFile(false)
                        .maxDepth(0) // unlimited
                        .partitionDepth(1)
                        .storage(storage)
                        .build();
                List<String> fileNames = getLeafObjectsByPartitionPrefix(bucketName, objectName, 1);
                if (fileNames.isEmpty()) {
                    throw new UncheckedStorageDatasourceException("No files found to retrieve schema for partitioned table "
                            + tableInfo.getTableName() + " under schema " + tableInfo.getSchemaName());
                }
//                List<FilterExpression> expressions = getExpressions(bucketName, fileNames.get(0), schema, tableInfo, constraints, Map.of());
                List<FilterExpression> expressions;
                Optional<Schema> optionalSchema = GcsSchemaUtils.getSchemaFromGcsPrefix(fileNames.get(0), getFileFormat(), this.datasourceConfig);
                if (optionalSchema.isPresent()) {
                    expressions = new FilterExpressionBuilder(optionalSchema.get())
                            .getExpressions(constraints, Map.of());
                }
                else {
                    throw new UncheckedStorageDatasourceException("Table schema couldn't be retrieved for table " + tableInfo.getSchemaName()
                            + " under the database " + tableInfo.getSchemaName() + ". Please check whether the file is corrupted or having other issues.");
                }
                LOGGER.debug("AbstractStorageDatasource.getStoragePartitions() -> List of expressions:\n{}", expressions);
                context.addAllFilers(expressions);
                Optional<StorageNode<String>> optionalRoot = StorageTreeNodeBuilder.buildTreeWithPartitionedDirectories(bucketName,
                        objectName, objectName, context);
                List<StoragePartition> partitions = new ArrayList<>();
                if (optionalRoot.isPresent()) {
                    for (StorageNode<String> partitionedFolderNode : optionalRoot.get().getChildren()) {
                        partitions.add(StoragePartition.builder()
                                .objectNames(List.of())
                                .location(partitionedFolderNode.getPath() + "/")
                                .bucketName(bucketName)
                                .recordCount(0L)
                                .children(List.of())
                                .build());
                    }
                    LOGGER.debug("Storage partitions using tree: \n{}", partitions);
                }
                else {
                    LOGGER.debug("the object {} in the bucket {} is partitioned. However, it doesn't contain any nested objects", objectName, bucketName);
                }
                return partitions;
            }
            else {
                LOGGER.debug("Folder {} in bucket {} is not a partitioned folder", objectName, bucketName);
            }
        }
        else {
            LOGGER.debug("Folder {} under buket {} is NOT partitioned", objectName, bucketName);
            // A file (aka Table) under a non-partitioned bucket/folder
            StoragePartition partition = StoragePartition.builder()
                    .objectNames(List.of(objectName))
                    .location(objectName)
                    .bucketName(bucketName)
                    .build();
            return List.of(partition);
        }
        return storagePartitions;
    }

    @Override
    public Storage getStorage()
    {
        return storage;
    }

    public StorageDatasourceConfig getConfig()
    {
        return datasourceConfig;
    }

//    @Override
//    public StorageProvider getStorageProvider()
//    {
//        return this.storageProvider;
//    }

    /**
     * Returns the size of records per split
     *
     * @return Size of records per split
     */
    public abstract int recordsPerSplit();

    /**
     * Loads all tables for the given database ana maintain a references of actual bucket and file names
     *
     * @param databaseName Name of the database
     * @param nextToken    Next token to meta-data pagination
     * @param pageSize     Number of table per page when tokenized for pagination
     * @return Next token to retrieve next page
     */
    protected String loadTablesInternal(String databaseName, String nextToken, int pageSize) throws Exception
    {
        System.out.println("Loading tables internal with token " + nextToken);
        requireNonNull(databaseName, "Database name was null");
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("Bucket null does not exist for Database " + databaseName);
        }
        PagedObject pagedObject = getObjectNames(bucketName, nextToken, pageSize);
        // in the following Map, the key is the Table name, and the value is a list of String contains one or more objects (file)
        // when the file_pattern environment variable is set, usually list String may contain more than one object
        // And in case a table consists of multiple objects, during read it read data from all the objects
        Map<StorageObject, List<String>> objectNameMap = convertBlobsToTableObjectsMap(bucketName, pagedObject.getFileNames());
        tableObjects.put(databaseName, objectNameMap);
        String currentNextToken = pagedObject.getNextToken();
        if (currentNextToken == null) {
            markEntitiesLoaded(databaseName);
        }
        return currentNextToken;
    }

    private PagedObject getObjectNames(String bucket, String continuationToken, int pageSize)
    {
        System.out.println("Getting object names from bucket " + bucket);
        Storage.BlobListOption maxTableCountOption = Storage.BlobListOption.pageSize(pageSize);
        Page<Blob> blobs;
        if (continuationToken != null) {
            blobs = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                    Storage.BlobListOption.pageToken(continuationToken), maxTableCountOption);
        }
        else {
            blobs = storage.list(bucket, Storage.BlobListOption.currentDirectory(), maxTableCountOption);
        }
        return PagedObject.builder()
                .fileNames(toImmutableObjectNameList(blobs))
                .nextToken(blobs.getNextPageToken())
                .build();
    }

    private List<String> toImmutableObjectNameList(Page<Blob> blobs)
    {
        List<String> blobNameList = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (blob != null) {
                blobNameList.add(blob.getName());
            }
        }
        System.out.println("blobNameList\n" + blobNameList);
        LOGGER.debug("blobNameList\n{}", blobNameList);
        return ImmutableList.copyOf(blobNameList);
    }

    /**
     * Loads all tables for the given database without pagination
     *
     * @param databaseName Name of the database
     */
    protected void loadTablesInternal(String databaseName) throws Exception
    {
        requireNonNull(databaseName, "Database name was null");
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("Bucket null does not exist for Database " + databaseName);
        }
        List<String> fileNames = toImmutableObjectNameList(storage.list(bucketName, Storage.BlobListOption.currentDirectory()));
        Map<StorageObject, List<String>> objectNameMap = convertBlobsToTableObjectsMap(bucketName, fileNames);
        tableObjects.put(databaseName, objectNameMap);
        markEntitiesLoaded(databaseName);
    }

    protected Map<StorageObject, List<String>> convertBlobsToTableObjectsMap(String bucketName, List<String> objectNames) throws Exception
    {
        System.out.println("Converting blobs to object name map");
        Map<StorageObject, List<String>> objectNameMap = new HashMap<>();
        for (String objectName : objectNames) {
            if (checkTableIsValid(bucketName, objectName)) {
                addTable(bucketName, objectName, objectNameMap);
            }
        }
        return objectNameMap;
    }

    /**
     * Adds a table with one or more files (when it's partitioned)
     *
     * @param objectName Name of the object in storage (file)
     * @param tableMap   Name of the table under which we'll maintain a list of files fall under the pattern
     */
    protected void addTable(String bucketName, String objectName,
                            Map<StorageObject, List<String>> tableMap) throws Exception
    {
        if (containsInvalidExtension(objectName)) {
            LOGGER.debug("The object {} contains invalid extension. Expected extension {}", objectName, extension);
            return;
        }
        boolean isPartitionedTable = isPartitionedDirectory(bucketName, objectName);
        LOGGER.debug("Adding table for object {}, under bucket {}, and is partitioned? {}", objectName, bucketName, isPartitionedTable);
        String strLowerObjectName = objectName.toLowerCase(Locale.ROOT);
        String tableName = objectName;
        if (isPartitionedTable) {
            tableName = getFolderName(tableName);
        }
        else {
            tableName = tableNameFromFile(tableName, extension);
        }
        LOGGER.debug("Table for the object {} under the bucket {} is {}", objectName, bucketName, tableName);
        if (!isPartitionedTable
                // is a directory
                && (objectName.endsWith("/") || isDirectory(bucketName, objectName))) {
            // we're currently not supporting un-partitioned folder to traverse for tables
            return;
        }

        if (strLowerObjectName.endsWith(extension.toLowerCase(Locale.ROOT))
                || !isPartitionedTable
                || isSupported(bucketName, objectName)) {
            StorageObject storageObject = StorageObject.builder()
                    .setTabletName(getValidEntityNameFromFile(tableName, this.extension))
                    .setObjectName(objectName)
                    .setPartitioned(isPartitionedTable)
                    .build();
            tableMap.computeIfAbsent(storageObject,
                    files -> new ArrayList<>()).add(objectName);
        }
        else if (isPartitionedDirectory(bucketName, objectName)) {
            List<String> fileNames = getLeafObjectsByPartitionPrefix(bucketName, objectName, 1);
            if (fileNames.isEmpty()) {
                LOGGER.debug("No files found under partitioned table {}", tableName);
            }
            else {
                LOGGER.debug("Following files are found found under partitioned table {}\n{}", tableName, fileNames);
            }
            StorageObject storageObject = StorageObject.builder()
                    .setTabletName(getValidEntityNameFromFile(tableName, this.extension))
                    .setObjectName(objectName)
                    .setPartitioned(isPartitionedTable)
                    .build();
            tableMap.computeIfAbsent(storageObject,
                    files -> new ArrayList<>()).addAll(fileNames);
        }
        LOGGER.debug("After adding table tableMap\n{}", tableMap);
    }

    private boolean isPartitionedDirectory(String bucket, String location)
    {
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.currentDirectory(), Storage.BlobListOption.prefix(location));
        for (Blob blob : blobPage.iterateAll()) {
            if (location.equals(blob.getName())) { // same blob as the location
                continue;
            }
            if (isPartitionFolder(blob.getName())) {
                LOGGER.debug("Path {} is a partitioned directory", location);
                return true;
            }
        }
        LOGGER.debug("Path {} is NOT a partitioned directory", location);
        return false;
    }

    public static boolean isDirectory(String bucket, String prefix)
    {
        BlobId blobId = BlobId.of(bucket, prefix);
        Blob blob = storage.get(blobId);
        if (blob == null && !prefix.endsWith("/")) { // maybe a folder without ending with a '/' character
            blob = storage.get(BlobId.of(bucket, prefix + "/"));
        }
        LOGGER.debug("Blob for prefix {} under the bucket {} is: {} with size: {}", prefix, bucket, blob, blob == null ? -1 : blob.getSize());
        return  (blob != null && blob.getSize() == 0);
    }

    public static List<String> getLeafObjectsByPartitionPrefix(String bucket, String partitionPrefix, int maxCount)
    {
        LOGGER.debug("Iterating recursively through a folder under the bucket to list all file object");
        List<String> leaves = new ArrayList<>();
        getLeafObjectsRecurse(bucket, partitionPrefix, leaves, maxCount);
        return leaves;
    }

    public static void getLeafObjectsRecurse(String bucket, String prefix, List<String> leafObjects, int maxCount)
    {
        if (maxCount > 0 && leafObjects.size() >= maxCount) {
            return;
        }
        LOGGER.debug("Walking through {} under bucket '{}'", prefix, bucket);
        if (!prefix.endsWith("/")) {
            prefix += '/';
        }
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(prefix));
        for (Blob blob : blobPage.iterateAll()) {
            if (blob.getName().equals(prefix)) {
                continue;
            }
            if (blob.getSize() > 0) { // it's a file
                leafObjects.add(blob.getName());
            }
            else {
                getLeafObjectsRecurse(bucket, blob.getName(), leafObjects, maxCount);
            }
        }
    }

    /**
     * Determines whether tables are loaded for a given database
     *
     * @param database Name of the database
     * @return True if tables are loaded for the database, false otherwise
     */
    protected boolean tablesLoadedForDatabase(String database)
    {
        Optional<LoadedEntities> optionalLoadedEntities = loadedEntitiesList.stream()
                .filter(entity -> entity.databaseName.equalsIgnoreCase(database))
                .findFirst();
        if (optionalLoadedEntities.isEmpty()) {
            return false;
//            throw new DatabaseNotFoundException("Schema " + database + " not found");
        }
        LoadedEntities entities = optionalLoadedEntities.get();
        return entities.tablesLoaded;
    }

    /**
     * Inner class for keep track whether all tables are loaded for a given table
     */
    protected static class LoadedEntities
    {
        private final String databaseName;
        private boolean tablesLoaded;

        public LoadedEntities(String databaseName)
        {
            this.databaseName = databaseName;
        }
    }

    // helpers
    /**
     * Determines whether a bucket exists for the given database name
     *
     * @param databaseName Name of the database
     * @return True if a bucket exists for the database, false otherwise
     */
    private boolean checkBucketExists(String databaseName)
    {
        if (storeCheckingComplete
                && tablesLoadedForDatabase(databaseName)) {
            return true;
        }
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            getAllDatabases();
        }
        bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("No bucket founds for schema '" + databaseName + "'");
        }
        return true;
    }

    // helpers
    /**
     * Mark whether all tables (entities) are loaded maintained in a LoadedEntities object
     *
     * @param database Name of the database to mark all of its tables are loaded
     */
    private void markEntitiesLoaded(String database)
    {
        Optional<LoadedEntities> optionalLoadedEntities = loadedEntitiesList.stream()
                .filter(entity -> entity.databaseName.equalsIgnoreCase(database))
                .findFirst();
        if (optionalLoadedEntities.isEmpty()) {
            throw new DatabaseNotFoundException("Schema " + database + " not found");
        }
        LoadedEntities entities = optionalLoadedEntities.get();
        entities.tablesLoaded = true;
    }

//    /**
//     * Loads all classes from the class loader that are implementation of {@link StorageProvider} interface. An implementation of StorageProvider must
//     * contain a static method named accept(String), which returns true. When it accepts the value set in the provider_name environment variable, it returns
//     * true. In such case, this method initialize the storage provider. An implementation of {@link StorageProvider} MUST have a constructor that has a String argument
//     *
//     * @throws InvocationTargetException Occurs if any
//     * @throws InstantiationException When constructor raises an error or the MUST-HAVE constructor does not exist
//     * @throws IllegalAccessException Classes scope is not accessible
//     * @throws NoSuchMethodException Occurs if any when accept(String) static method is not present
//     */
//    private void loadStorageProvider() throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
//    {
//        String providerCodeName = datasourceConfig.getPropertyElseDefault(STORAGE_PROVIDER_ENV_VAR, "gcs");
//        Reflections reflections = new Reflections("com.amazonaws.athena.storage");
//        Set<Class<? extends StorageProvider>> classes = reflections.getSubTypesOf(StorageProvider.class);
//        for (Class<?> storageProviderImpl : classes) {
//            Method acceptMethod = storageProviderImpl.getMethod("accept", String.class);
//            Object trueOfFalse = acceptMethod.invoke(null, providerCodeName);
//            if (((Boolean) trueOfFalse)) {
//                Constructor<?> constructor = storageProviderImpl.getConstructor(String.class);
//                storageProvider = (StorageProvider) constructor.newInstance(datasourceConfig.credentialsJson());
//                break;
//            }
//        }
//        // Still no storage providers found? Then throw run-time exception
//        if (storageProvider == null) {
//            throw new UncheckedStorageDatasourceException("Storage provider for code name '" + providerCodeName + "' was not found");
//        }
//    }

    private Optional<StorageObject> findStorageObjectKey(String tableName, String databaseName)
    {
        LOGGER.debug("Resolving Table {} under the schema {}", tableObjects, databaseName);
        Map<StorageObject, List<String>> objectNameMap = tableObjects.get(databaseName);
        if (objectNameMap != null && !objectNameMap.isEmpty()) {
            Set<StorageObject> keys = objectNameMap.keySet();
            for (StorageObject storageObject : keys) {
                if (storageObject.getTableName().equals(tableName)) {
                    return Optional.of(storageObject);
                }
            }
        }
        return Optional.empty();
    }

    private boolean checkTableIsValid(String bucket, String objectName) throws Exception
    {
        if (isPartitionedDirectory(bucket, objectName)) {
            System.out.println("Object " + objectName + " in bucket " + bucket + " is a partitioned");
//            if (true) {
//                // TODO: as of now no partition support
//                return false;
//            }
            LOGGER.debug("Object {} in the bucket {} is partitioned", objectName, bucket);
            Optional<String> optionalObjectName = getFirstObjectNameRecurse(bucket, objectName);
            boolean isValid = (optionalObjectName.isPresent() && isSupported(bucket, optionalObjectName.get()));
            LOGGER.debug("In datasource {} the file {} is valid? {}", this.getClass().getSimpleName(),
                    optionalObjectName.orElse("NONE"), isValid);
            return isValid;
        }
        else if (isExtensionCheckMandatory() && objectName.toLowerCase().endsWith(extension.toLowerCase())) {
            System.out.println("Object " + objectName + "'s extension check is mandatory and it matches, and isn't partitioned. It is in bucket " + bucket);
            return true;
        }
        System.out.println("Object " + objectName + " in bucket " + bucket  + " isn't partitioned. Check partitioned mandatory? " + isExtensionCheckMandatory());
        return !isExtensionCheckMandatory();
    }

    private Optional<String> getFirstObjectNameRecurse(String bucket, String prefix)
    {
        if (!prefix.endsWith("/")) {
            prefix += '/';
        }
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(prefix));
        for (Blob blob : blobPage.iterateAll()) {
            LOGGER.debug("GcsStorageProvider.getFirstObjectNameRecurse(): checking if {} is a folder under prefix {}", blob.getName(), prefix);
            if (prefix.equals(blob.getName())) {
                continue;
            }
            if (blob.getSize() > 0) { // it's a file
                return Optional.of(blob.getName());
            }
            else {
                Optional<String> optionalObjectName = getFirstObjectNameRecurse(bucket, blob.getName());
                if (optionalObjectName.isPresent()) {
                    return optionalObjectName;
                }
            }
        }
        return Optional.empty();
    }
}
