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
package com.amazonaws.athena.storage;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.storage.common.PagedObject;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.common.StorageProvider;
import com.amazonaws.athena.storage.datasource.StorageDatasourceConfig;
import com.amazonaws.athena.storage.datasource.exception.DatabaseNotFoundException;
import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.storage.StorageConstants.STORAGE_PROVIDER_ENV_VAR;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityName;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityNameFromFile;
import static java.util.Objects.requireNonNull;

public abstract class AbstractStorageDatasource implements StorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStorageDatasource.class);

    protected final String extension;
    protected final StorageDatasourceConfig datasourceConfig;
    protected final Map<String, String> databaseBuckets = new HashMap<>();
    protected final Map<String, Map<String, List<String>>> tableObjects = new HashMap<>();
    protected boolean storeCheckingComplete = false;
    protected List<LoadedEntities> loadedEntitiesList = new ArrayList<>();
    protected StorageProvider storageProvider;

    /**
     * Instantiate a storage data source object with provided config
     *
     * @param config An instance of GcsDatasourceConfig that contains necessary properties for instantiating an appropriate data source
     * @throws IOException If occurs during initializing input stream with GCS credential JSON
     */
    protected AbstractStorageDatasource(StorageDatasourceConfig config) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        this.datasourceConfig = requireNonNull(config, "ObjectStorageMetastoreConfig is null");
        requireNonNull(config.credentialsJson(), "GCS credential JSON is null");
        requireNonNull(config.properties(), "Environment variables were null");
        this.extension = requireNonNull(config.extension(), "File extension is null");
        loadStorageProvider();
    }

    /**
     * Return a list of Field instances with field name and field type (Arrow type)
     *
     * @param bucketName  Name of the bucket
     * @param objectNames Name of the file in the specified bucket
     * @return List of field instances
     * @throws IOException Raises if any occurs
     */
    protected abstract List<Field> getTableFields(String bucketName, List<String> objectNames) throws IOException;

    /**
     * Returns a list of all buckets from a cloud storage as databases
     *
     * @return List of database names
     */
    @Override
    public List<String> getAllDatabases()
    {
        List<String> buckets = storageProvider.getAllBuckets();
        for (String bucketName : buckets) {
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
    public synchronized TableListResult getAllTables(String databaseName, String nextToken, int pageSize)
    {
        String currentNextToken = null;
        if (!storeCheckingComplete
                || !tablesLoadedForDatabase(databaseName)) {
            currentNextToken = this.loadTablesWithContinuationToken(databaseName, nextToken, pageSize);
        }
        List<String> tables = List.copyOf(tableObjects.getOrDefault(databaseName, Map.of()).keySet());
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
    @Override
    public synchronized String loadTablesWithContinuationToken(String databaseName, String nextToken, int pageSize)
    {
        if (!checkBucketExists(databaseName)) {
            return null;
        }

        if (datasourceConfig.isFilePatterned() && !supportsMultiPartFiles()) {
            throw new UncheckedStorageDatasourceException("Datasource that reads file with " + extension + " extension does not support reading multiple files");
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
    public List<String> loadAllTables(String databaseName)
    {
        checkMetastoreForAll(databaseName);
        return List.copyOf(tableObjects.getOrDefault(databaseName, Map.of()).keySet());
    }

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param database For which datastore will be checked
     */
    @Override
    public void checkMetastoreForAll(String database)
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
    public synchronized Optional<StorageTable> getStorageTable(String databaseName, String tableName)
    {
        if (!storeCheckingComplete) {
            this.checkMetastoreForAll(databaseName);
        }
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new RuntimeException("ObjectStorageHiveMetastore.getTable: bucket null does not exist");
        }
        Map<String, List<String>> objectNameMap = tableObjects.get(databaseName);
        if (objectNameMap != null && !objectNameMap.isEmpty()) {
            List<String> objectNames = objectNameMap.get(tableName);
            if (objectNames != null) {
                try {
                    StorageTable table = StorageTable.builder()
                            .setDatabaseName(databaseName)
                            .setTableName(tableName)
                            .setParameter(TABLE_PARAM_BUCKET_NAME, bucketName)
                            .setParameter(TABLE_PARAM_OBJECT_NAME, String.join(",", objectNames))
                            .setFieldList(getTableFields(bucketName, objectNames))
                            .build();
                    return Optional.of(table);
                }
                catch (Exception exception) {
                    // We ignore this exception. Because if this Exception is propagated, other necessary Table(s) will not be
                    // listed in the Tables list in the Athena Console (or when calling programmatically)
                    LOGGER.error("Error occurred during resolving Table {} under the schema {}", tableObjects, databaseName, exception);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public List<StoragePartition> getStoragePartitions(Schema schema, Constraints constraints, TableName tableInfo,
                                                       String bucketName, String objectName)
    {
//        requireNonNull(bucketName, "Bucket name was null");
//        requireNonNull(objectName, "objectName name was null");
//        BlobId blobId = BlobId.of(bucketName, objectName);
//        Blob storageObject = storage.get(blobId);
//        if (storageObject.isDirectory()) {
//            // TODO: need to implement correctly to list all the objects under a folder inside a bucket
//            Page<Blob> blobs = storage.list(bucketName + "/" + objectName);
//            QueryPlanner queryPlanner = new QueryPlanner();
//            for (Blob blob : blobs.iterateAll()) {
//                if (blob.isDirectory()) {
//                    if (PartitionUtil.isPartitionFolder(blob.getName())) {
//                        List<FilterExpression> expressions = getAllFilterExpressions(constraints, bucketName, objectName);
//                        if (expressions.isEmpty()) {
//                            // load all files form all nested sub-folders
//                        } else {
//                            Optional<FieldValue> optionalFieldValue = FieldValue.from(blob.getName());
//                            if (optionalFieldValue.isPresent()) {
//                                if (queryPlanner.accept(optionalFieldValue.get(), expressions)) {
//                                    // TODO: read nested folder content and partition accordingly
//                                }
//                            }
//                        }
//                    } else {
//                        // TODO: read nested folder without partition logic. That is, single partition
//                    }
//                }
//            }
//        } else {
//            // A file (aka Table) under a non-partitioned bucket/folder
//        }
        return List.of();
    }

    @Override
    public StorageProvider getStorageProvider()
    {
        return this.storageProvider;
    }

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
    protected String loadTablesInternal(String databaseName, String nextToken, int pageSize)
    {
        requireNonNull(databaseName, "Database name was null");
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("Bucket null does not exist for Database " + databaseName);
        }
        PagedObject pagedObject = storageProvider.getFileNames(bucketName, nextToken, pageSize);
        // in the following Map, the key is the Table name, and the value is a list of String contains one or more objects (file)
        // when the file_pattern environment variable is set, usually list String may contain more than one object
        // And in case a table consists of multiple objects, during read it read data from all the objects
        Map<String, List<String>> objectNameMap = convertBlobsToTableObjectsMap(pagedObject.getFileNames());
        tableObjects.put(databaseName, objectNameMap);
        String currentNextToken = pagedObject.getNextToken();
        if (currentNextToken == null) {
            markEntitiesLoaded(databaseName);
        }
        return currentNextToken;
    }

    /**
     * Loads all tables for the given database without pagination
     *
     * @param databaseName Name of the database
     */
    protected void loadTablesInternal(String databaseName)
    {
        requireNonNull(databaseName, "Database name was null");
        String bucketName = databaseBuckets.get(databaseName);
        if (bucketName == null) {
            throw new DatabaseNotFoundException("Bucket null does not exist for Database " + databaseName);
        }
        List<String> fileNames = storageProvider.getFileNames(bucketName);
        Map<String, List<String>> objectNameMap = convertBlobsToTableObjectsMap(fileNames);
        tableObjects.put(databaseName, objectNameMap);
        markEntitiesLoaded(databaseName);
    }

    protected Map<String, List<String>> convertBlobsToTableObjectsMap(List<String> fileNames)
    {
        Map<String, List<String>> objectNameMap = new HashMap<>();
        for (String fileName : fileNames) {
            if (!isExtensionCheckMandatory()) {
                addTable(fileName, objectNameMap);
            }
            else if (fileName.toLowerCase(Locale.ROOT).endsWith(extension.toLowerCase(Locale.ROOT))) {
                addTable(fileName, objectNameMap);
            }
        }
        return objectNameMap;
    }

    /**
     * Adds a table with multi parts files. This will maintain a list of multipart files
     *
     * @param objectName Name of the object in storage (file)
     * @param tableMap   Name of the table under which we'll maintain a list of files fall under the pattern
     */
    protected void addTable(String objectName,
                            Map<String, List<String>> tableMap)
    {
        String strLowerObjectName = objectName.toLowerCase(Locale.ROOT);
        if (strLowerObjectName.endsWith(extension.toLowerCase(Locale.ROOT))) {
            if (datasourceConfig.isFilePatterned()) {
                Pattern pattern = datasourceConfig.filePattern();
                String tableName = strLowerObjectName.substring(0, strLowerObjectName.lastIndexOf(extension.toLowerCase(Locale.ROOT)));
                Matcher matcher = pattern.matcher(tableName);
                while (matcher.find()) {
                    String strTableName = getValidEntityNameFromFile(matcher.group(1), this.extension);
                    tableMap.computeIfAbsent(strTableName, files -> new ArrayList<>()).add(objectName);
                }
            }
            else {
                tableMap.computeIfAbsent(getValidEntityNameFromFile(objectName, this.extension), files -> new ArrayList<>()).add(objectName);
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
            throw new DatabaseNotFoundException("Schema " + database + " not found");
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

    private void loadStorageProvider() throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException
    {
        String providerCodeName = datasourceConfig.getPropertyElseDefault(STORAGE_PROVIDER_ENV_VAR, "gcs");
        ServiceLoader<StorageProvider> loader = ServiceLoader.load(StorageProvider.class);
        for (StorageProvider storageProviderImpl : loader) {
            Class<?> clazz = storageProviderImpl.getClass();
            Method acceptMethod = clazz.getMethod("accept", String.class);
            Object trueOfFalse = acceptMethod.invoke(null, providerCodeName);
            if (((Boolean) trueOfFalse) == true) {
                Constructor<?> constructor = clazz.getConstructor(String.class);
                storageProvider = (StorageProvider) constructor.newInstance(datasourceConfig.credentialsJson());
            }
        }
        throw new UncheckedStorageDatasourceException("Storage provider for code name '" + providerCodeName + "' was not found");
    }
}
