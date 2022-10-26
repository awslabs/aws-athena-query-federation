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

import com.amazonaws.athena.storage.datasource.GcsDatasourceConfig;
import com.amazonaws.athena.storage.datasource.exception.DatabaseNotFoundException;
import com.amazonaws.athena.storage.datasource.exception.UncheckedStorageDatasourceException;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_BUCKET_NAME;
import static com.amazonaws.athena.storage.StorageConstants.TABLE_PARAM_OBJECT_NAME;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityName;
import static com.amazonaws.athena.storage.StorageUtil.getValidEntityNameFromFile;
import static java.util.Objects.requireNonNull;

public abstract class AbstractStorageDatasource implements StorageDatasource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStorageDatasource.class);

    protected final Storage storage;
    protected final String extension;
    protected final GcsDatasourceConfig metastoreConfig;
    protected final Map<String, String> databaseBuckets = new HashMap<>();
    protected final Map<String, Map<String, List<String>>> tableObjects = new HashMap<>();
    protected boolean storeCheckingComplete = false;
    protected List<LoadedEntities> loadedEntitiesList = new ArrayList<>();

    /**
     * Instantiate a storage data source object with provided config
     *
     * @param config An instance of GcsDatasourceConfig that contains necessary properties for instantiating an appropriate data source
     * @throws IOException If occurs during initializing input stream with GCS credential JSON
     */
    protected AbstractStorageDatasource(GcsDatasourceConfig config) throws IOException
    {
        this.metastoreConfig = requireNonNull(config, "ObjectStorageMetastoreConfig is null");
        requireNonNull(config.credentialsJson(), "GCS credential JSON is null");
        requireNonNull(config.properties(), "Environment variables were null");
        this.extension = requireNonNull(config.extension(), "File extension is null");
        GoogleCredentials credentials
                = GoogleCredentials.fromStream(new ByteArrayInputStream(config.credentialsJson().getBytes(StandardCharsets.UTF_8)))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
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
    public synchronized List<String> getAllDatabases()
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
    public synchronized TableListResult getAllTables(String databaseName, String nextToken, int pageSize)
    {
        String currentNextToken = null;
        if (!storeCheckingComplete
                || !tablesLoadedForDatabase(databaseName)) {
            currentNextToken = this.checkMetastoreForPagination(databaseName, nextToken, pageSize);
        }
        List<String> tables = List.copyOf(tableObjects.getOrDefault(databaseName, Map.of()).keySet());
        return new TableListResult(tables, currentNextToken);
    }

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param databaseName For which datastore will be checked
     * @param nextToken    Next token for retrieve next page of table list, may be null
     */
    @Override
    public synchronized String checkMetastoreForPagination(String databaseName, String nextToken, int pageSize)
    {
        if (!checkBucketExists(databaseName)) {
            return null;
        }

        if (metastoreConfig.isFilePatterned() && !supportsMultiPartFiles()) {
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
        Storage.BlobListOption maxTableCountOption = Storage.BlobListOption.pageSize(pageSize);
        Page<Blob> blobs;
        if (nextToken != null) {
            blobs = storage.list(bucketName, Storage.BlobListOption.pageToken(nextToken), maxTableCountOption);
        }
        else {
            blobs = storage.list(bucketName, maxTableCountOption);
        }
        Map<String, List<String>> objectNameMap = addTables2FileMap(blobs);
        tableObjects.put(databaseName, objectNameMap);
        String currentNextToken = blobs.getNextPageToken();
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
        Page<Blob> blobs = storage.list(bucketName);
        Map<String, List<String>> objectNameMap = addTables2FileMap(blobs);
        tableObjects.put(databaseName, objectNameMap);
        markEntitiesLoaded(databaseName);
    }

    protected Map<String, List<String>> addTables2FileMap(Page<Blob> blobs)
    {
        Map<String, List<String>> objectNameMap = new HashMap<>();
        for (Blob blob : blobs.getValues()) {
            if (blob.getName().toLowerCase(Locale.ROOT).endsWith(extension.toLowerCase(Locale.ROOT))) {
                addTable(blob.getName(), objectNameMap);
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
            if (metastoreConfig.isFilePatterned()) {
                Pattern pattern = metastoreConfig.filePattern();
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

    /**
     * Returns the size of records per split
     *
     * @return Size of records per split
     */
    public abstract int recordsPerSplit();
}
