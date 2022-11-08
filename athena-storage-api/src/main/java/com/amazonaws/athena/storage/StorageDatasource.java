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
package com.amazonaws.athena.storage;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.storage.common.FilterExpression;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.common.StorageProvider;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface StorageDatasource
{
    /**
     * Returns a list of all buckets from a cloud storage as databases
     *
     * @return List of database names
     */
    List<String> getAllDatabases();

    /**
     * List all tables in a database
     *
     * @param databaseName Name of the database
     * @param nextToken    Token for the next page token, may be null
     * @param pageSie      Size of the page (number of tables per table)
     * @return List of all tables under the database
     */
    TableListResult getAllTables(String databaseName, String nextToken, int pageSie) throws IOException;

    /**
     * List all tables in a database
     *
     * @param databaseName Name of the database
     * @return List of all tables under the database
     */
    List<String> loadAllTables(String databaseName) throws IOException;

    /**
     * Returns a storage object (file) as a DB table with field names and associated file type
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @return An instance of {@link StorageTable} with column metadata
     */
    Optional<StorageTable> getStorageTable(String databaseName, String tableName) throws IOException;

    default List<StoragePartition> getStoragePartitions(Schema schema, TableName tableInfo, Constraints constraints,
                                                        String bucketName, String objectName) throws IOException
    {
        throw new RuntimeException(new UnsupportedOperationException("Method List<StoragePartition> " +
                "getStoragePartitions(Constraints, TableName, Split, String, String) not implemented in class "
                + getClass().getSimpleName()));
    }

    /**
     * Returns splits, usually by page size with offset and limit so that lambda can parallelize to load data against a given SQL statement
     *
     * @param schema      Schema of the table
     * @param constraints Constraint if any
     * @param tableInfo   Table info with table and schema name
     * @param bucketName  Name of the bucket
     * @param objectName  Name of the file under the bucket
     * @return An instance of {@link StorageSplit}
     * @throws IOException Raised if any raised during connecting to the cloud storage
     */
    default List<StorageSplit> getStorageSplits(Schema schema, Constraints constraints, TableName tableInfo,
                                                String bucketName, String objectName) throws IOException
    {
        throw new RuntimeException(new UnsupportedOperationException("Method List<StorageSplit> getStorageSplits(Schema," +
                " Constraints, TableName, Split, String," + " String) not implemented in class "
                + getClass().getSimpleName()));
    }

    /**
     * Returns splits, usually by page size with offset and limit so that lambda can parallelize to load data against a given SQL statement
     *
     * @param bucketName Name of the bucket
     * @param objectName Name of the file under the bucket
     * @return An instance of {@link StorageSplit}
     * @throws IOException Raised if any raised during connecting to the cloud storage
     */
    default List<StorageSplit> getStorageSplits(String bucketName,
                                                String objectName) throws IOException
    {
        throw new RuntimeException(new UnsupportedOperationException("Method List<StorageSplit> getStorageSplits(String,\n" +
                " String) not implemented in class " + getClass().getSimpleName()));
    }

    /**
     * Retrieves table data for provided arguments
     *
     * @param schema             Schema of the table
     * @param constraints        Constraints if any
     * @param tableInfo          Table info containing table and schema name
     * @param split              Current Split instance
     * @param queryStatusChecker An instance of {@link QueryStatusChecker} to decide whether to stop spilling while iterating over the records
     * @apiNote spiller   An instance of {@link BlockSpiller} to spill records being fetched
     */
    void readRecords(Schema schema, Constraints constraints, TableName tableInfo, Split split, BlockSpiller spiller,
                     QueryStatusChecker queryStatusChecker) throws IOException;

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param database  For which datastore will be checked
     * @param nextToken Next token for retrieve next page of table list, may be null
     */
    String loadTablesWithContinuationToken(String database, String nextToken, int pageSize) throws IOException;

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param database For which datastore will be checked
     */
    void checkMetastoreForAll(String database) throws IOException;

    /**
     * Indicates whether a datasource supports grouping of multiple files to form a single table
     *
     * @return True if the underlying datasou
     * supports multiple files to treat as a single table, false otherwise
     */
    boolean supportsPartitioning();

    List<FilterExpression> getAllFilterExpressions(Constraints constraints, String bucketName, String objectName);

    /**
     * Provides storage provider that helps accessing the storage bucket, folder and files inside
     *
     * @return A storage specific instance
     */
    StorageProvider getStorageProvider();

    /**
     * Indicates whether a file's extension check is mandatory.
     * For example, for CSV, this check maybe mandatory to check file extension
     * On the other hand, parquet file may not mandate to check its extension. Because, in some cases, when the file is generated
     * from some other system (e.g, Database), the parquet file may or may not hae the .parquet extension
     * @return true if file's extension check is mandatory, false otherwise
     */
    boolean isExtensionCheckMandatory();

    /**
     * Determines the schema information. It only discovers the field name and its index.
     * Along with the record count
     * @param bucket The name of the bucket
     * @param objectName Name of the object (file)
     * @returnt A type specific instance of {@link StorageObjectSchema}
     */
    StorageObjectSchema getObjectSchema(String bucket, String objectName) throws IOException;

    /**
     * Constitute a list of filter expression based on provided {@link Constraints} instance
     * @param schema An instance of {@link Schema}
     * @param tableName An instance of {@link TableName} that contains schema and table name
     * @param constraints An instance of {@link Constraints}
     * @param partitionFieldValueMap A map that contains partition column name(s) and value(s). Value maybe unreal
     * @return List of {@link FilterExpression} if any found
     */
    List<FilterExpression> getExpressions(String bucket, String objectName, Schema schema, TableName tableName,
                                          Constraints constraints, Map<String, String> partitionFieldValueMap) throws IOException;

    /**
     * Check to see if the storage object is supported format by the underlying data source. This is required when the file does not
     * have extension
     * @param bucket bucket The name of the bucket
     * @param objectName objectName Name of the object (file)
     * @return true if supported, false otherwise
     */
    boolean isSupported(String bucket, String objectName) throws IOException;

    /**
     * If the objectName parameter is itself a file, this is the base name. Base name helps us to retrieve metadata information.
     * However, if the objectName is a folder and contains nested folder(s), then we need to traverse the folder inside to pick one file as a base.
     * Usually nested folder is used to partition a table when exported as files from another datasource. And these file SHOULD follow the same schema
     * @param bucket bucket The name of the bucket
     * @param objectName objectName Name of the object (file)
     * @return base file name for metadata extraction
     */
    Optional<String> getBaseName(String bucket, String objectName) throws IOException;

    /**
     *
     * @param objectName
     * @param bucketName
     * @return
     */
    List<StoragePartition> getByObjectNameInBucket(String objectName, String bucketName);

    /**
     *
     * @param partition
     * @return
     */
    List<StorageSplit> getSplitsByStoragePartition(StoragePartition partition);
}
