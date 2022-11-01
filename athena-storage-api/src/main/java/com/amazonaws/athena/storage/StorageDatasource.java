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
import com.amazonaws.athena.storage.gcs.StorageSplit;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.List;
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
    TableListResult getAllTables(String databaseName, String nextToken, int pageSie);

    /**
     * List all tables in a database
     *
     * @param databaseName Name of the database
     * @return List of all tables under the database
     */
    List<String> loadAllTables(String databaseName);

    /**
     * Returns a storage object (file) as a DB table with field names and associated file type
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @return An instance of {@link StorageTable} with column metadata
     */
    Optional<StorageTable> getStorageTable(String databaseName, String tableName);

    default List<StoragePartition> getStoragePartitions(Schema schema, Constraints constraints, TableName tableInfo,
                                                String bucketName, String objectName) throws IOException
    {
        // dummy code for testing for now
        if (true) {
            return List.of(StoragePartition.builder()
                    .objectName("abc.parquet")
                    .location("bucket1/folder1")
                    .recordCount(500000L)
                    .build());
        }

        throw new RuntimeException(new UnsupportedOperationException("Method List<StorageSplit> getStoragePartitions(Schema," +
                " Constraints, TableName, Split, String," + " String) not implemented in class "
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
     * @param schema      Schema of the table
     * @param constraints Constraints if any
     * @param tableInfo   Table info containing table and schema name
     * @param split       Current Split instance
     * @apiNote spiller   An instance of {@link BlockSpiller} to spill records being fetched
     * @param queryStatusChecker An instance of {@link QueryStatusChecker} to decide whether to stop spilling while iterating over the records
     */
    void readRecords(Schema schema, Constraints constraints, TableName tableInfo, Split split, BlockSpiller spiller,
                     QueryStatusChecker queryStatusChecker);

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param database  For which datastore will be checked
     * @param nextToken Next token for retrieve next page of table list, may be null
     */
    String checkMetastoreForPagination(String database, String nextToken, int pageSize);

    /**
     * Checks datastore for a specific database (bucket). It looks whether the database exists, if it does, it loads all
     * the tables (files) in it based on extension specified in the environment variables
     *
     * @param database For which datastore will be checked
     */
    void checkMetastoreForAll(String database);

    /**
     * Indicates whether a datasource supports grouping of multiple files to form a single table
     *
     * @return True if the underlying datasource supports multiple files to treat as a single table, false otherwise
     */
    boolean supportsMultiPartFiles();
}
