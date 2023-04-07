/*-
 * #%L
 * athena-aws-cmdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.aws.cmdb.tables;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;

/**
 * Defines the functionality required to supply the metadata and data required for the Athena AWS CMDB connector to
 * to allow SQL queries to run over the virtual table.
 */
public interface TableProvider
{
    /**
     * The schema name (aka database) that this table provider's table belongs to.
     *
     * @return String containing the schema name.
     */
    String getSchema();

    /**
     * The fully qualified name of the table represented by this TableProvider.
     *
     * @return The TableName containing the fully qualified name of the Table.
     */
    TableName getTableName();

    /**
     * Provides access to the Schema details of the requested table.
     *
     * @See MetadataHandler
     */
    GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest);

    /**
     * Default implementation returns a single partition since many of the TableProviders may not support
     * parallel scans.
     *
     * @See MetadataHandler
     */
    default void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request)
            throws Exception
    {
        //NoOp as we do not support partitioning.
    }

    /**
     * Default implementation does not enhance the partition results schema
     *
     * @See MetadataHandler
     */
    default void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        //NoOp as we do not support partitioning or added partition data
    }

    /**
     * Effects the requested read against the table, writing result row data using the supplied BlockSpliller.
     *
     * @See RecordHandler
     */
    void readWithConstraint(BlockAllocator allocator, BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker);
}
