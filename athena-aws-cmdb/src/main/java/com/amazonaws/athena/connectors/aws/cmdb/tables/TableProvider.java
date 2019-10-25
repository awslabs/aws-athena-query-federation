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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import org.apache.arrow.vector.types.Types;

import java.util.HashSet;

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
    default GetTableLayoutResponse getTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
    {
        //Even though our table doesn't support complex layouts or partitioning, we need to convey that there is at least
        //1 partition to read as part of the query or Athena will assume partition pruning found no candidate layouts to read.
        Block partitions = BlockUtils.newBlock(blockAllocator, "partitionId", Types.MinorType.INT.getType(), 0);
        return new GetTableLayoutResponse(request.getCatalogName(), request.getTableName(), partitions, new HashSet<>());
    }

    /**
     * Effects the requested read against the table, writing result row data using the supplied BlockSpliller.
     *
     * @See RecordHandler
     */
    void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest);
}
