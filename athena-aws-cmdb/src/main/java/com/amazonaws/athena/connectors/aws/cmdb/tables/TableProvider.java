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

public interface TableProvider
{
    String getSchema();

    TableName getTableName();

    GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest);

    default GetTableLayoutResponse getTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
    {
        //Even though our table doesn't support complex layouts or partitioning, we need to convey that there is at least
        //1 partition to read as part of the query or Athena will assume partition pruning found no candidate layouts to read.
        Block partitions = BlockUtils.newBlock(blockAllocator, "partitionId", Types.MinorType.INT.getType(), 0);
        return new GetTableLayoutResponse(request.getCatalogName(), request.getTableName(), partitions, new HashSet<>());
    }

    void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest);
}
