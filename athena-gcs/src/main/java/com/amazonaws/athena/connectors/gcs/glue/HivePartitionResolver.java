package com.amazonaws.athena.connectors.gcs.glue;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.services.glue.AWSGlueClient;

import java.util.List;

public class HivePartitionResolver implements PartitionResolver
{
    /**
     * {@inheritDoc}
     */
    @Override
    public List<StoragePartition> getPartitions(AWSGlueClient glueClient, TableName tableInfo, Constraints constraints)
    {
        return List.of();
    }
}
