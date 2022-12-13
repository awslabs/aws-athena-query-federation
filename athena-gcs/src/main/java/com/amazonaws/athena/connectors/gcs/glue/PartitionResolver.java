package com.amazonaws.athena.connectors.gcs.glue;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.common.StoragePartition;
import com.amazonaws.services.glue.AWSGlueClient;

import java.util.List;

public interface PartitionResolver
{
    /**
     * Determine the partitions based on Glue Catalog
     * @param glueClient An instance of {@link AWSGlueClient}
     * @param tableInfo An instance of {@link TableName}
     * @param constraints An instance of {@link Constraints}
     * @return A list of partitions
     */
    List<StoragePartition> getPartitions(AWSGlueClient glueClient, TableName tableInfo, Constraints constraints);
}
