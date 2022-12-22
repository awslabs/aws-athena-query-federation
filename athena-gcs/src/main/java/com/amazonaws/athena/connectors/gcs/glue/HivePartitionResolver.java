/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs.glue;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connectors.gcs.common.PartitionLocation;
import com.amazonaws.athena.connectors.gcs.common.PartitionResult;
import com.amazonaws.athena.connectors.gcs.common.StorageLocation;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpression;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilder;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Table;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;

public class HivePartitionResolver implements PartitionResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HivePartitionResolver.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionResult getPartitions(MetadataRequest request, Schema schema, TableName tableInfo, Constraints constraints, AWSGlue awsGlue)
    {
        LOGGER.info("Retrieving partitions for table {} under the schema {}", tableInfo.getTableName(), tableInfo.getSchemaName());
        Table table = GlueUtil.getGlueTable(request, tableInfo, awsGlue);
        String locationUri = table.getStorageDescriptor().getLocation();
        LOGGER.info("Location URI for table {}.{} is {}", tableInfo.getSchemaName(), tableInfo.getTableName(), locationUri);
        StorageLocation storageLocation = StorageLocation.fromUri(locationUri);
        LOGGER.info("Storage location for {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), storageLocation);
        List<FilterExpression> expressions = new FilterExpressionBuilder(schema).getExpressions(constraints);
        LOGGER.info("Expressions for the request of {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), expressions);
        if (expressions.isEmpty()) {
            // Returning a single partition
        }
        else {
            // list all prefix based on expression and constitute a list of partitions
        }
        return new PartitionResult(table.getParameters().get(CLASSIFICATION_GLUE_TABLE_PARAM), List.of(PartitionLocation.builder()
                .bucketName(storageLocation.getBucketName())
                .location(storageLocation.getLocation())
                .build()));
//        return new PartitionResult(table.getParameters().get(CLASSIFICATION_GLUE_TABLE_PARAM), List.of());
    }
}
