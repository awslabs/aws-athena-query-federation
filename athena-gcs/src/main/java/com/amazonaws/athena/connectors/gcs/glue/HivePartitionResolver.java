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

import com.amazonaws.athena.connectors.gcs.common.PartitionLocation;
import com.amazonaws.athena.connectors.gcs.common.PartitionResult;
import com.amazonaws.athena.connectors.gcs.common.StorageLocation;
import com.amazonaws.services.glue.model.Table;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
import java.util.List;
=======
import java.util.Map;
>>>>>>> 8913f0f9 (GcsMetadataHandler changes for doGetSplits)

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;

public class HivePartitionResolver implements PartitionResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HivePartitionResolver.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionResult getPartitions(Table table, Map<String, FieldReader> fieldReadersMap)
    {
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_PATTERN);
        for (Map.Entry<String, FieldReader> field : fieldReadersMap.entrySet()) {
            partitionPattern = partitionPattern.replace("{" + field.getKey() + "}", String.valueOf(field.getValue().readObject()));
        }
        String tableLocation = table.getStorageDescriptor().getLocation();

        String locationUri = (tableLocation.endsWith("/")
                ? tableLocation : tableLocation + "/") + partitionPattern;

        StorageLocation storageLocation = StorageLocation.fromUri(locationUri);
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
        //LOGGER.info("Storage location for {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), storageLocation);
        //List<FilterExpression> expressions = new FilterExpressionBuilder(schema).getExpressions(constraints, Map.of());
        //LOGGER.info("Expressions for the request of {}.{} is \n{}", tableInfo.getSchemaName(), tableInfo.getTableName(), expressions);
//        if (expressions.isEmpty()) {
//            // Returning a single partition
//        }
//        else {
//            // list all prefix based on expression and constitute a list of partitions
//        }
=======

>>>>>>> 06e0c49c (GcsMetadataHandler changes for doGetSplits)
        return new PartitionResult(table.getParameters().get(CLASSIFICATION_GLUE_TABLE_PARAM), PartitionLocation.builder()
>>>>>>> 8913f0f9 (GcsMetadataHandler changes for doGetSplits)
                .bucketName(storageLocation.getBucketName())
                .location(storageLocation.getLocation())
                .build());
    }
}
