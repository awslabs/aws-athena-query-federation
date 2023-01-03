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

import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_PATTERN;

public class GenericPartitionResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericPartitionResolver.class);

    /**
     * Determine the partitions based on Glue Catalog
     * @return A list of partitions
     */
    public PartitionResult getPartitions(Table table, Map<String, FieldReader> fieldReadersMap)
    {
        String locationUri = null;
        String tableLocation = table.getStorageDescriptor().getLocation();
        String partitionPattern = table.getParameters().get(PARTITION_PATTERN_PATTERN);
        if (null != partitionPattern) {
            for (Map.Entry<String, FieldReader> field : fieldReadersMap.entrySet()) {
                partitionPattern = partitionPattern.replace("{" + field.getKey() + "}", String.valueOf(field.getValue().readObject()));
            }
            locationUri = (tableLocation.endsWith("/")
                    ? tableLocation
                    : tableLocation + "/") + partitionPattern;
        }
        else {
            locationUri = tableLocation;
        }
        StorageLocation storageLocation = StorageLocation.fromUri(locationUri);
        LOGGER.info("Retrieved partition for table {} is {}", table.getName(), storageLocation);
        return new PartitionResult(table.getParameters().get(CLASSIFICATION_GLUE_TABLE_PARAM), PartitionLocation.builder()
                .bucketName(storageLocation.getBucketName())
                .location(storageLocation.getLocation())
                .build());
    }
}
