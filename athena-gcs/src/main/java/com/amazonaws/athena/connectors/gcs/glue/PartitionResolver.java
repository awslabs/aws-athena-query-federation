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
