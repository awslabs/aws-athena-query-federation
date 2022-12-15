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
import com.amazonaws.athena.connectors.gcs.filter.FilterExpression;
import com.amazonaws.athena.connectors.gcs.filter.FilterExpressionBuilder;
import com.amazonaws.services.glue.AWSGlueClient;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;

public class HivePartitionResolver implements PartitionResolver
{
    /**
     * {@inheritDoc}
     */
    @Override
    public List<StoragePartition> getPartitions(AWSGlueClient glueClient, TableName tableInfo, Constraints constraints)
    {
        Schema schema = getTableSchema(glueClient, tableInfo.getTableName());
        List<FilterExpression> expressions = new FilterExpressionBuilder(schema).getExpressions(constraints, Map.of());
        if (expressions.isEmpty()) {
            // Returning a single partition
        }
        return List.of();
    }

    private Schema getTableSchema(AWSGlueClient glueClient, String tableName)
    {
        return null;
    }
}
