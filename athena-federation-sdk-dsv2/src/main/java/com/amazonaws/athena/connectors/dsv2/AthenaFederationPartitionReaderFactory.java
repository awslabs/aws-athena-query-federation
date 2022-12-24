/*-
 * #%L
 * athena-federation-sdk-dsv2
 * %%
 * Copyright (C) 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.dsv2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Map;

// This class is serialized to all the executors and then createColumnarReader is run on each executor.
public class AthenaFederationPartitionReaderFactory implements PartitionReaderFactory
{
    private final AthenaFederationAdapterDefinition federationAdapterDefinition;
    private final Map<String, String> properties;

    public AthenaFederationPartitionReaderFactory(Map<String, String> properties, AthenaFederationAdapterDefinition federationAdapterDefinition)
    {
        this.federationAdapterDefinition = federationAdapterDefinition;
        this.properties = properties;
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition)
    {
        return true;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
    {
        throw new UnsupportedOperationException("Only columnar reads supported");
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition)
    {
        return new AthenaFederationPartitionReader(properties, federationAdapterDefinition, (AthenaFederationInputPartition) partition);
    }
}
