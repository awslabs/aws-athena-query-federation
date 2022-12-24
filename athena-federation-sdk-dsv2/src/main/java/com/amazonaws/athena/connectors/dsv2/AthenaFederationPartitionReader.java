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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.Map;

// This class is created fresh on an executor by AthenaFederationPartitionReaderFactory.
// This is essentially the "main" on each executor for this DSV2 connector.
public class AthenaFederationPartitionReader implements PartitionReader<ColumnarBatch>
{
    private final Map<String, String> properties;
    private final BlockAllocator blockAllocator;
    private final AthenaFederationAdapterDefinition federationAdapterDefinition;
    private final AthenaFederationInputPartition inputPartition;

    private ColumnarBatch columnBatch = null;

    public AthenaFederationPartitionReader(
        Map<String, String> properties,
        AthenaFederationAdapterDefinition federationAdapterDefinition,
        AthenaFederationInputPartition inputPartition)
    {
        // Disable timezone packing on the executor
        com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil.disableTimezonePacking();
        this.properties = properties;
        this.blockAllocator = new BlockAllocatorImpl(ArrowUtils.rootAllocator());
        this.federationAdapterDefinition = federationAdapterDefinition;
        this.inputPartition = inputPartition;
    }

    @Override
    public boolean next() throws IOException
    {
        if (columnBatch != null) {
            // There's nothing further to read if we already read in a columnBatch
            return false;
        }

        try {
            ObjectMapper objectMapper = VersionedObjectMapperFactory.create(blockAllocator);
            // We do not want to close readResponse because we need `records` to be available
            // even after this method is done.
            ReadRecordsResponse readResponse = (ReadRecordsResponse) federationAdapterDefinition
                .getRecordHandler(federationAdapterDefinition.getFederationConfig(properties))
                .doReadRecords(
                    blockAllocator,
                    inputPartition.toReadRecordsRequest(objectMapper));
            Block records = readResponse.getRecords();
            ColumnVector[] columnVectors = records.getFieldVectors().stream()
                .map(ArrowColumnVector::new)
                .toArray(ColumnVector[]::new);
            columnBatch = new ColumnarBatch(columnVectors, readResponse.getRecordCount());
            return readResponse.getRecordCount() > 0;
        }
        catch (IOException ex) {
            // If we get an IOException, pass it through
            throw ex;
        }
        catch (Exception ex) {
            // If we get anything else, rethrow unchecked since the base class doesn't
            // declare throwing anything other than IOException
            throw new RuntimeException(ex);
        }
    }

    @Override
    public ColumnarBatch get()
    {
        return columnBatch;
    }

    @Override
    public void close()
    {
        if (columnBatch != null) {
            columnBatch.close();
        }
        blockAllocator.close();
    }
}
