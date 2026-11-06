/*-
 * #%L
 * athena-aws-cmdb
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.aws.cmdb.tables;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.mockito.Mockito.mock;

/**
 * Tests for TableProvider interface default methods (getPartitions and enhancePartitionSchema).
 */
public class TableProviderTest
{
    private final BlockAllocator allocator = new BlockAllocatorImpl();

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void getPartitions_defaultImplementation_doesNotThrowException() throws Exception
    {
        TableProvider provider = new MinimalTableProvider();
        try (GetTableLayoutRequest request = createGetTableLayoutRequest()) {
            provider.getPartitions(mock(BlockWriter.class), request);
        }
    }

    @Test
    public void enhancePartitionSchema_defaultImplementation_doesNotThrowException() throws Exception
    {
        TableProvider provider = new MinimalTableProvider();
        try (GetTableLayoutRequest request = createGetTableLayoutRequest()) {
            provider.enhancePartitionSchema(SchemaBuilder.newBuilder(), request);
        }
    }

    private static GetTableLayoutRequest createGetTableLayoutRequest()
    {
        return new GetTableLayoutRequest(
                new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()),
                "queryId", "catalog", new TableName("test", "table"),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                SchemaBuilder.newBuilder().build(), Collections.emptySet());
    }

    private static class MinimalTableProvider implements TableProvider
    {
        @Override
        public String getSchema()
        {
            return "test";
        }

        @Override
        public TableName getTableName()
        {
            return new TableName("test", "table");
        }

        @Override
        public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
        {
            return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SchemaBuilder.newBuilder().build());
        }

        @Override
        public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
        {
            // No-op for default method testing
        }
    }
}
