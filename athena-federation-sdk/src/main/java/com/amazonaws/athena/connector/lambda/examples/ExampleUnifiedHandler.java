package com.amazonaws.athena.connector.lambda.examples;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.handlers.UnifiedHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.arrow.util.VisibleForTesting;

public class ExampleUnifiedHandler
        extends UnifiedHandler
{
    private static final String SOURCE_TYPE = "custom";
    private final ExampleMetadataHandler metadataHandler;
    private final ExampleRecordHandler recordHandler;

    public ExampleUnifiedHandler()
    {
        this(AmazonS3ClientBuilder.standard().build(), new ExampleMetadataHandler(), new ExampleRecordHandler());
    }

    @VisibleForTesting
    protected ExampleUnifiedHandler(AmazonS3 amazonS3, ExampleMetadataHandler metadataHandler, ExampleRecordHandler recordHandler)
    {
        super(amazonS3, SOURCE_TYPE);
        this.metadataHandler = metadataHandler;
        this.recordHandler = recordHandler;
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        recordHandler.readWithConstraint(constraintEvaluator, spiller, recordsRequest);
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        return metadataHandler.doListSchemaNames(allocator, request);
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        return metadataHandler.doListTables(allocator, request);
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        return metadataHandler.doGetTable(allocator, request);
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator allocator, GetTableLayoutRequest request)
    {
        return metadataHandler.doGetTableLayout(allocator, request);
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        return metadataHandler.doGetSplits(allocator, request);
    }
}
