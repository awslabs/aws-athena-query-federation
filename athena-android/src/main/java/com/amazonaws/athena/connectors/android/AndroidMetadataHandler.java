/*-
 * #%L
 * athena-android
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
package com.amazonaws.athena.connectors.android;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;

import java.util.Collections;

public class AndroidMetadataHandler
        extends MetadataHandler
{
    private static final String sourceType = "android";
    private static final AndroidDeviceTable androidDeviceTable = new AndroidDeviceTable();

    public AndroidMetadataHandler()
    {
        super(sourceType);
    }

    @VisibleForTesting
    protected AndroidMetadataHandler(
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        String schemaName = androidDeviceTable.getTableName().getSchemaName();
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), Collections.singletonList(schemaName));
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        return new ListTablesResponse(listTablesRequest.getCatalogName(),
                Collections.singletonList(androidDeviceTable.getTableName()));
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        if (!androidDeviceTable.getTableName().equals(getTableRequest.getTableName())) {
            throw new RuntimeException("Unknown table " + getTableRequest.getTableName());
        }

        return new GetTableResponse(getTableRequest.getCatalogName(),
                androidDeviceTable.getTableName(),
                androidDeviceTable.getSchema());
    }

    @Override
    public void getPartitions(ConstraintEvaluator constraintEvaluator, BlockWriter blockWriter, GetTableLayoutRequest request)
            throws Exception
    {
        //NoOp since we don't support partitioning
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        //Every split needs a unique spill location.
        SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
        EncryptionKey encryptionKey = makeEncryptionKey();
        Split split = Split.newBuilder(spillLocation, encryptionKey).build();
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), split);
    }
}
