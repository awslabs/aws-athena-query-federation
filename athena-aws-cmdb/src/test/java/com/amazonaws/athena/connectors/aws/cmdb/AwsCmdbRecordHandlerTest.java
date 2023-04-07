/*-
 * #%L
 * athena-aws-cmdb
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
package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.proto.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufUtils;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AwsCmdbRecordHandlerTest
{
    private String bucket = "bucket";
    private String prefix = "prefix";
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private FederatedIdentity identity = FederatedIdentity.newBuilder().setArn("arn").setAccount("account").build();

    @Mock
    private AmazonS3 mockS3;

    @Mock
    private TableProviderFactory mockTableProviderFactory;

    @Mock
    private ConstraintEvaluator mockEvaluator;

    @Mock
    private BlockAllocator mockBlockAllocator;

    @Mock
    private BlockSpiller mockBlockSpiller;

    @Mock
    private TableProvider mockTableProvider;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Mock
    private QueryStatusChecker queryStatusChecker;

    private AwsCmdbRecordHandler handler;

    @Before
    public void setUp()
            throws Exception
    {
        when(mockTableProviderFactory.getTableProviders())
                .thenReturn(Collections.singletonMap(TableName.newBuilder().setSchemaName("schema").setTableName("table").build(), mockTableProvider));

        handler = new AwsCmdbRecordHandler(mockS3, mockSecretsManager, mockAthena, mockTableProviderFactory, com.google.common.collect.ImmutableMap.of());

        verify(mockTableProviderFactory, times(1)).getTableProviders();
        verifyNoMoreInteractions(mockTableProviderFactory);

        Mockito.lenient().when(queryStatusChecker.isQueryRunning()).thenReturn(true);
    }

    @Test
    public void readWithConstraint()
    {
        ReadRecordsRequest request = ReadRecordsRequest.newBuilder()
            .setIdentity(identity)
            .setCatalogName("catalog")
            .setQueryId("queryId")
            .setTableName(TableName.newBuilder().setSchemaName("schema").setTableName("table").build())
            .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(SchemaBuilder.newBuilder().build()))
            .setSplit(Split.newBuilder()
            .setSpillLocation(SpillLocation.newBuilder()
                    .setBucket(bucket)
                    .setKey(ProtobufUtils.buildS3SpillLocationKey(prefix, UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                    .setDirectory(true)
                    .build()
                ).setEncryptionKey(keyFactory.create())
                .build())
            .setMaxInlineBlockSize(100_000)
            .setMaxBlockSize(100_000)
            .build();

        handler.readWithConstraint(mockBlockAllocator, mockBlockSpiller, request, queryStatusChecker);

        verify(mockTableProvider, times(1)).readWithConstraint(nullable(BlockAllocator.class), nullable(BlockSpiller.class), eq(request), eq(queryStatusChecker));
    }
}
