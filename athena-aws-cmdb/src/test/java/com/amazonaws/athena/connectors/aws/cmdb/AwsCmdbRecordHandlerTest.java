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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AwsCmdbRecordHandlerTest
{
    /** Used for both maxBlockSize and maxInlineBlockSize on {@link ReadRecordsRequest} in tests. */
    private static final int DEFAULT_TEST_BLOCK_LIMIT = 100_000;

    private final String catalog = "catalog";
    private final String queryId = "queryId";
    private final String bucket = "bucket";
    private final String prefix = "prefix";
    private final TableName tableName = new TableName("schema", "table");
    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private final FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());

    @Mock
    private S3Client mockS3;

    @Mock
    private TableProviderFactory mockTableProviderFactory;

    @Mock
    private BlockSpiller mockBlockSpiller;

    @Mock
    private TableProvider mockTableProvider;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Mock
    private QueryStatusChecker queryStatusChecker;

    private AwsCmdbRecordHandler handler;

    @Before
    public void setUp()
            throws Exception
    {
        when(mockTableProviderFactory.getTableProviders())
                .thenReturn(Collections.singletonMap(tableName, mockTableProvider));

        handler = new AwsCmdbRecordHandler(mockS3, mockSecretsManager, mockAthena, mockTableProviderFactory, com.google.common.collect.ImmutableMap.of());

        verify(mockTableProviderFactory, times(1)).getTableProviders();
        verifyNoMoreInteractions(mockTableProviderFactory);

        Mockito.lenient().when(queryStatusChecker.isQueryRunning()).thenReturn(true);
    }

    @Test
    public void readWithConstraint()
    {
        ReadRecordsRequest request = createReadRecordsRequest(tableName);

        handler.readWithConstraint(mockBlockSpiller, request, queryStatusChecker);

        verify(mockTableProvider, times(1)).readWithConstraint(nullable(BlockSpiller.class), eq(request), eq(queryStatusChecker));
        assertEquals(tableName, request.getTableName());
        assertEquals(catalog, request.getCatalogName());
        assertEquals(queryId, request.getQueryId());
    }

    @Test
    public void readWithConstraint_whenTableUnknown_throwsNullPointerException()
    {
        when(mockTableProviderFactory.getTableProviders()).thenReturn(Collections.emptyMap());
        AwsCmdbRecordHandler handlerWithEmptyProviders = new AwsCmdbRecordHandler(mockS3, mockSecretsManager, mockAthena, mockTableProviderFactory, com.google.common.collect.ImmutableMap.of());

        ReadRecordsRequest request = createReadRecordsRequest(new TableName("unknown_schema", "unknown_table"));

        assertThrows(NullPointerException.class, () ->
                handlerWithEmptyProviders.readWithConstraint(mockBlockSpiller, request, queryStatusChecker));
    }

    private ReadRecordsRequest createReadRecordsRequest(TableName requestTableName)
    {
        return new ReadRecordsRequest(identity, catalog, queryId, requestTableName,
                SchemaBuilder.newBuilder().build(),
                createSplit(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                DEFAULT_TEST_BLOCK_LIMIT,
                DEFAULT_TEST_BLOCK_LIMIT);
    }

    private Split createSplit()
    {
        return Split.newBuilder(S3SpillLocation.newBuilder()
                .withBucket(bucket)
                .withPrefix(prefix)
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build(), keyFactory.create()).build();
    }
}
