package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
    private FederatedIdentity identity = new FederatedIdentity("id", "principal", "account");

    @Mock
    private AmazonS3 mockS3;

    @Mock
    private TableProviderFactory mockTableProviderFactory;

    @Mock
    private ConstraintEvaluator mockEvaluator;

    @Mock
    private BlockSpiller mockBlockSpiller;

    @Mock
    private TableProvider mockTableProvider;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    private AwsCmdbRecordHandler handler;

    @Before
    public void setUp()
            throws Exception
    {
        when(mockTableProviderFactory.getTableProviders())
                .thenReturn(Collections.singletonMap(new TableName("schema", "table"), mockTableProvider));

        handler = new AwsCmdbRecordHandler(mockS3, mockSecretsManager, mockTableProviderFactory);

        verify(mockTableProviderFactory, times(1)).getTableProviders();
        verifyNoMoreInteractions(mockTableProviderFactory);
    }

    @Test
    public void readWithConstraint()
    {
        ReadRecordsRequest request = new ReadRecordsRequest(identity, "catalog",
                "queryId",
                new TableName("schema", "table"),
                SchemaBuilder.newBuilder().build(),
                Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket(bucket)
                        .withSplitId(UUID.randomUUID().toString())
                        .withQueryId(UUID.randomUUID().toString())
                        .withIsDirectory(true)
                        .build(), keyFactory.create()).build(),
                new Constraints(Collections.EMPTY_MAP),
                100_000,
                100_000);

        handler.readWithConstraint(mockEvaluator, mockBlockSpiller, request);

        verify(mockTableProvider, times(1)).readWithConstraint(any(ConstraintEvaluator.class), any(BlockSpiller.class), eq(request));
    }
}