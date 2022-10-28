/*-
 * #%L
 * athena-msk
 * %%
 * Copyright (C) 2019 - 2022 Trianz
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
package com.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.athena.connectors.msk.trino.QueryExecutor;
import com.athena.connectors.msk.trino.TrinoRecord;
import com.athena.connectors.msk.trino.TrinoRecordSet;
import com.google.common.io.ByteStreams;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.MaterializedResult;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest(AmazonMskUtils.class)
public class AmazonMskRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(AmazonMskRecordHandlerTest.class);
    public static final String BOOL_FIELD_NAME_1 = "bool1";
    public static final String INTEGER_FIELD_NAME_1 = "int1";
    public static final String STRING_FIELD_NAME_1 = "string1";
    public static final String FLOAT_FIELD_NAME_1 = "float1";

    private String bucket = "bucket";
    private String prefix = "prefix";

    @Mock
    AmazonS3 amazonS3;

    @Mock
    AWSSecretsManager awsSecretsManager;

    @Mock
    private AmazonAthena athena;

    @Mock
    QueryExecutor queryExecutor;

    @Mock
    FederatedIdentity federatedIdentity;

    private AmazonMskRecordHandler amazonMskRecordHandler;

    private BlockAllocator allocator;
    private S3BlockSpiller spillWriter;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private EncryptionKey encryptionKey = keyFactory.create();
    private SpillConfig spillConfig;
    private S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(UUID.randomUUID().toString())
            .withIsDirectory(true)
            .build();

    //Gets the schema in Arrow Format.
    static org.apache.arrow.vector.types.pojo.Schema getBlockTestSchema()
    {
        return SchemaBuilder.newBuilder()
                .addBitField(BOOL_FIELD_NAME_1)
                .addIntField(INTEGER_FIELD_NAME_1)
                .addStringField(STRING_FIELD_NAME_1)
                .addFloat8Field(FLOAT_FIELD_NAME_1)
                .build();
    }

    @Before
    public void init()
    {
        logger.info("Starting init.");
        System.setProperty("aws.region", "us-east-1");
        MockitoAnnotations.initMocks(this);

        allocator = new BlockAllocatorImpl();
        mockS3Client();

        //Create Spill config
        spillConfig = SpillConfig.newBuilder()
                .withEncryptionKey(encryptionKey)
                //This will be enough for a single block
                .withMaxBlockBytes(100000)
                //This will force the writer to spill.
                .withMaxInlineBlockBytes(100)
                //Async Writing.
                .withNumSpillThreads(0)
                .withRequestId(UUID.randomUUID().toString())
                .withSpillLocation(s3SpillLocation)
                .build();

        schemaForRead = getBlockTestSchema();

        spillWriter = new S3BlockSpiller(amazonS3, spillConfig, allocator, schemaForRead, ConstraintEvaluator.emptyEvaluator());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        amazonMskRecordHandler = new AmazonMskRecordHandler(amazonS3, awsSecretsManager, athena, queryExecutor);

        logger.info("Completed init.");
    }

    @Test
    public void testReadWithConstraint()
            throws Exception
    {
        try (ReadRecordsRequest request = new ReadRecordsRequest(
                federatedIdentity,
                "testCatalog",
                "queryId",
                new TableName("testSchema", "testTable"),
                getBlockTestSchema(),
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket(bucket)
                                .withPrefix(prefix)
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).build(),
                new Constraints(Collections.EMPTY_MAP),
                0,          //This is ignored when directly calling readWithConstraints.
                0)) {   //This is ignored when directly calling readWithConstraints.
            //Always return try for the evaluator to keep all rows.
            ConstraintEvaluator evaluator = mock(ConstraintEvaluator.class);
            when(evaluator.apply(any(String.class), any(Object.class))).thenAnswer(
                    (InvocationOnMock invocationOnMock) -> {
                        return true;
                    }
            );

            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            List<Object> row1 = new ArrayList<>(Arrays.asList(false, 1000, "test1", 123123.12312));
            List<Object> row2 = new ArrayList<>(Arrays.asList(true, 500, "test2", 5345234.22111));

            List<Type> types = new ArrayList<>(Arrays.asList(BooleanType.BOOLEAN, IntegerType.INTEGER, VarcharType.createVarcharType(20), DoubleType.DOUBLE));

            when(queryExecutor.execute("SELECT bool1,int1,string1,float1 from testSchema.testTable")).thenReturn(
                    new TrinoRecordSet(List.of(
                            new TrinoRecord(MaterializedResult.DEFAULT_PRECISION, row1),
                            new TrinoRecord(MaterializedResult.DEFAULT_PRECISION, row2))));

            amazonMskRecordHandler.readWithConstraint(spillWriter, request, queryStatusChecker);
        }
    }

    //Mocks the S3 client by storing any putObjects() and returning the object when getObject() is called.
    private void mockS3Client()
    {
        List<ByteHolder> mockS3Storage = new ArrayList<>();

        when(amazonS3.putObject(anyObject(), anyObject(), anyObject(), anyObject()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = (InputStream) invocationOnMock.getArguments()[2];
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    mockS3Storage.add(byteHolder);
                    return mock(PutObjectResult.class);
                });

        when(amazonS3.getObject(anyString(), anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder = mockS3Storage.get(0);
                    mockS3Storage.remove(0);
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolder.getBytes()), null));
                    return mockObject;
                });
    }

    private class ByteHolder
    {
        private byte[] bytes;

        void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        byte[] getBytes()
        {
            return bytes;
        }
    }
}
