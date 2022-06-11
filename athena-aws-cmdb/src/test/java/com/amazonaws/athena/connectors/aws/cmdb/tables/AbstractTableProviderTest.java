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
package com.amazonaws.athena.connectors.aws.cmdb.tables;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SpillConfig;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractTableProviderTest.class);

    private BlockAllocator allocator;

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private String idField = getIdField();
    private String idValue = getIdValue();
    private String expectedQuery = "queryId";
    private String expectedCatalog = "catalog";
    private String expectedSchema = getExpectedSchema();
    private String expectedTable = getExpectedTable();
    private TableName expectedTableName = new TableName(expectedSchema, expectedTable);

    private TableProvider provider;

    private final List<ByteHolder> mockS3Store = new ArrayList<>();

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private QueryStatusChecker queryStatusChecker;

    private S3BlockSpillReader blockSpillReader;

    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    protected abstract String getIdField();

    protected abstract String getIdValue();

    protected abstract String getExpectedSchema();

    protected abstract String getExpectedTable();

    protected abstract TableProvider setUpSource();

    protected abstract void setUpRead();

    protected abstract int getExpectedRows();

    protected abstract void validateRow(Block block, int pos);

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();

        when(amazonS3.putObject(anyObject()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    mockS3Store.add(byteHolder);
                    return mock(PutObjectResult.class);
                });

        when(amazonS3.getObject(anyString(), anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder = mockS3Store.get(0);
                    mockS3Store.remove(0);
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolder.getBytes()), null));
                    return mockObject;
                });

        blockSpillReader = new S3BlockSpillReader(amazonS3, allocator);

        provider = setUpSource();

        when(queryStatusChecker.isQueryRunning()).thenReturn(true);
    }

    @After
    public void after()
    {
        mockS3Store.clear();
        allocator.close();
    }

    @Test
    public void getSchema()
    {
        assertEquals(expectedSchema, provider.getSchema());
    }

    @Test
    public void getTableName()
    {
        assertEquals(expectedTableName, provider.getTableName());
    }

    @Test
    public void readTableTest()
    {
        GetTableRequest request = new GetTableRequest(identity, expectedQuery, expectedCatalog, expectedTableName);
        GetTableResponse response = provider.getTable(allocator, request);
        assertTrue(response.getSchema().getFields().size() > 1);

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put(idField,
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add(idValue).build());

        Constraints constraints = new Constraints(constraintsMap);

        ConstraintEvaluator evaluator = new ConstraintEvaluator(allocator, response.getSchema(), constraints);

        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket("bucket")
                .withPrefix("prefix")
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        ReadRecordsRequest readRequest = new ReadRecordsRequest(identity,
                expectedCatalog,
                "queryId",
                expectedTableName,
                response.getSchema(),
                Split.newBuilder(spillLocation, keyFactory.create()).build(),
                constraints,
                100_000_000,
                100_000_000);

        SpillConfig spillConfig = SpillConfig.newBuilder()
                .withSpillLocation(spillLocation)
                .withMaxBlockBytes(3_000_000)
                .withMaxInlineBlockBytes(0)
                .withRequestId("queryid")
                .withEncryptionKey(keyFactory.create())
                .build();

        setUpRead();

        BlockSpiller spiller = new S3BlockSpiller(amazonS3, spillConfig, allocator, response.getSchema(), evaluator);
        provider.readWithConstraint(spiller, readRequest, queryStatusChecker);

        validateRead(response.getSchema(), blockSpillReader, spiller.getSpillLocations(), spillConfig.getEncryptionKey());
    }

    protected void validateRead(Schema schema, S3BlockSpillReader reader, List<SpillLocation> locations, EncryptionKey encryptionKey)
    {
        int blockNum = 0;
        int rowNum = 0;
        for (SpillLocation next : locations) {
            S3SpillLocation spillLocation = (S3SpillLocation) next;
            try (Block block = reader.read(spillLocation, encryptionKey, schema)) {
                logger.info("validateRead: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());

                for (int i = 0; i < block.getRowCount(); i++) {
                    logger.info("validateRead: {}", BlockUtils.rowToString(block, i));
                    rowNum++;
                    validateRow(block, i);
                }
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        assertEquals(getExpectedRows(), rowNum);
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
