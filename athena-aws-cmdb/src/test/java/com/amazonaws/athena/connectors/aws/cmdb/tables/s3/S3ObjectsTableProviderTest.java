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
package com.amazonaws.athena.connectors.aws.cmdb.tables.s3;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ObjectsTableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(S3ObjectsTableProviderTest.class);

    @Mock
    private AmazonS3 mockS3;

    protected String getIdField()
    {
        return "bucket_name";
    }

    protected String getIdValue()
    {
        return "my_bucket";
    }

    protected String getExpectedSchema()
    {
        return "s3";
    }

    protected String getExpectedTable()
    {
        return "objects";
    }

    protected int getExpectedRows()
    {
        return 4;
    }

    protected TableProvider setUpSource()
    {
        return new S3ObjectsTableProvider(mockS3);
    }

    @Override
    protected void setUpRead()
    {
        AtomicLong count = new AtomicLong(0);
        when(mockS3.listObjectsV2(nullable(ListObjectsV2Request.class))).thenAnswer((InvocationOnMock invocation) -> {
            ListObjectsV2Request request = (ListObjectsV2Request) invocation.getArguments()[0];
            assertEquals(getIdValue(), request.getBucketName());

            ListObjectsV2Result mockResult = mock(ListObjectsV2Result.class);
            List<S3ObjectSummary> values = new ArrayList<>();
            values.add(makeObjectSummary(getIdValue()));
            values.add(makeObjectSummary(getIdValue()));
            values.add(makeObjectSummary("fake-id"));
            when(mockResult.getObjectSummaries()).thenReturn(values);

            if (count.get() > 0) {
                assertNotNull(request.getContinuationToken());
            }

            if (count.incrementAndGet() < 2) {
                when(mockResult.isTruncated()).thenReturn(true);
                when(mockResult.getNextContinuationToken()).thenReturn("token");
            }

            return mockResult;
        });
    }

    protected void validateRow(Block block, int pos)
    {
        for (FieldReader fieldReader : block.getFieldReaders()) {
            fieldReader.setPosition(pos);
            Field field = fieldReader.getField();

            if (field.getName().equals(getIdField())) {
                assertEquals(getIdValue(), fieldReader.readText().toString());
            }
            else {
                validate(fieldReader);
            }
        }
    }

    private void validate(FieldReader fieldReader)
    {
        Field field = fieldReader.getField();
        Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
        switch (type) {
            case VARCHAR:
                if (field.getName().equals("$data$")) {
                    assertNotNull(fieldReader.readText().toString());
                }
                else {
                    assertEquals(field.getName(), fieldReader.readText().toString());
                }
                break;
            case DATEMILLI:
                assertEquals(100_000, fieldReader.readLocalDateTime().atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli());
                break;
            case BIT:
                assertTrue(fieldReader.readBoolean());
                break;
            case INT:
                assertTrue(fieldReader.readInteger() > 0);
                break;
            case BIGINT:
                assertTrue(fieldReader.readLong() > 0);
                break;
            case STRUCT:
                for (Field child : field.getChildren()) {
                    validate(fieldReader.reader(child.getName()));
                }
                break;
            case LIST:
                validate(fieldReader.reader());
                break;
            default:
                throw new RuntimeException("No validation configured for field " + field.getName() + ":" + type + " " + field.getChildren());
        }
    }

    private S3ObjectSummary makeObjectSummary(String id)
    {
        S3ObjectSummary summary = new S3ObjectSummary();
        Owner owner = new Owner();
        owner.setId("owner_id");
        owner.setDisplayName("owner_name");
        summary.setOwner(owner);
        summary.setBucketName(id);
        summary.setETag("e_tag");
        summary.setKey("key");
        summary.setSize(100);
        summary.setLastModified(new Date(100_000));
        summary.setStorageClass("storage_class");
        return summary;
    }
}
