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

import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class S3BucketsTableProviderTest
        extends AbstractTableProviderTest
{
    @Mock
    private S3Client mockS3;

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
        return "buckets";
    }

    protected int getExpectedRows()
    {
        return 2;
    }

    protected TableProvider setUpSource()
    {
        return new S3BucketsTableProvider(mockS3);
    }

    @Override
    protected void setUpRead()
    {
        when(mockS3.listBuckets()).thenAnswer((InvocationOnMock invocation) -> {
            List<Bucket> values = new ArrayList<>();
            values.add(makeBucket("asdf"));
            values.add(makeBucket(getIdValue()));
            values.add(makeBucket(getIdValue()));
            values.add(makeBucket("fake-id"));
            return ListBucketsResponse.builder().buckets(values).build();
        });
        when(mockS3.getBucketAcl(any(GetBucketAclRequest.class)))
                .thenThrow(S3Exception.builder().message("I am an exception").build())
                .thenAnswer((InvocationOnMock invocation) -> {
            return GetBucketAclResponse.builder()
                    .owner(Owner.builder()
                            .displayName("owner_name")
                            .id("owner_id")
                            .build())
                    .build();
        });
    }

    private Bucket makeBucket(String id)
    {
        Bucket bucket = Bucket.builder()
            .name(id)
            .creationDate(new Date(100_000).toInstant())
            .build();
        return bucket;
    }
}
