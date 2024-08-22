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

package com.amazonaws.athena.connector.lambda.domain.spill;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Spy;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SpillLocationVerifierTest
{
    private static final Logger logger = LoggerFactory.getLogger(SpillLocationVerifierTest.class);

    @Spy
    private SpillLocationVerifier spyVerifier;
    private List<String> bucketNames;

    @Before
    public void setup()
    {
        logger.info("setUpBefore - enter");

        bucketNames = Arrays.asList("bucket1", "bucket2", "bucket3");
        S3Client mockS3 = createMockS3(bucketNames);
        spyVerifier = spy(new SpillLocationVerifier(mockS3));

        logger.info("setUpBefore - exit");
    }

    @After
    public void tearDown()
    {
        // no op
    }

    @Test
    public void checkBucketAuthZ()
    {
        logger.info("checkBucketAuthZ - enter");

        Random random = new Random();
        int index = random.nextInt(bucketNames.size());

        try {
            spyVerifier.checkBucketAuthZ(bucketNames.get(index));
        }
        catch(RuntimeException e) {
            fail("checkBucketAuthZ failed");
        }
        verify(spyVerifier, times(1)).updateBucketState();
        verify(spyVerifier, times(1)).passOrFail();

        try {
            spyVerifier.checkBucketAuthZ(bucketNames.get(index));
        }
        catch(RuntimeException e) {
            fail("checkBucketAuthZ failed");
        }
        verify(spyVerifier, times(1)).updateBucketState();
        verify(spyVerifier, times(2)).passOrFail();

        try {
            spyVerifier.checkBucketAuthZ("");
        }
        catch(RuntimeException e) {
            fail("checkBucketAuthZ failed");
        }
        verify(spyVerifier, times(1)).updateBucketState();
        verify(spyVerifier, times(2)).passOrFail();

        logger.info("checkBucketAuthZ - exit");
    }

    @Test
    public void checkBucketAuthZFail()
    {
        logger.info("checkBucketAuthZFail - enter");

        String bucketNotOwn = "forbidden";

        try {
            spyVerifier.checkBucketAuthZ(bucketNotOwn);
            fail("checkBucketAuthZFail failed");
        }
        catch(RuntimeException e) {
            assertEquals(String.format("spill_bucket: \"%s\" not found under your account. Please make sure you have access to the bucket and spill_bucket input has no trailing '/'", bucketNotOwn), e.getMessage());
        }
        verify(spyVerifier, times(1)).updateBucketState();
        verify(spyVerifier, times(1)).passOrFail();

        try {
            spyVerifier.checkBucketAuthZ(bucketNotOwn);
            fail("checkBucketAuthZFail failed");
        }
        catch(RuntimeException e) {
            assertEquals(String.format("spill_bucket: \"%s\" not found under your account. Please make sure you have access to the bucket and spill_bucket input has no trailing '/'", bucketNotOwn), e.getMessage());
        }
        verify(spyVerifier, times(1)).updateBucketState();
        verify(spyVerifier, times(2)).passOrFail();

        logger.info("checkBucketAuthZFail - exit");
    }

    private S3Client createMockS3(List<String> buckets)
    {
        S3Client s3mock = mock(S3Client.class);
        when(s3mock.headBucket(any(HeadBucketRequest.class)))
                .thenAnswer((Answer<HeadBucketResponse>) invocationOnMock -> {
                    String bucketName = ((HeadBucketRequest) invocationOnMock.getArguments()[0]).bucket();
                    if (buckets.contains(bucketName)) {
                        return null;
                    }
                    AwsServiceException exception;
                    if (bucketName.equals("forbidden")) {
                        exception = S3Exception.builder().statusCode(403).message("Forbidden").build();
                    }
                    else {
                        exception = S3Exception.builder().statusCode(404).message("Not Found").build();
                    }
                    throw exception;
                });
        return s3mock;
    }
}
