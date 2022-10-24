package com.amazonaws.athena.connector.lambda.domain.spill;

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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
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
        List<Bucket> buckets = createBuckets(bucketNames);
        AmazonS3 mockS3 = createMockS3(buckets);
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

        String bucketNotOwn = "spill-bucket";

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

    private AmazonS3 createMockS3(List<Bucket> buckets)
    {
        AmazonS3 s3mock = mock(AmazonS3.class);
        when(s3mock.listBuckets()).thenReturn(buckets);
        return s3mock;
    }

    private List<Bucket> createBuckets(List<String> names)
    {
        List<Bucket> buckets = new ArrayList();
        for (String name : names) {
            Bucket bucket = mock(Bucket.class);
            when(bucket.getName()).thenReturn(name);
            buckets.add(bucket);
        }

        return buckets;
    }
}
