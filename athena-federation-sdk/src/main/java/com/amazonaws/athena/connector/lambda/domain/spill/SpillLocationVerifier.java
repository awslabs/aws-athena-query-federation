package com.amazonaws.athena.connector.lambda.domain.spill;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class is used to track the bucket and its state, and check its validity
 */
public class SpillLocationVerifier
{
    private static final Logger logger = LoggerFactory.getLogger(SpillLocationVerifier.class);

    private enum BucketState
    {UNCHECKED, VALID, INVALID}

    private final AmazonS3 amazons3;
    private String bucket;
    private BucketState state;

    /**
     * @param amazons3 The S3 object for the account.
     */
    public SpillLocationVerifier(AmazonS3 amazons3)
    {
        this.amazons3 = amazons3;
        this.bucket = null;
        this.state = BucketState.UNCHECKED;
    }

    /**
     * Public function used to check if the account that calls the lambda function owns the spill bucket
     *
     * @param spillBucket The name of the spill bucket.
     */
    public void checkBucketAuthZ(String spillBucket)
    {
        if (spillBucket == null || spillBucket.equals("")) {
            return;
        }

        if (bucket == null || !bucket.equals(spillBucket)) {
            logger.info("Spill bucket has been changed from {} to {}", bucket, spillBucket);
            bucket = spillBucket;
            state = BucketState.UNCHECKED;
        }

        if (state == BucketState.UNCHECKED) {
            updateBucketState();
        }

        passOrFail();
    }

    /**
     * Helper function to check bucket ownership if it hasn't been checked before
     *
     */
    @VisibleForTesting
    void updateBucketState()
    {
        try {
            Set<String> buckets = amazons3.listBuckets().stream().map(b -> b.getName()).collect(Collectors.toSet());

            if (!buckets.contains(bucket)) {
                state = BucketState.INVALID;
            }
            else {
                state = BucketState.VALID;
            }

            logger.info("The state of bucket {} has been updated to {} from {}", bucket, state, BucketState.UNCHECKED);
        }
        catch (AmazonS3Exception ex) {
            throw new RuntimeException("Error while checking bucket ownership for " + bucket, ex);
        }
    }

    /**
     * Helper function to throw exception if lambda function doesn't own the spill bucket
     *
     */
    @VisibleForTesting
    void passOrFail()
    {
        switch (state) {
            case UNCHECKED:
                throw new RuntimeException("Bucket state should have been checked already.");
            case INVALID:
                throw new RuntimeException(String.format("spill_bucket: \"%s\" not found under your account. Please make sure you have access to the bucket and spill_bucket input has no trailing '/'", bucket));
            default:
                return;
        }
    }
}
