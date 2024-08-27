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

package com.amazonaws.athena.connector.lambda.domain.spill;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * This class is used to track the bucket and its state, and check its validity
 */
public class SpillLocationVerifier
{
    private static final Logger logger = LoggerFactory.getLogger(SpillLocationVerifier.class);

    private enum BucketState
    {UNCHECKED, VALID, INVALID}

    private final S3Client amazons3;
    private String bucket;
    private BucketState state;

    /**
     * @param amazons3 The S3 object for the account.
     */
    public SpillLocationVerifier(S3Client amazons3)
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
            amazons3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            state = BucketState.VALID;
        }
        catch (S3Exception ex) {
            int statusCode = ex.statusCode();
            // returns 404 if bucket was not found, 403 if bucket access is forbidden
            if (statusCode == 404 || statusCode == 403) {
                state = BucketState.INVALID;
            }
            else {
                throw new RuntimeException("Error while checking bucket ownership for " + bucket, ex);
            }
        }
        finally {
            logger.info("The state of bucket {} has been updated to {} from {}", bucket, state, BucketState.UNCHECKED);
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
