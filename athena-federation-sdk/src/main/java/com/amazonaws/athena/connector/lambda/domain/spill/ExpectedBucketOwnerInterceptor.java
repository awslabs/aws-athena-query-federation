/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Adds x-amz-expected-bucket-owner to every request from the spill S3 client, so a write to a bucket
 * owned by any other account fails with 403 instead of landing there.
 */
public final class ExpectedBucketOwnerInterceptor
        implements ExecutionInterceptor
{
    static final String EXPECTED_BUCKET_OWNER_HEADER = "x-amz-expected-bucket-owner";
    private static final Pattern ACCOUNT_ID = Pattern.compile("\\d{12}");

    private final String bucketOwnerAccount;

    public ExpectedBucketOwnerInterceptor(String bucketOwnerAccount)
    {
        requireNonNull(bucketOwnerAccount, "bucketOwnerAccount was null");
        if (!ACCOUNT_ID.matcher(bucketOwnerAccount).matches()) {
            throw new IllegalArgumentException("bucketOwnerAccount must be a 12-digit AWS account id");
        }
        this.bucketOwnerAccount = bucketOwnerAccount;
    }

    @Override
    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes)
    {
        return context.httpRequest().toBuilder()
                .putHeader(EXPECTED_BUCKET_OWNER_HEADER, bucketOwnerAccount)
                .build();
    }
}
