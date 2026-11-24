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

import org.junit.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpectedBucketOwnerInterceptorTest
{
    @Test
    public void addsExpectedBucketOwnerHeader()
    {
        SdkHttpRequest request = SdkHttpRequest.builder()
                .method(SdkHttpMethod.PUT)
                .uri(URI.create("https://spill-bucket.s3.us-east-1.amazonaws.com/prefix/key"))
                .build();
        Context.ModifyHttpRequest context = mock(Context.ModifyHttpRequest.class);
        when(context.httpRequest()).thenReturn(request);

        SdkHttpRequest result = new ExpectedBucketOwnerInterceptor("123456789012")
                .modifyHttpRequest(context, new ExecutionAttributes());

        assertEquals("123456789012",
                result.firstMatchingHeader(ExpectedBucketOwnerInterceptor.EXPECTED_BUCKET_OWNER_HEADER).get());
    }

    @Test
    public void rejectsNonAccountId()
    {
        assertThrows(IllegalArgumentException.class, () -> new ExpectedBucketOwnerInterceptor("12345"));
        assertThrows(IllegalArgumentException.class, () -> new ExpectedBucketOwnerInterceptor("12345678901a"));
        assertThrows(IllegalArgumentException.class, () -> new ExpectedBucketOwnerInterceptor(" 123456789012"));
        assertThrows(NullPointerException.class, () -> new ExpectedBucketOwnerInterceptor(null));
    }
}
