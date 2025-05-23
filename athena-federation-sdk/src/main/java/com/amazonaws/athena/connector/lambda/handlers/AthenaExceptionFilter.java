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
package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import software.amazon.awssdk.services.athena.model.TooManyRequestsException;

public class AthenaExceptionFilter
        implements ThrottlingInvoker.ExceptionFilter
{
    public static final ThrottlingInvoker.ExceptionFilter ATHENA_EXCEPTION_FILTER = new AthenaExceptionFilter();

    private AthenaExceptionFilter() {}

    @Override
    public boolean isMatch(Exception ex)
    {
        return ex instanceof TooManyRequestsException;
    }
}
