
/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.google.cloud.bigquery.BigQueryException;
import software.amazon.awssdk.services.athena.model.AthenaException;
public class BigQueryExceptionFilter implements ThrottlingInvoker.ExceptionFilter
{
    public static final ThrottlingInvoker.ExceptionFilter EXCEPTION_FILTER = new BigQueryExceptionFilter();

    @Override
    public boolean isMatch(Exception ex)
    {
        if (ex instanceof AthenaException && ex.getMessage().contains("Rate exceeded")) {
            return true;
        }
        if (ex instanceof BigQueryException && ex.getMessage().contains("Exceeded rate limits")) {
            return true;
        }
        return false;
    }
}
