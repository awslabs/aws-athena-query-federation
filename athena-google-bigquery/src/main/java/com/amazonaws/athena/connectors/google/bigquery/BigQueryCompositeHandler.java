
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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQuerySQLException;
import com.google.cloud.bigquery.JobException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;

import java.io.IOException;

import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.installGoogleCredentialsJsonFile;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.setupNativeEnvironmentVariables;

public class BigQueryCompositeHandler
        extends CompositeHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryCompositeHandler.class);
    //TODO: To be removed, once we expose a way to reuse the BufferAllocator created via the Federation SDK.
    private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    public BigQueryCompositeHandler()
            throws IOException
    {
        super(new BigQueryMetadataHandler(new BigQueryEnvironmentProperties().createEnvironment()), new BigQueryRecordHandler(new BigQueryEnvironmentProperties().createEnvironment(), allocator));
        installGoogleCredentialsJsonFile(new BigQueryEnvironmentProperties().createEnvironment());
        setupNativeEnvironmentVariables();
        logger.info("Inside BigQueryCompositeHandler()");
    }

    @Override
    public Exception handleException(Exception e)
    {
        return handleBigQueryException(super.handleException(e));
    }

    /**
     * Transforms BigQuery exceptions into AthenaConnectorException with appropriate error code.
     * Maps BigQuery error codes to FederationSourceErrorCode for better customer experience.
     * Returns other exceptions unchanged.
     *
     * @param e The exception to handle
     * @return The transformed exception or original exception
     */
    public static Exception handleBigQueryException(Exception e)
    {
        if (e instanceof BigQueryException || e instanceof BigQuerySQLException || e instanceof JobException) {
            int errorCode = 0;
            if (e instanceof BigQueryException) {
                errorCode = ((BigQueryException) e).getCode();
            }
            return new AthenaConnectorException(
                    e.getMessage() != null ? e.getMessage() : "",
                    ErrorDetails.builder()
                            .errorCode(mapHTTPErrorCode(errorCode).toString())
                            .build()
            );
        }
        return e;
    }
}
