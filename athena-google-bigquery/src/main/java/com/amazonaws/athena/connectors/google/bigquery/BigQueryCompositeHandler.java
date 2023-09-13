
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

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        super(new BigQueryMetadataHandler(System.getenv()), new BigQueryRecordHandler(System.getenv(), allocator));
        installGoogleCredentialsJsonFile(System.getenv());
        setupNativeEnvironmentVariables();
        logger.info("Inside BigQueryCompositeHandler()");
    }
}
