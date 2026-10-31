/*-
 * #%L
 * athena-larkbase
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
package com.amazonaws.athena.connectors.lark.base;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import org.apache.arrow.util.VisibleForTesting;

import java.io.IOException;

/**
 * This class is the entry point for the Athena Lambda function. It is used to create the CompositeHandler
 * which is the glue between the metadata and record handling logic.
 * <p>
 * This class is used to create the CompositeHandler with the BaseMetadataHandler and BaseRecordHandler.
 * The BaseMetadataHandler is used to handle metadata requests and the BaseRecordHandler is used to handle
 * record requests.
 */
public class BaseCompositeHandler
        extends CompositeHandler
{
    public BaseCompositeHandler() throws IOException
    {
        super(new BaseMetadataHandler(new EnvironmentProperties().createEnvironment()), new BaseRecordHandler(new EnvironmentProperties().createEnvironment()));
    }

    /**
     * Constructor for testing purposes that accepts pre-configured handlers.
     *
     * @param metadataHandler The metadata handler to use
     * @param recordHandler   The record handler to use
     */
    @VisibleForTesting
    protected BaseCompositeHandler(BaseMetadataHandler metadataHandler, BaseRecordHandler recordHandler)
    {
        super(metadataHandler, recordHandler);
    }
}
