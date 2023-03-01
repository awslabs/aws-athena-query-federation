/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

import java.util.HashMap;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose DynamoDBMetadataHandler and DynamoDBRecordHandler.
 */
public class DynamoDBCompositeHandler
        extends CompositeHandler
{
    public DynamoDBCompositeHandler()
    {
        // System.getenv returns an unmodifiable map, so wrap it in a modifiable map, so we can set configs dynamically on lambda execution.
        super(new DynamoDBMetadataHandler(new HashMap<>(System.getenv())), new DynamoDBRecordHandler(new HashMap<>(System.getenv())));
    }
}
