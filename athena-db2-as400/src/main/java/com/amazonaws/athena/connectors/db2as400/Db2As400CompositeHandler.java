/*-
 * #%L
 * athena-db2-as400
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.db2as400;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose {@link Db2As400MetadataHandler} and {@link Db2As400RecordHandler}.
 *
 * Recommend using {@link Db2As400MuxCompositeHandler} instead.
 */
public class Db2As400CompositeHandler extends CompositeHandler
{
    public Db2As400CompositeHandler()
    {
        super(new Db2As400MetadataHandler(System.getenv()), new Db2As400RecordHandler(System.getenv()));
    }
}
