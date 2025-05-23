/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data.
 * Automatically chooses to use either the Mux*Handlers or normal *Handlers
 * depending on if catalog connection strings are being used.
 */
public class MultiplexingJdbcCompositeHandler
        extends CompositeHandler
{
    private static final boolean hasCatalogConnections = System.getenv().keySet().stream().anyMatch(k ->
        k.endsWith(com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfigBuilder.CONNECTION_STRING_PROPERTY_SUFFIX));

    public MultiplexingJdbcCompositeHandler(
        Class<? extends MultiplexingJdbcMetadataHandler> muxMetadataHandlerClass,
        Class<? extends MultiplexingJdbcRecordHandler> muxRecordHandlerClass,
        Class<? extends JdbcMetadataHandler> metadataHandlerClass,
        Class<? extends JdbcRecordHandler> recordHandlerClass) throws java.lang.ReflectiveOperationException
    {
        super(
            hasCatalogConnections ?
                muxMetadataHandlerClass.getConstructor(java.util.Map.class).newInstance(System.getenv()) :
                metadataHandlerClass.getConstructor(java.util.Map.class).newInstance(System.getenv()),
            hasCatalogConnections ?
                muxRecordHandlerClass.getConstructor(java.util.Map.class).newInstance(System.getenv()) :
                recordHandlerClass.getConstructor(java.util.Map.class).newInstance(System.getenv()));
    }
}
