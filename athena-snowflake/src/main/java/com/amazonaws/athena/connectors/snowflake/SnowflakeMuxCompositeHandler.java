

/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcCompositeHandler;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeUtils.installCaCertificate;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeUtils.setupNativeEnvironmentVariables;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose {@link SnowflakeMuxMetadataHandler} and {@link SnowflakeMuxRecordHandler}.
 */
public class SnowflakeMuxCompositeHandler
        extends MultiplexingJdbcCompositeHandler
{
    public SnowflakeMuxCompositeHandler() throws ReflectiveOperationException, CertificateEncodingException, IOException, NoSuchAlgorithmException, KeyStoreException
    {
        super(SnowflakeMuxMetadataHandler.class, SnowflakeMuxRecordHandler.class, SnowflakeMetadataHandler.class, SnowflakeRecordHandler.class);
        installCaCertificate();
        setupNativeEnvironmentVariables();
    }
}
