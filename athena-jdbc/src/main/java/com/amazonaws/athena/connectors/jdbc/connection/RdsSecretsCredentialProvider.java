/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates RDS secrets deserialization. AWS Secrets Manager managed RDS credentials are stored in following JSON format (showing minimal required for extraction):
 * <code>
 * {
 *     "username": "${user}",
 *     "password": "${password}"
 * }
 * </code>
 */
public class RdsSecretsCredentialProvider
        implements JdbcCredentialProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RdsSecretsCredentialProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final JdbcCredential jdbcCredential;

    public RdsSecretsCredentialProvider(final String secretString)
    {
        Map<String, String> rdsSecrets;
        try {
            rdsSecrets = OBJECT_MAPPER.readValue(secretString, HashMap.class);
        }
        catch (IOException ioException) {
            throw new RuntimeException("Could not deserialize RDS credentials into HashMap", ioException);
        }

        this.jdbcCredential = new JdbcCredential(rdsSecrets.get("username"), rdsSecrets.get("password"));
    }

    @Override
    public JdbcCredential getCredential()
    {
        return this.jdbcCredential;
    }
}
