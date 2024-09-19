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
package com.amazonaws.athena.connector.credentials;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.services.glue.model.ErrorDetails;
import com.amazonaws.services.glue.model.FederationSourceErrorCode;
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
public class DefaultCredentialsProvider
        implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCredentialsProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DefaultCredentials defaultCredentials;

    public DefaultCredentialsProvider(final String secretString)
    {
        Map<String, String> rdsSecrets;
        try {
            Map<String, String> originalMap = OBJECT_MAPPER.readValue(secretString, HashMap.class);

            rdsSecrets = new HashMap<>();
            for (Map.Entry<String, String> entry : originalMap.entrySet()) {
                if (entry.getKey().equalsIgnoreCase("username") || entry.getKey().equalsIgnoreCase("password")) {
                    rdsSecrets.put(entry.getKey().toLowerCase(), entry.getValue());
                }
            }
        }
        catch (IOException ioException) {
            throw new AthenaConnectorException("Could not deserialize RDS credentials into HashMap: ",
                    new ErrorDetails().withErrorCode(FederationSourceErrorCode.InternalServiceException.toString()).withErrorMessage(ioException.getMessage()));
        }

        this.defaultCredentials = new DefaultCredentials(rdsSecrets.get("username"), rdsSecrets.get("password"));
    }

    @Override
    public DefaultCredentials getCredential()
    {
        return this.defaultCredentials;
    }
}
