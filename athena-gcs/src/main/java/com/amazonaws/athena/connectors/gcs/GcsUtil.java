/*-
 * #%L
 * Amazon Athena GCS Connector
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
package com.amazonaws.athena.connectors.gcs;

import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_CREDENTIAL_KEYS_ENV_VAR;
import static java.util.Objects.requireNonNull;

public class GcsUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsUtil.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private GcsUtil()
    {
    }

    /**
     * Retrieves the GCS credential JSON from the JSON (key/value pairs)
     *
     * @param secretString String from the Secrets Manager
     * @return GCS credentials JSON
     */
    public static String getGcsCredentialJsonString(final String secretString) throws IOException
    {
        String appCredentialsJsonString = null;
        if (secretString != null) {
            TypeReference<HashMap<String, String>> typeRef
                    = new TypeReference<>()
            {
            };
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> secretKeys = mapper.readValue(secretString.getBytes(StandardCharsets.UTF_8), typeRef);
            appCredentialsJsonString = secretKeys.get(System.getenv(GCS_CREDENTIAL_KEYS_ENV_VAR));
        }
        return requireNonNull(appCredentialsJsonString, "GCS credential was null using key "
                + GCS_CREDENTIAL_KEYS_ENV_VAR
                + " in the secret " + System.getenv(GCS_CREDENTIAL_KEYS_ENV_VAR));
    }

    /**
     * Builds a string representation of an instance of {@link StorageSplit}
     *
     * @param split An instance of {@link StorageSplit}
     * @return String representation of an instance of {@link StorageSplit}
     * @throws JsonProcessingException If JSON processing error happens
     */
    public static synchronized String splitAsJson(StorageSplit split) throws JsonProcessingException
    {
        return objectMapper.writeValueAsString(split);
    }

    public static void printJson(Object object, String prefix)
    {
        LOGGER.info("Printing json for {}", prefix);
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            LOGGER.info(prefix + ":%n{}", object);
        }
        catch (Exception exception) {
            // ignored
            LOGGER.error("Unable to print JSON for {}. Error: {}", prefix, exception.getMessage());
        }
    }
}
