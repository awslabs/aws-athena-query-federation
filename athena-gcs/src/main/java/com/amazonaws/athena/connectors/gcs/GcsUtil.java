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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_CREDENTIAL_KEYS_ENV_VAR;
import static java.util.Objects.requireNonNull;

public class GcsUtil
{
    private GcsUtil()
    {
    }

    /**
     * Retrieves the GCS credential JSON from the JSON (key/value pairs)
     *
     * @param secretString String from the Secrets Manager
     * @return GCS credentials JSON
     */
    public static String getGcsCredentialJsonString(final String secretString)
    {
        String appCredentialsJsonString = null;
        try {
            if (secretString != null) {
                TypeReference<HashMap<String, String>> typeRef
                        = new TypeReference<>()
                {
                };
                ObjectMapper mapper = new ObjectMapper();
                Map<String, String> secretKeys = mapper.readValue(secretString.getBytes(StandardCharsets.UTF_8), typeRef);
                appCredentialsJsonString = secretKeys.get(System.getenv(GCS_CREDENTIAL_KEYS_ENV_VAR));
            }
        }
        catch (Throwable throwable) {
            throw new GcsConnectorException("Unable to retrieve JSON string for GCS credential", throwable);
        }
        return requireNonNull(appCredentialsJsonString, "GCS credential was null using key "
                + GCS_CREDENTIAL_KEYS_ENV_VAR
                + " in the secret " + System.getenv(GCS_CREDENTIAL_KEYS_ENV_VAR));
    }
}
