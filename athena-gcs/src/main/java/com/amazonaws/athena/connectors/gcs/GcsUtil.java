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

import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_CREDENTIAL_KEYS_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.SSL_CERT_FILE_LOCATION;
import static java.util.Objects.requireNonNull;

public class GcsUtil
{
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
    public static String getGcsCredentialJsonString(final String secretString, String gcsCredentialKeysEnvVar) throws IOException
    {
        String appCredentialsJsonString = null;
        if (secretString != null) {
            TypeReference<HashMap<String, String>> typeRef
                    = new TypeReference<>()
            {
            };
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> secretKeys = mapper.readValue(secretString.getBytes(StandardCharsets.UTF_8), typeRef);
            appCredentialsJsonString = secretKeys.get(System.getenv(gcsCredentialKeysEnvVar));
        }
        return requireNonNull(appCredentialsJsonString, "GCS credential was null using key "
                + gcsCredentialKeysEnvVar
                + " in the secret " + System.getenv(gcsCredentialKeysEnvVar));
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

    public static boolean isFieldTypeNull(Field field)
    {
        return field.getType() == null
                || field.getType().equals(Types.MinorType.NULL.getType());
    }

    public static void installCaCertificate() throws IOException
    {
        ClassLoader classLoader = GcsRecordHandler.class.getClassLoader();
        File file = new File(requireNonNull(classLoader.getResource("")).getFile());
        File src = new File(file.getAbsolutePath() + File.separator + "cacert.pem");
        File dest = new File(System.getenv(SSL_CERT_FILE_LOCATION));
        File parentDest = new File(dest.getParent());
        if (!dest.exists()) {
            if (!parentDest.exists()) {
                parentDest.mkdirs();
            }
            Files.copy(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static void installGoogleCredentialsJsonFile() throws IOException
    {
        CachableSecretsManager secretsManager = new CachableSecretsManager(AWSSecretsManagerClientBuilder.defaultClient());
        String gcsCredentialsJsonString = getGcsCredentialJsonString(secretsManager.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR)), GCS_CREDENTIAL_KEYS_ENV_VAR);
        File dest = new File(System.getenv(GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION));
        boolean destDirExists = new File(dest.getParent()).mkdirs();
        if (!destDirExists && dest.exists()) {
            return;
        }
        try (OutputStream out = new FileOutputStream(dest)) {
            out.write(gcsCredentialsJsonString.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }
}
