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

import com.amazonaws.athena.connectors.gcs.storage.StorageSplit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_NAME;
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

    public static void printJson(Object object, String prefix)
    {
        if (true) {
            return;
        }
        LOGGER.info("Printing json for {}", prefix);
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            LOGGER.info(prefix + ":\n{}", mapper.writeValueAsString(object));
        }
        catch (Exception exception) {
            // ignored
            LOGGER.error("Unable to print JSON for {}. Error: {}", prefix, exception.getMessage());
        }
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
        File dest = new File(Paths.get("/tmp").toAbsolutePath() + File.separator  + "cacert.pem");
        if (!dest.exists()) {
            Files.copy(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static void installGoogleCredentialsJsonFile(String gcsCredentialJsonString) throws IOException
    {
        File dest = new File(Paths.get("/tmp").toAbsolutePath() + File.separator  + GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_NAME);
        if (dest.exists()) {
            return;
        }
        try (OutputStream out = new FileOutputStream(dest)) {
            out.write(gcsCredentialJsonString.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }
}
