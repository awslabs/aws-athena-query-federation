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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connectors.gcs.common.StorageSplit;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.Arrays;
import java.util.List;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.SSL_CERT_FILE_LOCATION;
import static java.util.Objects.requireNonNull;

public class GcsUtil
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    static List<String> supportedTypeList = Arrays.asList("parquet", "csv");

    private GcsUtil()
    {
    }

    /**
     * Builds a string representation of an instance of {@link StorageSplit}
     *
     * @param splits An instance of {@link StorageSplit}
     * @return String representation of an instance of {@link StorageSplit}
     * @throws JsonProcessingException If JSON processing error happens
     */
    public static synchronized String splitAsJson(List<StorageSplit> splits) throws JsonProcessingException
    {
        return objectMapper.writeValueAsString(splits);
    }

    public static boolean isFieldTypeNull(Field field)
    {
        return field.getType() == null
                || field.getType().equals(Types.MinorType.NULL.getType());
    }

    /**
     * Install cacert from resource folder to temp location
     * This is required for dataset api
     */
    public static void installCaCertificate() throws IOException
    {
        ClassLoader classLoader = GcsRecordHandler.class.getClassLoader();
        File file = new File(requireNonNull(classLoader.getResource("")).getFile());
        File src = new File(file.getAbsolutePath() + File.separator + "cacert.pem");
        File destination = new File(System.getenv(SSL_CERT_FILE_LOCATION));
        File parentDestination = new File(destination.getParent());
        if (!destination.exists()) {
            if (!parentDestination.exists()) {
                parentDestination.mkdirs();
            }
            Files.copy(src.toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Install/place Google cloud platform credentials from AWS secret manager to temp location
     * This is required for dataset api
     */
    public static void installGoogleCredentialsJsonFile() throws IOException
    {
        CachableSecretsManager secretsManager = new CachableSecretsManager(AWSSecretsManagerClientBuilder.defaultClient());
        String gcsCredentialsJsonString = secretsManager.getSecret(System.getenv(GCS_SECRET_KEY_ENV_VAR));
        File destination = new File(System.getenv(GOOGLE_SERVICE_ACCOUNT_JSON_TEMP_FILE_LOCATION));
        boolean destinationDirExists = new File(destination.getParent()).mkdirs();
        if (!destinationDirExists && destination.exists()) {
            return;
        }
        try (OutputStream out = new FileOutputStream(destination)) {
            out.write(gcsCredentialsJsonString.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }

    /**
     * return whether file format supported or not
     *
     * @param classification file format
     * @return boolean is format supported
     */
    public static boolean isSupportedFileType(String classification)
    {
        return supportedTypeList.stream().anyMatch(str -> str.trim().equalsIgnoreCase(classification));
    }

    /**
     * Builds a GCS uri
     *
     * @param bucketName bucket name
     * @param objectNames folder path
     * @return String representation uri
     */
    public static String createUri(String bucketName, String objectNames)
    {
        return "gs://" + bucketName + "/" + objectNames;
    }

    /**
     * Builds a GCS uri
     *
     * @param path bucket path
     * @return String representation uri
     */
    public static String createUri(String path)
    {
        return "gs://"  + path;
    }

    /**
     * Get AWS Glue table object
     *
     * @param tableName table info
     * @param awsGlue AWS Glue client
     * @return Table object
     */
    public static Table getGlueTable(TableName tableName, AWSGlue awsGlue)
    {
        com.amazonaws.services.glue.model.GetTableRequest getTableRequest = new com.amazonaws.services.glue.model.GetTableRequest();
        getTableRequest.setDatabaseName(tableName.getSchemaName());
        getTableRequest.setName(tableName.getTableName());

        GetTableResult result = awsGlue.getTable(getTableRequest);
        return result.getTable();
    }
}
