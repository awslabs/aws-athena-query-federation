/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class HbaseKerberosUtils
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseKerberosUtils.class);

    /**
     * For Kerberos authentication, user need to give S3 bucket reference where the config
     * files are uploaded.
     */
    public static final String KERBEROS_CONFIG_FILES_S3_REFERENCE = "kerberos_config_files_s3_reference";
    /**
     * This is temp folder where the kerberos config files from S3 will be downloaded
     */
    public static final String TEMP_DIR = "/tmp";
    public static final String PRINCIPAL_NAME = "principal_name";
    public static final String HBASE_RPC_PROTECTION = "hbase_rpc_protection";
    public static final String KERBEROS_AUTH_ENABLED = "kerberos_auth_enabled";
    public static final String HBASE_SECURITY_AUTHENTICATION = "kerberos";

    private HbaseKerberosUtils() {}

    /**
     * Downloads the config files from S3 to temp directory.
     *
     * @throws Exception - {@link Exception}
     */
    public static Path copyConfigFilesFromS3ToTempFolder(java.util.Map<String, String> configOptions) throws Exception
    {
        logger.debug("Creating the connection with AWS S3 for copying config files to Temp Folder");
        Path tempDir = getTempDirPath();
        // create() Uses default credentials chain
        S3Client s3Client = S3Client.create();

        String s3uri = getRequiredConfig(KERBEROS_CONFIG_FILES_S3_REFERENCE, configOptions);
        String[] s3Bucket = s3uri.split("s3://")[1].split("/");

        ListObjectsResponse objectListing = s3Client.listObjects(ListObjectsRequest.builder()
                .bucket(s3Bucket[0])
                .prefix(s3Bucket[1])
                .build());

        for (S3Object s3Object : objectListing.contents()) {
            ResponseInputStream<GetObjectResponse> responseStream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(s3Bucket[0])
                    .key(s3Object.key())
                    .build());
            InputStream inputStream = new BufferedInputStream(responseStream);
            String key = s3Object.key();
            String fName = key.substring(key.indexOf('/') + 1);
            if (!fName.isEmpty()) {
                File file = new File(tempDir + File.separator + fName);
                Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return tempDir;
    }

    /**
     * Creates the temp directory to put the kerberos auth config files.
     *
     * @return {@link Path} Temp directory path
     */
    private static Path getTempDirPath()
    {
        Path tmpPath = Paths.get(TEMP_DIR).toAbsolutePath();
        File filePath = new File(tmpPath + File.separator + "hbasekerberosconfigs");
        if (filePath.exists()) {
            return filePath.toPath();
        }
        boolean isCreated = filePath.mkdirs();
        logger.info("Is new directory created? " + isCreated);
        return filePath.toPath();
    }

    /**
     * Gets the environment variable.
     *
     * @param key - the config key
     * @return {@link String}
     */
    private static String getRequiredConfig(String key, java.util.Map<String, String> configOptions)
    {
        String value = configOptions.getOrDefault(key, "");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Lambda Environment Variable " + key + " has not been populated! ");
        }
        return value;
    }
}
