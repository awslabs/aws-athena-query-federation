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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        AWSCredentials credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().
                withCredentials(new AWSStaticCredentialsProvider(credentials)).
                build();

        String s3uri = getRequiredConfig(KERBEROS_CONFIG_FILES_S3_REFERENCE, configOptions);
        String[] s3Bucket = s3uri.split("s3://")[1].split("/");

        ObjectListing objectListing = s3Client.listObjects(s3Bucket[0], s3Bucket[1]);

        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            S3Object object = s3Client.getObject(new GetObjectRequest(s3Bucket[0], objectSummary.getKey()));
            InputStream inputStream = new BufferedInputStream(object.getObjectContent());
            String key = objectSummary.getKey();
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
