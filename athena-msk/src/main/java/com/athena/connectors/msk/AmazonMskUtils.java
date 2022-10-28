/*-
 * #%L
 * athena-msk
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
package com.athena.connectors.msk;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.model.SchemaListItem;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.athena.connectors.msk.trino.MskTrinoQueryExecutor;
import com.athena.connectors.msk.trino.QueryExecutor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.kafka.KafkaPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AmazonMskUtils
{
    private static final String KEYSTORE = "kafka.client.keystore.jks";
    private static final String TRUSTSTORE = "kafka.client.truststore.jks";
    private static final String CATALOG_NAME = "kafka";
    private static final String CONNECTOR_NAME = "kafka";
    private static final String KAFKA_NODE = "kafka.nodes";
    private static final String KAFKA_TABLENAME = "kafka.table-names";
    private static final String KAFKA_SECURITY_PROTOCOL = "kafka.security-protocol";
    private static final String KAFKA_TRUSTSTORE_LOCATION = "kafka.ssl.truststore.location";
    private static final String KAFKA_KEYSTORE_LOCATION = "kafka.ssl.keystore.location";
    private static final String KAFKA_KEYSTORE_PASSWORD = "kafka.ssl.keystore.password";
    private static final String KAFKA_TRUSTSTORE_PASSWORD = "kafka.ssl.truststore.password";
    private static final String KAFKA_SSL_KEY_PASSWORD = "kafka.ssl.key.password";
    private static final String KAFKA_SPLIT = "kafka.messages-per-split";
    private static final String KAFKA_TABLE_DESCRIPTION_DIR = "kafka.table-description-dir";
    private static final String NO_AUTH = "NOAUTH";
    private static final String AUTH_TLS = "TLS";
    private static final String AUTH_SCRAM = "SCRAM";
    private static final String AUTH_IAM = "SERVERLESS";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonMskUtils.class);
    private static GlueRegistryReader glueRegistryReader;
    private static QueryExecutor queryExecutor;

    private AmazonMskUtils()
    {
    }

    public static QueryExecutor getQueryExecutor()
    {
        Path tempDir = initializeDependenciesAndReturnTempFolderPath();
        String glueRegistryArnValue = getEnvVar(AmazonMskConstants.GLUE_REGISTRY_ARN);
        String tableNames = String.join(",", getSchemaName(glueRegistryArnValue));
        LOGGER.info("table names: " + tableNames);
        String authType = getEnvVar(AmazonMskConstants.AUTH_TYPE).toUpperCase().trim();

        switch (authType) {
            case NO_AUTH:
                return getQueryExecutorForNoAuth(tempDir, tableNames);
            case AUTH_TLS:
                return getQueryExecutorForTLS(tempDir, tableNames);
            case AUTH_SCRAM:
                /**
                 * kafka property file need to be updated with cluster associated username and password
                 * for authentication
                 */
                updateScramPropertyFile(tempDir);
                return getQueryExecutorForScramOrIAM(tempDir, tableNames, true, "kafka-configuration-scram.properties");
            case AUTH_IAM:
                return getQueryExecutorForScramOrIAM(tempDir, tableNames, false, "kafka-configuration.properties");
            default:
                LOGGER.error("Unsupported Authentication type {}", authType);
                throw new RuntimeException("Unsupported Authentication type" + authType);
        }
    }

    /**
     * This method will create temp directory to store the keystore,truststore and glue schema
     * Initialize the glue registry and query runner
     *
     * @return temp directory
     */
    private static Path initializeDependenciesAndReturnTempFolderPath()
    {
        glueRegistryReader = new GlueRegistryReader();
        queryExecutor = new MskTrinoQueryExecutor(createSession());
        KafkaPlugin mskPlugin = new KafkaPlugin();
        queryExecutor.installPlugin(mskPlugin);

        Path tempDir = getTempDirPath();
        copyGlueSchemaToTempDir(tempDir);
        return tempDir;
    }

    /**
     * Create connection for authentication type TLS
     *
     * @param tempDir  Temp directory where truststore, keystore and glue registry data are present
     * @param fileName Table names on which operation is performed.
     * @return QueryExecutor
     */
    private static QueryExecutor getQueryExecutorForTLS(Path tempDir, String fileName)
    {
        try {
            Map<String, Object> cred = getCredentialsAsKeyValue();
            LOGGER.debug("Creating the connection with AWS S3");
            AWSCredentials credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().
                    withCredentials(new AWSStaticCredentialsProvider(credentials)).
                    build();

            copyDataToTempFolderFromS3(s3Client, tempDir);

            Map<String, String> mskConfig = getMSKConfigForTLS(cred, tempDir, fileName);

            queryExecutor.createCatalog(CATALOG_NAME, CONNECTOR_NAME, mskConfig);

            LOGGER.info("STEP 1: AWS MSK queryExecutor object created" + queryExecutor);
        }
        catch (Exception exception) {
            System.out.println("Error occurred in  AmazonMskUtils.getQueryExecutor() " + exception.getMessage());
        }
        return queryExecutor;
    }

    /*for kafka query runner */
    private static QueryExecutor getQueryExecutorForNoAuth(Path tempDir, String tableNames)
    {
        try {
            Map<String, String> mskConfig = getKafkaConfig(tempDir, tableNames);
            queryExecutor.createCatalog(CATALOG_NAME, CONNECTOR_NAME, mskConfig);
            LOGGER.info("STEP 1: Kafka queryExecutor object created" + queryExecutor);
        }
        catch (Exception exception) {
            LOGGER.error("Error occurred in  AmazonMskUtils.getQueryExecutorForNoAuth() " + exception.getMessage());
        }

        return queryExecutor;
    }

    /**
     * @param tempDir
     * @param fileName
     * @param isScramReq
     * @param propertyFile
     * @return
     */
    private static QueryExecutor getQueryExecutorForScramOrIAM(Path tempDir, String fileName, boolean isScramReq, String propertyFile)
    {
        try {
            queryExecutor.createCatalog(CATALOG_NAME, CONNECTOR_NAME, getMSKConfigForScramOrIAM(tempDir, isScramReq, propertyFile, fileName));

            LOGGER.info("STEP 1: AWS MSK queryExecutor object created" + queryExecutor);
        }
        catch (Exception exception) {
            System.out.println("Error occurred in  AmazonMskUtils.getQueryExecutorForScramOrIAM() " + exception.getMessage());
        }
        return queryExecutor;
    }

    private static File getResourcePath()
    {
        ClassLoader classLoader = AmazonMskUtils.class.getClassLoader();
        return new File(requireNonNull(classLoader.getResource("")).getFile());
    }

    static Session createSession()
    {
        return Session.builder(new SessionPropertyManager())
                .setIdentity(Identity.ofUser(AmazonMskConstants.KAFKA_IDENTITY))
                .setSource(AmazonMskConstants.KAFKA_SOURCE)
                .setCatalog(AmazonMskConstants.KAFKA_CATALOG)
                .setSchema(AmazonMskConstants.KAFKA_SCHEMA)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setQueryId(new QueryId(AmazonMskConstants.KAFKA_QUERY_ID))
                .build();
    }

    /**
     * @param tempDir copy schema to the temp directory and create file with the name as schema name
     */
    private static void copyGlueSchemaToTempDir(Path tempDir)
    {
        Map<String, Object> schemaAsMap;
        String schemaRegistryARN = getEnvVar(AmazonMskConstants.GLUE_REGISTRY_ARN);
        try {
            for (String arn : getTopicsSchemaArnList(schemaRegistryARN)) {
                schemaAsMap = glueRegistryReader.getGlueSchema(arn, new TypeReference<Map<String, Object>>()
                {
                });

                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(schemaAsMap);
                BufferedWriter bw = null;
                try {
                    bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempDir + schemaAsMap.get("tableName").toString() + ".json", true), StandardCharsets.UTF_8));
                    bw.write(json);
                }
                finally {
                    bw.close();
                }
//                file.close();
            }
        }
        catch (IOException ex) {
            LOGGER.info("IOException occurred in  AmazonMskUtils.copyGlueSchemaToTempDir() " + ex.getMessage());
        }
        catch (Exception ex) {
            LOGGER.info("Error occurred in  AmazonMskUtils.copyGlueSchemaToTempDir() " + ex.getMessage());
        }
    }

    /**
     * Configuration of the MSK CLuster for TLS
     *
     * @param cred     Credentials of keystore, truststore and ssl key
     * @param tempDir  Temp directory where truststore, keystore and glue registry data are present
     * @param fileName Table names
     * @return Configuration for TLS connection
     */
    protected static Map<String, String> getMSKConfigForTLS(Map<String, Object> cred, Path tempDir, String fileName)
    {
        if (null != tempDir.getParent()) {
            return new ImmutableMap.Builder<String, String>()
                    .put(KAFKA_NODE, getEnvVar(AmazonMskConstants.ENV_KAFKA_NODE))
                    .put(KAFKA_TABLENAME, fileName)
                    .put(KAFKA_SECURITY_PROTOCOL, "SSL")
                    .put(KAFKA_TRUSTSTORE_LOCATION, tempDir + File.separator + TRUSTSTORE)
                    .put(KAFKA_KEYSTORE_LOCATION, tempDir + File.separator + KEYSTORE)
                    .put(KAFKA_KEYSTORE_PASSWORD, cred.get(AmazonMskConstants.KEYSTORE_PASSWORD).toString())
                    .put(KAFKA_TRUSTSTORE_PASSWORD, cred.get(AmazonMskConstants.TRUSTSTORE_PASSWORD).toString())
                    .put(KAFKA_SSL_KEY_PASSWORD, cred.get(AmazonMskConstants.SSL_PASSWORD).toString())
                    .put(KAFKA_SPLIT, "1000")
                    .put(KAFKA_TABLE_DESCRIPTION_DIR, String.valueOf(tempDir.getParent()))
                    .build();
        }
        return null;
    }

    /**
     * Configuration of the MSK Cluster for No AUTH
     *
     * @param tempDir    Temp directory where truststore, keystore and glue registry data are present
     * @param tableNames Table names
     * @return Configuration for No Auth connection
     */
    private static Map<String, String> getKafkaConfig(Path tempDir, String tableNames)
    {
        return new ImmutableMap.Builder<String, String>()
                .put(KAFKA_NODE, getEnvVar(AmazonMskConstants.ENV_KAFKA_NODE))
                .put(KAFKA_TABLENAME, tableNames)
                .put(KAFKA_SPLIT, "1000")
                .put(KAFKA_TABLE_DESCRIPTION_DIR, String.valueOf(tempDir.getParent()))
                .build();
    }

    /**
     * Configuration of the MSK Cluster for SCRAM/IAM Auth
     *
     * @param tempDir      Temp directory where truststore, keystore and glue registry data are present
     * @param isScramReq   Check whether request for SCRAM OR IAM Auth
     * @param propertyFile Property file of SCRAM or IAM Auth
     * @param fileName     Table names
     * @return Configuration for SCRAM/IAM Auth connection
     */
    private static Map<String, String> getMSKConfigForScramOrIAM(Path tempDir, boolean isScramReq, String propertyFile, String fileName)
    {
        //Check with property file is required on the basis of isScramReq
        String configPath = isScramReq ? tempDir + File.separator + propertyFile :
                getResourcePath().getAbsolutePath() + File.separator + propertyFile;

        return new ImmutableMap.Builder<String, String>()
                .put(KAFKA_NODE, getEnvVar(AmazonMskConstants.ENV_KAFKA_NODE))
                .put(KAFKA_TABLENAME, fileName)
                .put("kafka.config.resources", configPath)
                .put(KAFKA_SPLIT, "1000")
                .put(KAFKA_TABLE_DESCRIPTION_DIR, String.valueOf(tempDir.getParent()))
                .build();
    }

    /**
     * Updates the SCRAM property file with username and password
     *
     * @param tempDir Temp directory where truststore, keystore and glue registry data are present
     */
    private static void updateScramPropertyFile(Path tempDir)
    {
        String path = tempDir + File.separator + "kafka-configuration-scram.properties";
        Properties prop = new Properties();
        try (InputStream inputStream = AmazonMskUtils.class.getResourceAsStream("/kafka-configuration-scram.properties");
             FileOutputStream outputStream = new FileOutputStream(path, false)) {
            prop.load(inputStream);
            Map<String, Object> cred = getCredentialsAsKeyValue();
            String user = cred.get(AmazonMskConstants.SCRAM_USERNAME).toString();
            String pwd = cred.get(AmazonMskConstants.SCRAM_PWD).toString();
            prop.setProperty(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=" + user + " password=" + pwd + ";");

            prop.store(outputStream, null);
        }
        catch (NullPointerException ex) {
            System.out.println("NullPointerException occurred in  AmazonMskUtils.updateScramPropertyFile() " + ex.getMessage());
        }
        catch (IOException ex) {
            System.out.println("IOException occurred in  AmazonMskUtils.updateScramPropertyFile() " + ex.getMessage());
        }
    }

    /**
     * Retrieve all the schemas from glue registry
     *
     * @return List of all the schemas
     */
    private static List<String> getSchemaName(String glueRegistryArn)
    {
        List<String> schemaName = new ArrayList<>();

        glueRegistryReader.getSchemaListItemsWithSchemaRegistryARN(glueRegistryArn).stream().map(SchemaListItem::getSchemaName)
                .forEach(schemaName::add);

        return schemaName;
    }

    /**
     * Copy truststore, keystore from S3 to temp directory
     *
     * @param s3client Object of the AmazonS3 client to connect with S3 bucket
     * @param tempDir  Temp directory where truststore, keystore and glue registry data are present
     * @throws IOException
     */
    protected static void copyDataToTempFolderFromS3(AmazonS3 s3client, Path tempDir) throws IOException
    {
        try {
            String s3uri = getEnvVar(AmazonMskConstants.CERTIFICATES_S3_REFERENCE);
            String[] s3Bucket = s3uri.split("s3://")[1].split("/");

            ObjectListing objectListing = s3client.listObjects(s3Bucket[0], s3Bucket[1]);

            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                S3Object object = s3client.getObject(new GetObjectRequest(s3Bucket[0], objectSummary.getKey()));
                InputStream inputStream = new BufferedInputStream(object.getObjectContent());
                String key = objectSummary.getKey();
                String fName = key.substring(key.indexOf('/') + 1);
                if (!fName.isEmpty()) {
                    File file = new File(tempDir + File.separator + fName);
                    Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
        catch (AmazonServiceException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * Create the temp directory path
     *
     * @return Temp directory path
     */
    private static Path getTempDirPath()
    {
        Path tmpPath = Paths.get(AmazonMskConstants.TEMP_DIR).toAbsolutePath();
        File filePath = new File(tmpPath + File.separator + "MSKData");
        boolean isCreated = filePath.mkdirs();
        LOGGER.info("Is new directory created? " + isCreated);
        return filePath.toPath();
    }

    private static List<String> getTopicsSchemaArnList(String glueRegistryARN)
    {
        List<String> arnList = new ArrayList<>();
        glueRegistryReader.getSchemaListItemsWithSchemaRegistryARN(glueRegistryARN).
                stream().map(SchemaListItem::getSchemaArn).forEach(arnList::add);
        return arnList;
    }

    /**
     * Retrieve credentials from AWS Secret Manager
     *
     * @return Map of Credentials from AWS Secret Manager
     * @throws JsonProcessingException
     */
    private static Map<String, Object> getCredentialsAsKeyValue() throws JsonProcessingException
    {
        AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.defaultClient();
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest();
        getSecretValueRequest.setSecretId(getEnvVar(AmazonMskConstants.SECRET_MANAGER_MSK_CREDS_NAME));
        GetSecretValueResult response = secretsManager.getSecretValue(getSecretValueRequest);
        LOGGER.info("response: {}", response);
        return new ObjectMapper().readValue(response.getSecretString(), new TypeReference<Map<String, Object>>()
        {
        });
    }

    private static String getEnvVar(String envVar)
    {
        String envVariable = System.getenv(envVar);
        if (envVariable == null || envVariable.length() == 0) {
            throw new IllegalArgumentException("Lambda Environment Variable " + envVar + " has not been populated! ");
        }
        return envVariable;
    }
}
