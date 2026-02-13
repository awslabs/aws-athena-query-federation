/*-
 * #%L
 * Athena Kafka Connector
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
package com.amazonaws.athena.connectors.kafka;

import com.amazonaws.athena.connectors.kafka.dto.SplitParameters;
import com.amazonaws.athena.connectors.kafka.dto.TopicResultSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.util.*;

import static com.amazonaws.athena.connectors.kafka.KafkaUtils.*;
import static org.junit.Assert.*;
import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class KafkaUtilsTest {
    private static final String TEST_USERNAME = "admin";
    private static final String TEST_PASSWORD = "test";
    private static final String TEST_KEYSTORE_PASSWORD = "keypass";
    private static final String TEST_TRUSTSTORE_PASSWORD = "trustpass";
    private static final String TEST_SSL_KEY_PASSWORD = "sslpass";
    private static final String DIFFERENT_USERNAME = "testuser";
    private static final String DIFFERENT_PASSWORD = "testpass";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_MECHANISM = "sasl.mechanism";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    private static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    private static final String SASL_SSL = "SASL_SSL";
    private static final String SCRAM_SHA_512 = "SCRAM-SHA-512";
    private static final String PLAIN = "PLAIN";

    private static final String SCRAM_JAAS_TEMPLATE = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    private static final String PLAIN_JAAS_TEMPLATE = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";

    @Mock
    FileWriter fileWriter;

    @Mock
    ObjectMapper objectMapper;

    @Mock
    SecretsManagerClient awsSecretsManager;

    @Mock
    GetSecretValueRequest secretValueRequest;

    @Mock
    GetSecretValueResponse secretValueResponse;

    @Mock
    DefaultCredentialsProvider chain;

    @Mock
    StaticCredentialsProvider credentialsProvider;

    @Mock
    AwsBasicCredentials credentials;

    @Mock
    S3Client amazonS3Client;


    final java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
        "glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-Kafka",
        "secret_manager_kafka_creds_name", "testSecret",
        "kafka_endpoint", "12.207.18.179:9092",
        "certificates_s3_reference", "s3://kafka-connector-test-bucket/kafkafiles/",
        "secrets_manager_secret", "Kafka_afq");

    private MockedConstruction<DefaultCredentialsProvider> mockedDefaultCredentials;
    private MockedStatic<S3Client> mockedS3ClientBuilder;
    private MockedStatic<SecretsManagerClient> mockedSecretsManagerClient;


    @Before
    public void init() {
        System.setProperty("aws.region", "us-west-2");
        System.setProperty("aws.accessKeyId", "xxyyyioyuu");
        System.setProperty("aws.secretKey", "vamsajdsjkl");

        mockedSecretsManagerClient = Mockito.mockStatic(SecretsManagerClient.class);
        mockedSecretsManagerClient.when(()-> SecretsManagerClient.create()).thenReturn(awsSecretsManager);

        String creds = createCredentialsJson(TEST_USERNAME, TEST_PASSWORD);

        Map<String, Object> map = new HashMap<>();
        map.put("username", TEST_USERNAME);
        map.put("password", TEST_PASSWORD);
        map.put("keystore_password", TEST_KEYSTORE_PASSWORD);
        map.put("truststore_password", TEST_TRUSTSTORE_PASSWORD);
        map.put("ssl_key_password", TEST_SSL_KEY_PASSWORD);

        Mockito.when(secretValueResponse.secretString()).thenReturn(creds);
        Mockito.when(awsSecretsManager.getSecretValue(Mockito.isA(GetSecretValueRequest.class))).thenReturn(secretValueResponse);

        mockedDefaultCredentials = Mockito.mockConstruction(DefaultCredentialsProvider.class,
                (mock, context) -> {
                    Mockito.when(mock.resolveCredentials()).thenReturn(credentials);
                });
        mockedS3ClientBuilder = Mockito.mockStatic(S3Client.class);
        mockedS3ClientBuilder.when(()-> S3Client.create()).thenReturn(amazonS3Client);

        S3Object s3 = S3Object.builder().key("test/key").build();
        Mockito.when(amazonS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(ListObjectsResponse.builder()
                .contents(s3)
                .build());
        Mockito.when(amazonS3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream("largeContentFile".getBytes())));
    }
    @After
    public void tearDown() {
        mockedDefaultCredentials.close();
        mockedS3ClientBuilder.close();
        mockedSecretsManagerClient.close();
    }

    @Test
    public void testGetScramAuthKafkaProperties() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", KafkaUtils.AuthType.SASL_SSL_SCRAM_SHA512.toString());
        String sasljaasconfig = formatJaasConfig(true, TEST_USERNAME, TEST_PASSWORD);
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals(SASL_SSL, properties.get(SECURITY_PROTOCOL));
        assertEquals(SCRAM_SHA_512, properties.get(SASL_MECHANISM));
        assertEquals(sasljaasconfig, properties.get(SASL_JAAS_CONFIG));
    }

    @Test
    public void testGetSSLAuthKafkaProperties() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", KafkaUtils.AuthType.SSL.toString());
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals("SSL", properties.get(SECURITY_PROTOCOL));
        assertEquals(TEST_KEYSTORE_PASSWORD, properties.get("ssl.keystore.password"));
        assertEquals(TEST_SSL_KEY_PASSWORD, properties.get("ssl.key.password"));
        assertEquals(TEST_TRUSTSTORE_PASSWORD, properties.get(SSL_TRUSTSTORE_PASSWORD));
    }

    @Test
    public void testToArrowType(){
        assertEquals(new ArrowType.Bool(), toArrowType("BOOLEAN"));
        assertEquals(Types.MinorType.TINYINT.getType(), toArrowType("TINYINT"));
        assertEquals(Types.MinorType.SMALLINT.getType(), toArrowType("SMALLINT"));
        assertEquals(Types.MinorType.INT.getType(), toArrowType("INT"));
        assertEquals(Types.MinorType.INT.getType(), toArrowType("INTEGER"));
        assertEquals(Types.MinorType.BIGINT.getType(), toArrowType("BIGINT"));
        assertEquals(Types.MinorType.FLOAT4.getType(), toArrowType("FLOAT"));
        assertEquals(Types.MinorType.FLOAT8.getType(), toArrowType("DOUBLE"));
        assertEquals(Types.MinorType.FLOAT8.getType(), toArrowType("DECIMAL"));
        assertEquals(Types.MinorType.DATEDAY.getType(), toArrowType("DATE"));
        assertEquals(Types.MinorType.DATEMILLI.getType(), toArrowType("TIMESTAMP"));
        assertEquals(Types.MinorType.VARCHAR.getType(), toArrowType("UNSUPPORTED"));
    }

    @Test
    public void testGetKafkaConsumerWithSchema() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", KafkaUtils.AuthType.NO_AUTH.toString());
        Field field = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "json");
        Schema schema= new Schema(asList(field), metadataSchema);
        Consumer<String, TopicResultSet> consumer = KafkaUtils.getKafkaConsumer(schema, testConfigOptions);
        assertNotNull(consumer);
    }

    @Test
    public void testGetKafkaConsumer() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", KafkaUtils.AuthType.NO_AUTH.toString());
        Consumer<String, String> consumer = KafkaUtils.getKafkaConsumer(testConfigOptions);
        assertNotNull(consumer);
    }

    @Test
    public void testCreateSplitParam() {
        Map<String, String> params = com.google.common.collect.ImmutableMap.of(
                SplitParameters.TOPIC, "testTopic",
                SplitParameters.PARTITION, "0",
                SplitParameters.START_OFFSET, "0",
                SplitParameters.END_OFFSET, "100"
        );
        SplitParameters splitParameters = createSplitParam(params);
        assertEquals("testTopic", splitParameters.topic);
        assertEquals(100L, splitParameters.endOffset);
    }

    @Test(expected = RuntimeException.class)
    public void testGetKafkaPropertiesForRuntimeException() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", "UNKNOWN");
        getKafkaProperties(testConfigOptions);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetKafkaPropertiesForIllegalArgumentException() throws Exception {
        getKafkaProperties(com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void setAuthKafkaProperties_withScramPlainText_setsScramProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(KafkaUtils.AuthType.SASL_PLAINTEXT_SCRAM_SHA512.toString());
        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_PLAINTEXT, SCRAM_SHA_512, formatJaasConfig(true, TEST_USERNAME, TEST_PASSWORD));
        verifyNoSslProperties(properties);
    }

    @Test
    public void setAuthKafkaProperties_withSaslPlain_setsSaslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(KafkaUtils.AuthType.SASL_PLAINTEXT_PLAIN.toString());
        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_PLAINTEXT, PLAIN, formatJaasConfig(false, TEST_USERNAME, TEST_PASSWORD));
        verifyNoSslProperties(properties);
    }

    @Test
    public void setAuthKafkaProperties_withSaslSsl_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(KafkaUtils.AuthType.SASL_SSL_PLAIN.toString());
        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, PLAIN, formatJaasConfig(false, TEST_USERNAME, TEST_PASSWORD));

        assertNotNull(properties.get(SSL_TRUSTSTORE_LOCATION));
        assertNotNull(properties.get(SSL_TRUSTSTORE_PASSWORD));
        assertEquals(TEST_TRUSTSTORE_PASSWORD, properties.get(SSL_TRUSTSTORE_PASSWORD));
    }

    @Test
    public void setAuthKafkaProperties_withSaslSslWithoutCertificates_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(KafkaUtils.AuthType.SASL_SSL_PLAIN.toString());
        testConfigOptions.remove(KafkaConstants.CERTIFICATES_S3_REFERENCE); // Remove S3 reference

        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, PLAIN, formatJaasConfig(false, TEST_USERNAME, TEST_PASSWORD));
        verifyNoSslProperties(properties);
    }

    @Test
    public void setAuthKafkaProperties_withSaslSslEmptyCertificates_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(KafkaUtils.AuthType.SASL_SSL_PLAIN.toString());
        testConfigOptions.put(KafkaConstants.CERTIFICATES_S3_REFERENCE, ""); // Empty S3 reference

        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, PLAIN, formatJaasConfig(false, TEST_USERNAME, TEST_PASSWORD));
        verifyNoSslProperties(properties);
    }

    @Test
    public void setAuthKafkaProperties_withSaslSslNullCertificates_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(KafkaUtils.AuthType.SASL_SSL_PLAIN.toString());
        testConfigOptions.put(KafkaConstants.CERTIFICATES_S3_REFERENCE, null); // Null S3 reference

        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, PLAIN, formatJaasConfig(false, TEST_USERNAME, TEST_PASSWORD));
        verifyNoSslProperties(properties);
    }

    @Test
    public void setAuthKafkaProperties_withDifferentCredentials_setsCorrectProperties() throws Exception {
        String differentCreds = createCredentialsJson(DIFFERENT_USERNAME, DIFFERENT_PASSWORD);
        Mockito.when(secretValueResponse.secretString()).thenReturn(differentCreds);

        // Test SCRAM PLAINTEXT
        Map<String, String> testConfigOptions = createTestConfigOptions(KafkaUtils.AuthType.SASL_PLAINTEXT_SCRAM_SHA512.toString());
        Properties properties = getKafkaProperties(testConfigOptions);
        verifyCommonSaslProperties(properties, SASL_PLAINTEXT, SCRAM_SHA_512, formatJaasConfig(true, DIFFERENT_USERNAME, DIFFERENT_PASSWORD));

        // Test SASL PLAIN
        testConfigOptions.put(KafkaConstants.AUTH_TYPE, KafkaUtils.AuthType.SASL_PLAINTEXT_PLAIN.toString());
        properties = getKafkaProperties(testConfigOptions);
        verifyCommonSaslProperties(properties, SASL_PLAINTEXT, PLAIN, formatJaasConfig(false, DIFFERENT_USERNAME, DIFFERENT_PASSWORD));

        // Test SASL SSL
        testConfigOptions.put(KafkaConstants.AUTH_TYPE, KafkaUtils.AuthType.SASL_SSL_PLAIN.toString());
        properties = getKafkaProperties(testConfigOptions);
        verifyCommonSaslProperties(properties, SASL_SSL, PLAIN, formatJaasConfig(false, DIFFERENT_USERNAME, DIFFERENT_PASSWORD));
        assertEquals(TEST_TRUSTSTORE_PASSWORD, properties.get(SSL_TRUSTSTORE_PASSWORD));
    }

    private String createCredentialsJson(String username, String password) {
        return String.format("{\"username\":\"%s\",\"password\":\"%s\",\"keystore_password\":\"%s\",\"truststore_password\":\"%s\",\"ssl_key_password\":\"%s\"}",
                username, password, KafkaUtilsTest.TEST_KEYSTORE_PASSWORD, KafkaUtilsTest.TEST_TRUSTSTORE_PASSWORD, KafkaUtilsTest.TEST_SSL_KEY_PASSWORD);
    }

    private String formatJaasConfig(boolean isScram, String username, String password) {
        return String.format(isScram ? SCRAM_JAAS_TEMPLATE : PLAIN_JAAS_TEMPLATE, username, password);
    }

    private Map<String, String> createTestConfigOptions(String authType) {
        Map<String, String> testConfigOptions = new HashMap<>(configOptions);
        testConfigOptions.put(KafkaConstants.AUTH_TYPE, authType);
        return testConfigOptions;
    }

    private void verifyCommonSaslProperties(Properties properties, String expectedProtocol, String expectedMechanism, String expectedJaasConfig) {
        assertEquals(expectedProtocol, properties.get(SECURITY_PROTOCOL));
        assertEquals(expectedMechanism, properties.get(SASL_MECHANISM));
        assertEquals(expectedJaasConfig, properties.get(SASL_JAAS_CONFIG));
    }

    private void verifyNoSslProperties(Properties properties) {
        assertNull(properties.get(SSL_TRUSTSTORE_LOCATION));
        assertNull(properties.get(SSL_TRUSTSTORE_PASSWORD));
    }
}
