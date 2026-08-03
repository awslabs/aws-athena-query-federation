/*-
 * #%L
 * Athena MSK Connector
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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.athena.connectors.msk.dto.Message;
import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
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
import java.util.*;

import static com.amazonaws.athena.connectors.msk.AmazonMskUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class AmazonMskUtilsTest {
    private static final String SASL_SSL = "SASL_SSL";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_MECHANISM = "sasl.mechanism";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String AUTH_TYPE = "auth_type";
    private static final String NAME = "name";
    private static final String DATA_FORMAT = "dataFormat";
    private static final String ADMIN = "admin";
    private static final String TEST = "test";
    private static final String KEY_PASS = "keypass";
    private static final String TRUST_PASS = "trustpass";
    private static final String SSL_PASS = "sslpass";
    private static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    @Mock
    SecretsManagerClient awsSecretsManager;

    @Mock
    GetSecretValueResponse secretValueResponse;

    @Mock
    BasicAWSCredentials credentials;

    @Mock
    S3Client amazonS3Client;

    final java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
        "glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-Kafka",
        "secret_manager_kafka_creds_name", "testSecret",
        "kafka_endpoint", "12.207.18.179:9092",
        "certificates_s3_reference", "s3://kafka-connector-test-bucket/kafkafiles/",
        "secrets_manager_secret", "Kafka_afq");
    private MockedConstruction<DefaultAWSCredentialsProviderChain> mockedDefaultCredentials;
    private MockedStatic<S3Client> mockedS3ClientBuilder;
    private MockedStatic<SecretsManagerClient> mockedSecretsManagerClient;

    @Before
    public void init() {
        System.setProperty("aws.region", "us-west-2");
        System.setProperty("aws.accessKeyId", "xxyyyioyuu");
        System.setProperty("aws.secretKey", "vamsajdsjkl");
        mockedSecretsManagerClient = Mockito.mockStatic(SecretsManagerClient.class);
        mockedSecretsManagerClient.when(SecretsManagerClient::create).thenReturn(awsSecretsManager);
        mockedS3ClientBuilder = Mockito.mockStatic(S3Client.class);
        mockedS3ClientBuilder.when(S3Client::create).thenReturn(amazonS3Client);


        String creds = "{\"username\":\"" + ADMIN + "\",\"password\":\"" + TEST + "\",\"keystore_password\":\"" + KEY_PASS + "\",\"truststore_password\":\"" + TRUST_PASS + "\",\"ssl_key_password\":\"" + SSL_PASS + "\"}";

        Map<String, Object> map = new HashMap<>();
        map.put("username", ADMIN);
        map.put("password", TEST);
        map.put("keystore_password", KEY_PASS);
        map.put("truststore_password", TRUST_PASS);
        map.put("ssl_key_password", SSL_PASS);

        Mockito.when(secretValueResponse.secretString()).thenReturn(creds);
        Mockito.when(awsSecretsManager.getSecretValue(Mockito.isA(GetSecretValueRequest.class))).thenReturn(secretValueResponse);
        mockedDefaultCredentials = Mockito.mockConstruction(DefaultAWSCredentialsProviderChain.class,
                (mock, context) -> Mockito.when(mock.getCredentials()).thenReturn(credentials));
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
    public void getKafkaProperties_whenSaslScramSha512_returnsScramAuthProperties() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, AmazonMskUtils.AuthType.SASL_SSL_SCRAM_SHA512.toString());
        String sasljaasconfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + ADMIN + "\" password=\"" + TEST + "\";";
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals(SASL_SSL, properties.get(SECURITY_PROTOCOL));
        assertEquals("SCRAM-SHA-512", properties.get(SASL_MECHANISM));
        assertEquals(sasljaasconfig, properties.get(SASL_JAAS_CONFIG));
    }

    @Test
    public void getKafkaProperties_whenIamAuth_returnsIamAuthProperties() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, AmazonMskUtils.AuthType.SASL_SSL_AWS_MSK_IAM.toString());
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals(SASL_SSL, properties.get(SECURITY_PROTOCOL));
        assertEquals("AWS_MSK_IAM", properties.get(SASL_MECHANISM));
        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", properties.get(SASL_JAAS_CONFIG));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", properties.get("sasl.client.callback.handler.class"));
    }

    @Test
    public void getKafkaProperties_whenSslAuth_returnsSslProperties() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, AmazonMskUtils.AuthType.SSL.toString());
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals("SSL", properties.get(SECURITY_PROTOCOL));
        assertEquals(KEY_PASS, properties.get("ssl.keystore.password"));
        assertEquals(SSL_PASS, properties.get("ssl.key.password"));
        assertEquals(TRUST_PASS, properties.get(SSL_TRUSTSTORE_PASSWORD));
    }

    @Test
    public void toArrowType_whenGivenTypes_returnsCorrectArrowTypes(){
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
    public void getKafkaConsumer_whenSchemaProvided_returnsConsumer() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, AmazonMskUtils.AuthType.NO_AUTH.toString());
        Field field = new Field(NAME, FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put(DATA_FORMAT, "json");
        Schema schema= new Schema(List.of(field), metadataSchema);
        Consumer<String, TopicResultSet> consumer = AmazonMskUtils.getKafkaConsumer(schema, testConfigOptions);
        assertNotNull(consumer);
    }

    @Test
    public void getKafkaConsumer_whenNoSchema_returnsConsumer() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, AmazonMskUtils.AuthType.NO_AUTH.toString());
        Consumer<String, String> consumer = AmazonMskUtils.getKafkaConsumer(testConfigOptions);
        assertNotNull(consumer);
    }

    @Test
    public void createSplitParam_whenValidParams_returnsSplitParameters() {
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
    public void getKafkaProperties_whenUnknownAuthType_throwsRuntimeException() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, "UNKNOWN");
        getKafkaProperties(testConfigOptions);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getKafkaProperties_whenEmptyConfig_throwsIllegalArgumentException() throws Exception {
        getKafkaProperties(com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void getKafkaConsumer_whenCsvDataFormat_returnsConsumer() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, AmazonMskUtils.AuthType.NO_AUTH.toString());

        Field field = new Field(NAME, FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put(DATA_FORMAT, Message.DATA_FORMAT_CSV);
        Schema schema = new Schema(List.of(field), metadataSchema);

        Consumer<String, TopicResultSet> consumer = AmazonMskUtils.getKafkaConsumer(schema, testConfigOptions);
        assertNotNull(consumer);
    }

    @Test(expected = Exception.class)
    public void getKafkaConsumer_whenUnsupportedDataFormat_throwsException() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put(AUTH_TYPE, AmazonMskUtils.AuthType.NO_AUTH.toString());

        Field field = new Field(NAME, FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put(DATA_FORMAT, "xml");
        Schema schema = new Schema(List.of(field), metadataSchema);

        AmazonMskUtils.getKafkaConsumer(schema, testConfigOptions);
    }

    @Test
    public void getKafkaProperties_whenSaslPlain_setsSaslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(AmazonMskUtils.AuthType.SASL_PLAINTEXT_PLAIN.toString());
        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, "SASL_PLAINTEXT", formatJaasConfig());
        verifyNoSslProperties(properties);
    }

    @Test
    public void getKafkaProperties_whenSaslSsl_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(AmazonMskUtils.AuthType.SASL_SSL_PLAIN.toString());
        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, formatJaasConfig());

        assertNotNull(properties.get("ssl.truststore.location"));
        assertNotNull(properties.get(SSL_TRUSTSTORE_PASSWORD));
        assertEquals(TRUST_PASS, properties.get(SSL_TRUSTSTORE_PASSWORD));
    }

    @Test
    public void getKafkaProperties_whenSaslSslWithoutCertificates_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(AmazonMskUtils.AuthType.SASL_SSL_PLAIN.toString());
        testConfigOptions.remove(AmazonMskConstants.CERTIFICATES_S3_REFERENCE); // Remove S3 reference

        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, formatJaasConfig());
        verifyNoSslProperties(properties);
    }

    @Test
    public void getKafkaProperties_whenSaslSslEmptyCertificates_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(AmazonMskUtils.AuthType.SASL_SSL_PLAIN.toString());
        testConfigOptions.put(AmazonMskConstants.CERTIFICATES_S3_REFERENCE, ""); // Empty S3 reference

        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, formatJaasConfig());
        verifyNoSslProperties(properties);
    }

    @Test
    public void getKafkaProperties_whenSaslSslNullCertificates_setsSaslSslProperties() throws Exception {
        Map<String, String> testConfigOptions = createTestConfigOptions(AmazonMskUtils.AuthType.SASL_SSL_PLAIN.toString());
        testConfigOptions.put(AmazonMskConstants.CERTIFICATES_S3_REFERENCE, null); // Null S3 reference

        Properties properties = getKafkaProperties(testConfigOptions);

        verifyCommonSaslProperties(properties, SASL_SSL, formatJaasConfig());
        verifyNoSslProperties(properties);
    }

    private String formatJaasConfig() {
        return String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", ADMIN, TEST);
    }

    private Map<String, String> createTestConfigOptions(String authType) {
        Map<String, String> testConfigOptions = new HashMap<>(configOptions);
        testConfigOptions.put(AmazonMskConstants.AUTH_TYPE, authType);
        return testConfigOptions;
    }

    private void verifyCommonSaslProperties(Properties properties, String expectedProtocol, String expectedJaasConfig) {
        assertEquals(expectedProtocol, properties.get(SECURITY_PROTOCOL));
        assertEquals("PLAIN", properties.get(SASL_MECHANISM));
        assertEquals(expectedJaasConfig, properties.get(SASL_JAAS_CONFIG));
    }

    private void verifyNoSslProperties(Properties properties) {
        assertNull(properties.get("ssl.truststore.location"));
        assertNull(properties.get(SSL_TRUSTSTORE_PASSWORD));
    }
}
