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

import com.amazonaws.athena.connectors.msk.dto.SplitParameters;
import com.amazonaws.athena.connectors.msk.dto.TopicResultSet;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
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
import java.io.FileWriter;
import java.util.*;

import static com.amazonaws.athena.connectors.msk.AmazonMskUtils.*;
import static org.junit.Assert.*;
import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class AmazonMskUtilsTest {
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
    DefaultAWSCredentialsProviderChain chain;

    @Mock
    AWSStaticCredentialsProvider credentialsProvider;

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
    public void init() throws Exception {
        System.setProperty("aws.region", "us-west-2");
        System.setProperty("aws.accessKeyId", "xxyyyioyuu");
        System.setProperty("aws.secretKey", "vamsajdsjkl");
        mockedSecretsManagerClient = Mockito.mockStatic(SecretsManagerClient.class);
        mockedSecretsManagerClient.when(()-> SecretsManagerClient.create()).thenReturn(awsSecretsManager);
        mockedS3ClientBuilder = Mockito.mockStatic(S3Client.class);
        mockedS3ClientBuilder.when(()-> S3Client.create()).thenReturn(amazonS3Client);


        String creds = "{\"username\":\"admin\",\"password\":\"test\",\"keystore_password\":\"keypass\",\"truststore_password\":\"trustpass\",\"ssl_key_password\":\"sslpass\"}";

        Map<String, Object> map = new HashMap<>();
        map.put("username", "admin");
        map.put("password", "test");
        map.put("keystore_password", "keypass");
        map.put("truststore_password", "trustpass");
        map.put("ssl_key_password", "sslpass");

        Mockito.when(secretValueResponse.secretString()).thenReturn(creds);
        Mockito.when(awsSecretsManager.getSecretValue(Mockito.isA(GetSecretValueRequest.class))).thenReturn(secretValueResponse);
        mockedDefaultCredentials = Mockito.mockConstruction(DefaultAWSCredentialsProviderChain.class,
                (mock, context) -> {
                    Mockito.when(mock.getCredentials()).thenReturn(credentials);
                });
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
        testConfigOptions.put("auth_type", AmazonMskUtils.AuthType.SASL_SSL_SCRAM_SHA512.toString());
        String sasljaasconfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"test\";";
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals("SASL_SSL", properties.get("security.protocol"));
        assertEquals("SCRAM-SHA-512", properties.get("sasl.mechanism"));
        assertEquals(sasljaasconfig, properties.get("sasl.jaas.config"));
    }

    @Test
    public void testGetIAMAuthKafkaProperties() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", AmazonMskUtils.AuthType.SASL_SSL_AWS_MSK_IAM.toString());
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals("SASL_SSL", properties.get("security.protocol"));
        assertEquals("AWS_MSK_IAM", properties.get("sasl.mechanism"));
        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", properties.get("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", properties.get("sasl.client.callback.handler.class"));
    }

    @Test
    public void testGetSSLAuthKafkaProperties() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", AmazonMskUtils.AuthType.SSL.toString());
        Properties properties = getKafkaProperties(testConfigOptions);
        assertEquals("SSL", properties.get("security.protocol"));
        assertEquals("keypass", properties.get("ssl.keystore.password"));
        assertEquals("sslpass", properties.get("ssl.key.password"));
        assertEquals("trustpass", properties.get("ssl.truststore.password"));
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
        testConfigOptions.put("auth_type", AmazonMskUtils.AuthType.NO_AUTH.toString());
        Field field = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "json");
        Schema schema= new Schema(asList(field), metadataSchema);
        Consumer<String, TopicResultSet> consumer = AmazonMskUtils.getKafkaConsumer(schema, testConfigOptions);
        assertNotNull(consumer);
    }

    @Test
    public void testGetKafkaConsumer() throws Exception {
        java.util.HashMap testConfigOptions = new java.util.HashMap(configOptions);
        testConfigOptions.put("auth_type", AmazonMskUtils.AuthType.NO_AUTH.toString());
        Consumer<String, String> consumer = AmazonMskUtils.getKafkaConsumer(testConfigOptions);
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
}
