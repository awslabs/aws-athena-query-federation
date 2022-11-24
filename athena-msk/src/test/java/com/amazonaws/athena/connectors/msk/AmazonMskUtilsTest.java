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
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.ListSchemasResult;
import com.amazonaws.services.glue.model.SchemaListItem;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import uk.org.webcompere.systemstubs.rules.EnvironmentVariablesRule;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.*;

import static com.amazonaws.athena.connectors.msk.AmazonMskUtils.*;
import static org.junit.Assert.*;
import static java.util.Arrays.asList;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*","org.w3c.*","javax.net.ssl.*","sun.security.*","jdk.internal.reflect.*","javax.crypto.*"})
@PrepareForTest({AWSGlueClientBuilder.class, AWSSecretsManagerClientBuilder.class,
        AWSStaticCredentialsProvider.class, DefaultAWSCredentialsProviderChain.class, AmazonS3ClientBuilder.class,
        ListObjectsRequest.class, FileOutputStream.class, Properties.class})
public class AmazonMskUtilsTest {
    @Mock
    FileWriter fileWriter;

    @Mock
    ObjectMapper objectMapper;

    @Rule
    public EnvironmentVariablesRule environmentVariables = new EnvironmentVariablesRule();

    @Mock
    AWSSecretsManager awsSecretsManager;

    @Mock
    GetSecretValueRequest secretValueRequest;

    @Mock
    GetSecretValueResult secretValueResult;

    @Mock
    DefaultAWSCredentialsProviderChain chain;

    @Mock
    AWSStaticCredentialsProvider credentialsProvider;

    @Mock
    BasicAWSCredentials credentials;

    @Mock
    AmazonS3Client amazonS3Client;

    @Mock
    AmazonS3ClientBuilder clientBuilder;

    @Mock
    ObjectListing oList;

    @Mock
    AWSGlue awsGlue;


    @Before
    public void init() throws Exception {
        System.setProperty("aws.region", "us-west-2");
        System.setProperty("aws.accessKeyId", "xxyyyioyuu");
        System.setProperty("aws.secretKey", "vamsajdsjkl");
        environmentVariables.set("glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-MSK");
        environmentVariables.set("secret_manager_msk_creds_name", "testSecret");
        environmentVariables.set("kafka_endpoint", "12.207.18.179:9092");
        environmentVariables.set("certificates_s3_reference", "s3://msk-connector-test-bucket/mskfiles/");
        environmentVariables.set("secrets_manager_secret", "AmazonMSK_afq");
        PowerMockito.whenNew(ObjectMapper.class).withNoArguments().thenReturn(objectMapper);
        String json = "{}";
        Mockito.when(objectMapper.writeValueAsString(Mockito.any(Map.class))).thenReturn(json);
        PowerMockito.whenNew(FileWriter.class).withAnyArguments().thenReturn(fileWriter);
        PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(awsSecretsManager);
        PowerMockito.whenNew(GetSecretValueRequest.class).withNoArguments().thenReturn(secretValueRequest);

        String creds = "{\"username\":\"admin\",\"password\":\"test\",\"keystore_password\":\"keypass\",\"truststore_password\":\"trustpass\",\"ssl_password\":\"sslpass\"}";

        Map<String, Object> map = new HashMap<>();
        map.put("username", "admin");
        map.put("password", "test");
        map.put("keystore_password", "keypass");
        map.put("truststore_password", "trustpass");
        map.put("ssl_password", "sslpass");

        Mockito.when(secretValueResult.getSecretString()).thenReturn(creds);
        Mockito.when(awsSecretsManager.getSecretValue(Mockito.isA(GetSecretValueRequest.class))).thenReturn(secretValueResult);

        Mockito.doReturn(map).when(objectMapper).readValue(Mockito.eq(creds), Mockito.any(TypeReference.class));
        PowerMockito.whenNew(DefaultAWSCredentialsProviderChain.class).withNoArguments().thenReturn(chain);
        Mockito.when(chain.getCredentials()).thenReturn(credentials);

        PowerMockito.mockStatic(AmazonS3ClientBuilder.class);
        PowerMockito.when(AmazonS3ClientBuilder.standard()).thenReturn(clientBuilder);
        PowerMockito.whenNew(AWSStaticCredentialsProvider.class).withArguments(credentials).thenReturn(credentialsProvider);
        Mockito.doReturn(clientBuilder).when(clientBuilder).withCredentials(Mockito.any());
        Mockito.when(clientBuilder.build()).thenReturn(amazonS3Client);
        Mockito.when(amazonS3Client.listObjects(Mockito.any(), Mockito.any())).thenReturn(oList);
        S3Object s3Obj = new S3Object();
        s3Obj.setObjectContent(new ByteArrayInputStream("largeContentFile".getBytes()));
        Mockito.when(amazonS3Client.getObject(Mockito.any())).thenReturn(s3Obj);
        S3ObjectSummary s3 = new S3ObjectSummary();
        s3.setKey("test/key");
        Mockito.when(oList.getObjectSummaries()).thenReturn(List.of(s3));
    }

    @Test
    public void testGetScramAuthKafkaProperties() throws Exception {
        environmentVariables.set("auth_type", "SCRAM");
        String sasljaasconfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"test\";";
        Properties properties = getKafkaProperties();
        assertEquals("SASL_SSL", properties.get("security.protocol"));
        assertEquals("SCRAM-SHA-512", properties.get("sasl.mechanism"));
        assertEquals(sasljaasconfig, properties.get("sasl.jaas.config"));
    }

    @Test
    public void testGetIAMAuthKafkaProperties() throws Exception {
        environmentVariables.set("auth_type", "IAM");
        Properties properties = getKafkaProperties();
        assertEquals("SASL_SSL", properties.get("security.protocol"));
        assertEquals("AWS_MSK_IAM", properties.get("sasl.mechanism"));
        assertEquals("software.amazon.msk.auth.iam.IAMLoginModule required;", properties.get("sasl.jaas.config"));
        assertEquals("software.amazon.msk.auth.iam.IAMClientCallbackHandler", properties.get("sasl.client.callback.handler.class"));
    }

    @Test
    public void testGetTLSAuthKafkaProperties() throws Exception {
        environmentVariables.set("auth_type", "TLS");
        Properties properties = getKafkaProperties();
        assertEquals("SSL", properties.get("security.protocol"));
        assertEquals("keypass", properties.get("ssl.keystore.password"));
        assertEquals("sslpass", properties.get("ssl.key.password"));
        assertEquals("trustpass", properties.get("ssl.truststore.password"));
    }

    @Test
    public void testGetTopicListFromGlueRegistry() throws Exception {
        List<SchemaListItem> schemaListItems = new ArrayList<>();
        ListSchemasResult listSchemasResult = new ListSchemasResult();
        SchemaListItem schemaListItem = new SchemaListItem();
        schemaListItem.setSchemaName("testtable");
        schemaListItems.add(schemaListItem);
        listSchemasResult.setSchemas(schemaListItems);
        PowerMockito.mockStatic(AWSGlueClientBuilder.class);
        PowerMockito.when(AWSGlueClientBuilder.defaultClient()).thenReturn(awsGlue);
        PowerMockito.when(awsGlue.listSchemas(Mockito.any())).thenReturn(listSchemasResult);
        List<String> topicList = AmazonMskUtils.getTopicListFromGlueRegistry();
        assertEquals("testtable", topicList.get(0));
    }

    @Test
    public void testToArrowType(){
        assertEquals(new ArrowType.Bool(), toArrowType("BOOLEAN"));
        assertEquals(Types.MinorType.TINYINT.getType(), toArrowType("TINYINT"));
        assertEquals(Types.MinorType.SMALLINT.getType(), toArrowType("SMALLINT"));
        assertEquals(Types.MinorType.INT.getType(), toArrowType("INT"));
        assertEquals(Types.MinorType.INT.getType(), toArrowType("INTEGER"));
        assertEquals(Types.MinorType.BIGINT.getType(), toArrowType("BIGINT"));
        assertEquals(Types.MinorType.FLOAT8.getType(), toArrowType("FLOAT"));
        assertEquals(Types.MinorType.FLOAT8.getType(), toArrowType("DOUBLE"));
        assertEquals(Types.MinorType.FLOAT8.getType(), toArrowType("DECIMAL"));
        assertEquals(Types.MinorType.DATEDAY.getType(), toArrowType("DATE"));
        assertEquals(Types.MinorType.DATEMILLI.getType(), toArrowType("TIMESTAMP"));
        assertEquals(Types.MinorType.VARCHAR.getType(), toArrowType("UNSUPPORTED"));
    }

    @Test
    public void testGetKafkaConsumerWithSchema() throws Exception {
        environmentVariables.set("auth_type", "NOAUTH");
        Field field = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "json");
        Schema schema= new Schema(asList(field), metadataSchema);
        Consumer<String, TopicResultSet> consumer = AmazonMskUtils.getKafkaConsumer(schema);
        assertNotNull(consumer);
    }

    @Test
    public void testGetKafkaConsumer() throws Exception {
        environmentVariables.set("auth_type", "NOAUTH");
        Consumer<String, String> consumer = AmazonMskUtils.getKafkaConsumer();
        assertNotNull(consumer);
    }

    @Test
    public void testCreateSplitParam() {
        Map<String, String> params = Map.of(
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
        environmentVariables.set("auth_type", "UNKNOWN");
        getKafkaProperties();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetKafkaPropertiesForIllegalArgumentException() throws Exception {
        environmentVariables = new EnvironmentVariablesRule();
        getKafkaProperties();
    }
}
