/*-
 * #%L
 * athena-msk
 * %%
 * Copyright (C) 2019 - 2022 Trianz
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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetSchemaResult;
import com.amazonaws.services.glue.model.GetSchemaVersionResult;
import com.amazonaws.services.glue.model.ListSchemasResult;
import com.amazonaws.services.glue.model.SchemaListItem;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.athena.connectors.msk.trino.MskTrinoQueryExecutor;
import com.athena.connectors.msk.trino.QueryExecutor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.Session;
import io.trino.plugin.kafka.KafkaPlugin;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
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
import java.nio.file.Path;
import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*","org.w3c.*","javax.net.ssl.*","sun.security.*","jdk.internal.reflect.*","javax.crypto.*"})
@PrepareForTest({ QueryExecutor.class , AWSGlueClientBuilder.class ,AWSSecretsManagerClientBuilder.class, AWSStaticCredentialsProvider.class, DefaultAWSCredentialsProviderChain.class, AmazonS3ClientBuilder.class, ListObjectsRequest.class, FileOutputStream.class, Properties.class})
public class AmazonMskUtilsTest {

    GlueRegistryReader glueRegistryReader = mock(GlueRegistryReader.class);

    @Mock
    FileWriter fileWriter;

    @Mock
    KafkaPlugin kafkaPlugin;

    @Mock
    ObjectMapper objectMapper;

    @Mock
    QueryExecutor queryExecutor;

    @Mock
    AWSGlue awsGlue;

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
    ObjectListing objectListing;

    @Mock
    ListObjectsRequest listObjectsRequest;

    @Mock
    AmazonS3ClientBuilder clientBuilder;

    @Mock
    Properties prop;

    @Mock
    FileOutputStream fos;

    @Mock
    ObjectListing oList;

    @Before
    public void init()
    {
        environmentVariables.set("aws.region", "us-east-1");
        environmentVariables.set("arn_value", "testArn");
        environmentVariables.set("glue_registry_arn", "arn:aws:glue:us-west-2:430676967608:registry/Athena-MSK");
        environmentVariables.set("auth_type", "NOAUTH");
        environmentVariables.set("secret_manager_msk_creds_name", "testSecret");
        environmentVariables.set("kafka_node", "44.207.18.177:9092");
        environmentVariables.set("certificates_s3_reference", "s3://msk-connector-test-bucket/mskfiles/");
        environmentVariables.set("secrets_manager_secret", "AmazonMSK_afq");
    }

    @Test
    public void testGetQueryExecutorObject() throws Exception {

        String arn = "arn:aws:glue:us-west-2:430676967608:registry/Athena-MSK";
        String arn1 = "arn:aws:glue:us-west-2:430676967608:registry/Athena-MSK/Customer";

        List<SchemaListItem> list = new ArrayList<>();
        SchemaListItem schemaListItem = new SchemaListItem();
        schemaListItem.setSchemaName("test");
        schemaListItem.setSchemaArn(arn1);
        list.add(schemaListItem);

        PowerMockito.whenNew(GlueRegistryReader.class).withNoArguments().thenReturn(glueRegistryReader);
        Mockito.when(glueRegistryReader.getSchemaListItemsWithSchemaRegistryARN(Mockito.any())).thenReturn(list);

        String schemaName="defaultschemaname";
        Long latestSchemaVersion =123L;
        GetSchemaResult getSchemaResult=new GetSchemaResult();
        GetSchemaVersionResult getSchemaVersionResult=new GetSchemaVersionResult();
        getSchemaVersionResult.setSchemaDefinition("{\"tableName\": \"test\"}");
        getSchemaResult.setSchemaArn(arn);
        getSchemaResult.setSchemaName(schemaName);
        getSchemaResult.setLatestSchemaVersion(latestSchemaVersion);

        PowerMockito.mockStatic(AWSGlueClientBuilder.class);
        PowerMockito.when(AWSGlueClientBuilder.defaultClient()).thenReturn(awsGlue);
        PowerMockito.when(awsGlue.getSchema(Mockito.any())).thenReturn(getSchemaResult);
        PowerMockito.when(awsGlue.getSchemaVersion(Mockito.any())).thenReturn(getSchemaVersionResult);

        ListSchemasResult listSchemasResult = new ListSchemasResult();
        schemaListItem.setSchemaName("defaultschemaname");
        list.add(schemaListItem);
        listSchemasResult.setSchemas(list);
        PowerMockito.when(awsGlue.listSchemas(Mockito.any())).thenReturn(listSchemasResult);

        PowerMockito.whenNew(ObjectMapper.class).withNoArguments().thenReturn(objectMapper);
        String json = "{}";
        Mockito.when(objectMapper.writeValueAsString(Mockito.any(Map.class))).thenReturn(json);
        PowerMockito.whenNew(FileWriter.class).withAnyArguments().thenReturn(fileWriter);

        PowerMockito.mockStatic(QueryExecutor.class);
        MskTrinoQueryExecutor queryExecutor = mock(MskTrinoQueryExecutor.class);
        PowerMockito.whenNew(MskTrinoQueryExecutor.class).withArguments(ArgumentMatchers.any(Session.class)).thenReturn(queryExecutor);

        PowerMockito.whenNew(KafkaPlugin.class).withNoArguments().thenReturn(kafkaPlugin);
        Mockito.doNothing().when(queryExecutor).installPlugin(kafkaPlugin);

        Mockito.doNothing().when(queryExecutor).createCatalog(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap());
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

        PowerMockito.whenNew(ListObjectsRequest.class).withNoArguments().thenReturn(listObjectsRequest);
        Mockito.when(listObjectsRequest.getBucketName()).thenReturn("msk-connector-test-bucket");
        Mockito.when(listObjectsRequest.getPrefix()).thenReturn("mskfiles");
        Mockito.when(listObjectsRequest.withBucketName(Mockito.eq("msk-connector-test-bucket"))).thenReturn(listObjectsRequest);
        Mockito.when(listObjectsRequest.withPrefix(Mockito.eq("mskfiles"))).thenReturn(listObjectsRequest);

        List<S3ObjectSummary> getObjectSummaries = new ArrayList<>();
        Mockito.when(objectListing.getObjectSummaries()).thenReturn(getObjectSummaries);
        Mockito.when(amazonS3Client.listObjects(listObjectsRequest)).thenReturn(objectListing);


        PowerMockito.whenNew(Properties.class).withNoArguments().thenReturn(prop);
        PowerMockito.whenNew(FileOutputStream.class).withArguments(Mockito.anyString(), Mockito.anyBoolean()).thenReturn(fos);
        Mockito.doNothing().when(prop).store(fos, null);


        assertNotNull(AmazonMskUtils.getQueryExecutor());

        environmentVariables.set("auth_type", "SCRAM");
        assertNotNull(AmazonMskUtils.getQueryExecutor());
        environmentVariables.set("auth_type", "SERVERLESS");
        assertNotNull(AmazonMskUtils.getQueryExecutor());
        environmentVariables.set("auth_type", "TLS");
        assertNotNull(AmazonMskUtils.getQueryExecutor());


    }

    @Test
    public void testGetMSKConfigForTLS()  {

        Map<String, Object> cred = new HashMap<>();
        cred.put(AmazonMskConstants.KEYSTORE_PASSWORD,"keystorepwd");
        cred.put(AmazonMskConstants.TRUSTSTORE_PASSWORD,"truststorepwd");
        cred.put(AmazonMskConstants.SSL_PASSWORD,"sslpwd");
        Path tempDir = mock(Path.class);
        Mockito.when(tempDir.getParent()).thenReturn(tempDir);
        String fileName = "mskfiles";
        AmazonMskUtils.getMSKConfigForTLS(cred,tempDir,fileName);
    }

    @Test
    public void testGetS3Data() throws Exception {

        AmazonS3 s3client = mock(AmazonS3.class);
        Path tempDir = mock(Path.class);
        ObjectListing objectListing = mock(ObjectListing.class);
        ListObjectsRequest listObjectsRequest = mock(ListObjectsRequest.class);

        Mockito.when(listObjectsRequest.withBucketName(Mockito.anyString())).thenReturn(listObjectsRequest);
        Mockito.when(listObjectsRequest.withPrefix(Mockito.anyString())).thenReturn(listObjectsRequest);

        S3Object s3Object = mock(S3Object.class);
        Mockito.when(s3client.listObjects(Mockito.anyString(),Mockito.anyString())).thenReturn(objectListing);
        Mockito.when(s3client.getObject(Mockito.anyString(),Mockito.anyString())).thenReturn(s3Object);
        AmazonMskUtils.copyDataToTempFolderFromS3(s3client,tempDir);

    }

}
