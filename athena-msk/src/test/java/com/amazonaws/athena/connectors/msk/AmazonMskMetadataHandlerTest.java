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


import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetSchemaResult;
import com.amazonaws.services.glue.model.GetSchemaVersionResult;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import uk.org.webcompere.systemstubs.rules.EnvironmentVariablesRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*", "javax.security.*"
})
@PrepareForTest({AWSSecretsManagerClientBuilder.class, AWSGlueClientBuilder.class})
public class AmazonMskMetadataHandlerTest {
    private static final String QUERY_ID = "queryId";
    private AmazonMskMetadataHandler amazonMskMetadataHandler;
    private BlockAllocator blockAllocator;
    private FederatedIdentity federatedIdentity;
    private Block partitions;
    private List<String> partitionCols;
    private Constraints constraints;

    @Rule
    public EnvironmentVariablesRule environmentVariables = new EnvironmentVariablesRule();

    @Mock
    AWSGlue awsGlue;

    MockConsumer<String, String> consumer;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        blockAllocator = new BlockAllocatorImpl();
        federatedIdentity = Mockito.mock(FederatedIdentity.class);
        partitions = Mockito.mock(Block.class);
        partitionCols = Mockito.mock(List.class);
        constraints = Mockito.mock(Constraints.class);
        environmentVariables.set("aws.region", "us-west-2");
        environmentVariables.set("glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-NEW");
        environmentVariables.set("auth_type", "TLS");
        environmentVariables.set("secret_manager_msk_creds_name", "testSecret");
        environmentVariables.set("kafka_endpoint", "12.207.18.179:9092");
        environmentVariables.set("certificates_s3_reference", "s3://msk-connector-test-bucket/mskfiles/");
        environmentVariables.set("secrets_manager_secret", "AmazonMSK_afq");

        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        Map<TopicPartition, Long> partitions = Map.of(
                new TopicPartition("testTopic", 0), 20100L,
                new TopicPartition("testTopic", 1), 7850L
        );
        List<PartitionInfo> partitionInfoList = new ArrayList<>(partitions.keySet())
                .stream()
                .map(it -> new PartitionInfo(it.topic(), it.partition(), null, null, null))
                .collect(Collectors.toList());
        consumer.updateEndOffsets(partitions);
        consumer.updatePartitions("testTopic", partitionInfoList);

        amazonMskMetadataHandler = new AmazonMskMetadataHandler(consumer);
    }

    @After
    public void tearDown() {
        blockAllocator.close();
    }

    @Test
    public void testDoListSchemaNames() {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("default");
        ListSchemasRequest listSchemasRequest = new ListSchemasRequest(federatedIdentity, QUERY_ID, "default");
        ListSchemasResponse listSchemasResponse = amazonMskMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertEquals(schemaNames, new ArrayList<>(listSchemasResponse.getSchemas()));
    }

    @Test(expected = RuntimeException.class)
    public void testDoListSchemaNamesThrowsException() {
        ListSchemasRequest listSchemasRequest = mock(ListSchemasRequest.class);
        when(listSchemasRequest.getCatalogName()).thenThrow(new RuntimeException("RuntimeException() "));
        ListSchemasResponse listSchemasResponse = amazonMskMetadataHandler.doListSchemaNames(blockAllocator, listSchemasRequest);
        assertNull(listSchemasResponse);
    }


    @Test
    public void testDoGetTable() throws Exception {
        String arn = "defaultarn", schemaName = "defaultschemaname", schemaVersionId = "defaultversionid";
        Long latestSchemaVersion = 123L;
        GetSchemaResult getSchemaResult = new GetSchemaResult();
        GetSchemaVersionResult getSchemaVersionResult = new GetSchemaVersionResult();
        getSchemaResult.setSchemaArn(arn);
        getSchemaResult.setSchemaName(schemaName);
        getSchemaResult.setLatestSchemaVersion(latestSchemaVersion);
        getSchemaVersionResult.setSchemaArn(arn);
        getSchemaVersionResult.setSchemaVersionId(schemaVersionId);
        getSchemaVersionResult.setSchemaDefinition("{\n" +
                "\t\"tableName\": \"testtable\",\n" +
                "\t\"schemaName\": \"default\",\n" +
                "\t\"topicName\": \"testtable\",\n" +
                "\t\"message\": {\n" +
                "\t\t\"dataFormat\": \"json\",\n" +
                "\t\t\"fields\": [{\n" +
                "\t\t\t\"name\": \"intcol\",\n" +
                "\t\t\t\"mapping\": \"intcol\",\n" +
                "\t\t\t\"type\": \"INTEGER\"\n" +
                "\t\t}]\n" +
                "\t}\n" +
                "}");
        PowerMockito.mockStatic(AWSGlueClientBuilder.class);
        PowerMockito.when(AWSGlueClientBuilder.defaultClient()).thenReturn(awsGlue);
        PowerMockito.when(awsGlue.getSchema(Mockito.any())).thenReturn(getSchemaResult);
        PowerMockito.when(awsGlue.getSchemaVersion(Mockito.any())).thenReturn(getSchemaVersionResult);
        GetTableRequest getTableRequest = new GetTableRequest(federatedIdentity, QUERY_ID, "kafka", new TableName("default", "testtable"));
        GetTableResponse getTableResponse = amazonMskMetadataHandler.doGetTable(blockAllocator, getTableRequest);
        assertEquals(1, getTableResponse.getSchema().getFields().size());
    }

    @Test
    public void testDoGetSplits() {
        GetSplitsRequest request = new GetSplitsRequest(
                federatedIdentity,
                QUERY_ID,
                "kafka",
                new TableName("default", "testTopic"),
                Mockito.mock(Block.class),
                new ArrayList<>(),
                Mockito.mock(Constraints.class),
                "continuationToken"
        );
        GetSplitsResponse response = amazonMskMetadataHandler.doGetSplits(blockAllocator, request);
        assertEquals(4, response.getSplits().size());
    }
}
