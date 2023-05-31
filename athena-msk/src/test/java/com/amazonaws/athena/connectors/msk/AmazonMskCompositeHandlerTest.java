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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AmazonMskCompositeHandlerTest {

    static {
        System.setProperty("aws.region", "us-west-2");
    }

    private java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
        "glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-Kafka",
        "bootstrap.servers", "test"
    );

    @Mock
    KafkaConsumer<String, String> kafkaConsumer;
    @Mock
    private AWSSecretsManager secretsManager;

    private AmazonMskCompositeHandler amazonMskCompositeHandler;
    private MockedStatic<AmazonMskUtils> mockedMskUtils;
    private MockedStatic<AWSSecretsManagerClientBuilder> mockedSecretsManagerClient;

    @Before
    public void setUp() throws Exception {
        mockedSecretsManagerClient = Mockito.mockStatic(AWSSecretsManagerClientBuilder.class);
        mockedSecretsManagerClient.when(()-> AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        mockedMskUtils = Mockito.mockStatic(AmazonMskUtils.class);
        mockedMskUtils.when(() -> AmazonMskUtils.getKafkaConsumer(configOptions)).thenReturn(kafkaConsumer);
    }

    @After
    public void close() {
        mockedMskUtils.close();
        mockedSecretsManagerClient.close();
    }

    @Test
    public void amazonMskCompositeHandlerTest() throws Exception {
        amazonMskCompositeHandler = new AmazonMskCompositeHandler();
        Assert.assertTrue(amazonMskCompositeHandler instanceof AmazonMskCompositeHandler);
    }

}
