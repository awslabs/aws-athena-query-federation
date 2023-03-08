/*-
 * #%L
 * athena-kafka
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

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.ArgumentMatchers.nullable;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*"})
@PrepareForTest({AWSSecretsManagerClientBuilder.class,
        AWSSecretsManager.class, KafkaUtils.class})
public class KafkaCompositeHandlerTest {

    static {
        System.setProperty("aws.region", "us-west-2");
    }

    private java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
        "glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-Kafka",
        "bootstrap.servers", "test"
    );

    @Mock
    KafkaConsumer<String, String> kafkaConsumer;

    private KafkaCompositeHandler kafkaCompositeHandler;

    @Mock
    private AWSSecretsManager secretsManager;

    @Test
    public void kafkaCompositeHandlerTest() throws Exception {
        mockStatic(AWSSecretsManagerClientBuilder.class);
        PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        mockStatic(KafkaUtils.class);
        PowerMockito.when(KafkaUtils.getKafkaConsumer(configOptions)).thenReturn(kafkaConsumer);
        kafkaCompositeHandler = new KafkaCompositeHandler();
        Assert.assertTrue(kafkaCompositeHandler instanceof KafkaCompositeHandler);
    }

}
