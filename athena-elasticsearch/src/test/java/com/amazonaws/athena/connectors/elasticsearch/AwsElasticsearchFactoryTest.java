/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.elasticsearch.ElasticsearchClient;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

/**
 * This class is used to test the AwsElasticsearchFactory class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AwsElasticsearchFactoryTest
{
    @Test
    public void getClient_returnsElasticsearchClient()
    {
        ElasticsearchClient mockClient = mock(ElasticsearchClient.class);

        try (MockedStatic<ElasticsearchClient> elasticsearchClientStaticMock = mockStatic(ElasticsearchClient.class)) {
            elasticsearchClientStaticMock.when(ElasticsearchClient::create).thenReturn(mockClient);

            AwsElasticsearchFactory factory = new AwsElasticsearchFactory();
            ElasticsearchClient client = factory.getClient();

            assertNotNull("Elasticsearch client should not be null", client);
            elasticsearchClientStaticMock.verify(ElasticsearchClient::create);
        }
    }
}
