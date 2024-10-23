/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates Elasticsearch secrets deserialization, stored in following JSON format (showing minimal required for extraction):
 * <code>
 * {
 *     "username": "${user}",
 *     "password": "${password}"
 * }
 * </code>
 */
public class ElasticsearchCredentialProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchCredentialProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ElasticsearchCredential elasticsearchCredential;

    public ElasticsearchCredentialProvider(final String secretString)
    {
        Map<String, String> elasticsearchSecrets;
        try {
            elasticsearchSecrets = OBJECT_MAPPER.readValue(secretString, HashMap.class);
        }
        catch (IOException ioException) {
            throw new RuntimeException("Could not deserialize Elasticsearch credentials into HashMap", ioException);
        }

        this.elasticsearchCredential = new ElasticsearchCredential(elasticsearchSecrets.get("username"), elasticsearchSecrets.get("password"));
    }

    public ElasticsearchCredential getCredential()
    {
        return this.elasticsearchCredential;
    }
}
