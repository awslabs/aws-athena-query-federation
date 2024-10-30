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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetSchemaRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionNumber;

public class GlueRegistryReader
{
    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Fetch glue schema content for latest version
     * @param glueRegistryName
     * @param glueSchemaName
     * @return
     */
    public GetSchemaVersionResponse getSchemaVersionResult(String glueRegistryName, String glueSchemaName)
    {
        GlueClient glue = GlueClient.create();
        SchemaId sid = SchemaId.builder().registryName(glueRegistryName).schemaName(glueSchemaName).build();
        GetSchemaResponse schemaResponse = glue.getSchema(GetSchemaRequest.builder().schemaId(sid).build());
        SchemaVersionNumber svn = SchemaVersionNumber.builder().versionNumber(schemaResponse.latestSchemaVersion()).build();
        return glue.getSchemaVersion(GetSchemaVersionRequest.builder()
                .schemaId(sid)
                .schemaVersionNumber(svn)
                .build()
        );
    }
    /**
     * fetch schema file content from glue schema.
     *
     * @param glueRegistryName
     * @param glueSchemaName
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> T getGlueSchema(String glueRegistryName, String glueSchemaName, Class<T> clazz) throws Exception
    {
        GetSchemaVersionResponse result = getSchemaVersionResult(glueRegistryName, glueSchemaName);
        return objectMapper.readValue(result.schemaDefinition(), clazz);
    }
    public String getGlueSchemaType(String glueRegistryName, String glueSchemaName)
    {
        GetSchemaVersionResponse result = getSchemaVersionResult(glueRegistryName, glueSchemaName);
        return result.dataFormatAsString();
    }
    public String getSchemaDef(String glueRegistryName, String glueSchemaName)
    {
        GetSchemaVersionResponse result = getSchemaVersionResult(glueRegistryName, glueSchemaName);
        return result.schemaDefinition();
    }
}
