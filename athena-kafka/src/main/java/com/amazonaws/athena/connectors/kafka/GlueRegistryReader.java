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

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetSchemaRequest;
import com.amazonaws.services.glue.model.GetSchemaResult;
import com.amazonaws.services.glue.model.GetSchemaVersionRequest;
import com.amazonaws.services.glue.model.GetSchemaVersionResult;
import com.amazonaws.services.glue.model.SchemaId;
import com.amazonaws.services.glue.model.SchemaVersionNumber;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    public GetSchemaVersionResult getSchemaVersionResult(String glueRegistryName, String glueSchemaName)
    {
        AWSGlue glue = AWSGlueClientBuilder.defaultClient();
        SchemaId sid = new SchemaId().withRegistryName(glueRegistryName).withSchemaName(glueSchemaName);
        GetSchemaResult schemaResult = glue.getSchema(new GetSchemaRequest().withSchemaId(sid));
        SchemaVersionNumber svn = new SchemaVersionNumber().withVersionNumber(schemaResult.getLatestSchemaVersion());
        return glue.getSchemaVersion(new GetSchemaVersionRequest()
                .withSchemaId(sid)
                .withSchemaVersionNumber(svn)
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
        GetSchemaVersionResult result = getSchemaVersionResult(glueRegistryName, glueSchemaName);
        return objectMapper.readValue(result.getSchemaDefinition(), clazz);
    }
}
