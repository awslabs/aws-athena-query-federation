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

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetSchemaRequest;
import com.amazonaws.services.glue.model.GetSchemaResult;
import com.amazonaws.services.glue.model.GetSchemaVersionRequest;
import com.amazonaws.services.glue.model.GetSchemaVersionResult;
import com.amazonaws.services.glue.model.ListSchemasRequest;
import com.amazonaws.services.glue.model.ListSchemasResult;
import com.amazonaws.services.glue.model.RegistryId;
import com.amazonaws.services.glue.model.SchemaId;
import com.amazonaws.services.glue.model.SchemaListItem;
import com.amazonaws.services.glue.model.SchemaVersionNumber;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class GlueRegistryReader
{
    private final AWSGlue glue;
    private final ObjectMapper objectMapper;

    public GlueRegistryReader()
    {
        this.objectMapper = new ObjectMapper();
        glue = AWSGlueClientBuilder.defaultClient();
        objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    }

    /**
     * Fetch glue schema content for latest version
     * @param arn
     * @return
     */
    public GetSchemaVersionResult getSchemaVersionResult(String arn)
    {
        SchemaId sid = new SchemaId().withSchemaArn(arn);
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
     * @param arn
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> T getGlueSchema(String arn, Class<T> clazz) throws Exception
    {
        GetSchemaVersionResult result = getSchemaVersionResult(arn);
        return objectMapper.readValue(result.getSchemaDefinition(), clazz);
    }

    /**
     * Fetch Schema name and schema ARN from Glue schema registry
     * @param registryNameArn
     * @return
     * Example
     * schema-name: employee, schema-arn: arn:aws:glue:us-west-2:430676967608:schema/Athena-MSK/employee
     */
    public List<SchemaListItem> getSchemaListItemsWithSchemaRegistryARN(String registryNameArn)
    {
        ListSchemasResult result = glue.listSchemas(new ListSchemasRequest().withRegistryId(new RegistryId().withRegistryArn(registryNameArn)));
        return result.getSchemas();
    }
}
