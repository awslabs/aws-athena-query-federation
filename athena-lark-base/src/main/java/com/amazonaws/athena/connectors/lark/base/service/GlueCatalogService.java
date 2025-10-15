/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.lark.base.service;

import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.utils.Pair;

import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_BASE_ID_PARAMETER;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_TABLE_ID_PARAMETER;
import static java.util.Objects.requireNonNull;

public class GlueCatalogService
{
    private static final Logger logger = LoggerFactory.getLogger(GlueCatalogService.class);
    private final GlueClient glueClient;

    public GlueCatalogService(GlueClient glueClient)
    {
        requireNonNull(glueClient);
        this.glueClient = glueClient;
    }

    private GetTableResponse getGlueTable(String databaseName, String tableName)
    {
        GetTableRequest request = GetTableRequest.builder()
                .databaseName(databaseName)
                .name(tableName)
                .build();

        return glueClient.getTable(request);
    }

    public Pair<String, String> getLarkBaseAndTableIdFromTable(String databaseName, String tableName)
    {
        GetTableResponse response = this.getGlueTable(databaseName, tableName);

        // On the glue table, lark table id is stored in the table properties key "larkTableId"
        String baseId = response.table().parameters().get(LARK_BASE_ID_PARAMETER);
        String tableId = response.table().parameters().get(LARK_TABLE_ID_PARAMETER);

        return Pair.of(baseId, tableId);
    }

    public List<AthenaFieldLarkBaseMapping> getFieldNameMappings(String schemaName, String tableName)
    {
        GetTableResponse response = getGlueTable(schemaName, tableName);

        List<AthenaFieldLarkBaseMapping> fieldNameMappings = new ArrayList<>();
        response.table().storageDescriptor().columns().forEach(column -> {
            String comment = column.comment();
            String columnName = CommonUtil.extractFieldNameFromComment(comment);
            String sanitizedColumnName = CommonUtil.sanitizeGlueRelatedName(columnName);
            NestedUIType fieldType = CommonUtil.extractFieldTypeFromComment(comment);

            logger.info("Field name: {}, UI Type: {}, Child Type: {}", columnName, fieldType.uiType(), fieldType.childType());

            fieldNameMappings.add(new AthenaFieldLarkBaseMapping(sanitizedColumnName, columnName, fieldType));
        });

        return fieldNameMappings;
    }
}
