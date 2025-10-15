
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
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;
import software.amazon.awssdk.utils.Pair;

import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_BASE_ID_PARAMETER;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_TABLE_ID_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class GlueCatalogServiceTest {

    @Test
    public void getLarkBaseAndTableIdFromTable_success() {
        GlueClient glueClient = Mockito.mock(GlueClient.class);
        Table table = Table.builder()
                .parameters(Map.of(LARK_BASE_ID_PARAMETER, "base_id", LARK_TABLE_ID_PARAMETER, "table_id"))
                .build();
        GetTableResponse getTableResponse = GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(getTableResponse);

        GlueCatalogService glueCatalogService = new GlueCatalogService(glueClient);
        Pair<String, String> result = glueCatalogService.getLarkBaseAndTableIdFromTable("database", "table");

        assertEquals("base_id", result.left());
        assertEquals("table_id", result.right());
    }

    @Test
    public void getFieldNameMappings_success() {
        GlueClient glueClient = Mockito.mock(GlueClient.class);
        Column column1 = Column.builder().name("col1").comment("LarkBaseFieldName=field1/LarkBaseFieldType=Text/LarkBaseChildFieldType=null").build();
        Column column2 = Column.builder().name("col2").comment("LarkBaseFieldName=field2/LarkBaseFieldType=Number/LarkBaseChildFieldType=null").build();
        StorageDescriptor storageDescriptor = StorageDescriptor.builder().columns(column1, column2).build();
        Table table = Table.builder().storageDescriptor(storageDescriptor).build();
        GetTableResponse getTableResponse = GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(getTableResponse);

        GlueCatalogService glueCatalogService = new GlueCatalogService(glueClient);
        List<AthenaFieldLarkBaseMapping> result = glueCatalogService.getFieldNameMappings("database", "table");

        assertEquals(2, result.size());
        assertEquals("field1", result.get(0).larkBaseFieldName());
        assertEquals(new NestedUIType(UITypeEnum.TEXT, UITypeEnum.UNKNOWN), result.get(0).nestedUIType());
        assertEquals("field2", result.get(1).larkBaseFieldName());
        assertEquals(new NestedUIType(UITypeEnum.NUMBER, UITypeEnum.UNKNOWN), result.get(1).nestedUIType());
    }
}
