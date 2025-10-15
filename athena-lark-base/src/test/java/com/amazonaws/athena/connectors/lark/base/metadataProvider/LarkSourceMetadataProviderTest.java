
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
package com.amazonaws.athena.connectors.lark.base.metadataProvider;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.AthenaLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.PartitionInfoResult;
import com.amazonaws.athena.connectors.lark.base.model.TableDirectInitialized;
import com.amazonaws.athena.connectors.lark.base.model.TableSchemaResult;
import org.junit.jupiter.api.Test;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LarkSourceMetadataProviderTest {

    @Test
    public void getTableSchema_success() {
        List<TableDirectInitialized> resolvedMappings = List.of(
                new TableDirectInitialized(
                        new AthenaLarkBaseMapping("db1", "base1"),
                        new AthenaLarkBaseMapping("table1", "tbl1"),
                        Collections.emptyList()
                )
        );
        LarkSourceMetadataProvider provider = new LarkSourceMetadataProvider(resolvedMappings);
        GetTableRequest request = new GetTableRequest(Mockito.mock(FederatedIdentity.class), "queryId", "catalog", new TableName("db1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = provider.getTableSchema(request);

        assertTrue(result.isPresent());
        assertNotNull(result.get().schema());
    }

    @Test
    public void getTableSchema_notFound() {
        LarkSourceMetadataProvider provider = new LarkSourceMetadataProvider(Collections.emptyList());
        GetTableRequest request = new GetTableRequest(Mockito.mock(FederatedIdentity.class), "queryId", "catalog", new TableName("db1", "table1"), Collections.emptyMap());

        Optional<TableSchemaResult> result = provider.getTableSchema(request);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_success() {
        List<TableDirectInitialized> resolvedMappings = List.of(
                new TableDirectInitialized(
                        new AthenaLarkBaseMapping("db1", "base1"),
                        new AthenaLarkBaseMapping("table1", "tbl1"),
                        List.of(new AthenaFieldLarkBaseMapping("col1", "field1", null))
                )
        );
        LarkSourceMetadataProvider provider = new LarkSourceMetadataProvider(resolvedMappings);
        TableName tableName = new TableName("db1", "table1");

        Optional<PartitionInfoResult> result = provider.getPartitionInfo(tableName);

        assertTrue(result.isPresent());
        assertEquals("base1", result.get().baseId());
        assertEquals("tbl1", result.get().tableId());
        assertEquals(1, result.get().fieldNameMappings().size());
    }

    @Test
    public void getPartitionInfo_notFound() {
        LarkSourceMetadataProvider provider = new LarkSourceMetadataProvider(Collections.emptyList());
        TableName tableName = new TableName("db1", "table1");

        Optional<PartitionInfoResult> result = provider.getPartitionInfo(tableName);

        assertFalse(result.isPresent());
    }

    @Test
    public void getPartitionInfo_invalidMapping() {
        List<TableDirectInitialized> resolvedMappings = List.of(
                new TableDirectInitialized(
                        new AthenaLarkBaseMapping("db1", ""),
                        new AthenaLarkBaseMapping("table1", ""),
                        Collections.emptyList()
                )
        );
        LarkSourceMetadataProvider provider = new LarkSourceMetadataProvider(resolvedMappings);
        TableName tableName = new TableName("db1", "table1");

        Optional<PartitionInfoResult> result = provider.getPartitionInfo(tableName);

        assertFalse(result.isPresent());
    }
}
