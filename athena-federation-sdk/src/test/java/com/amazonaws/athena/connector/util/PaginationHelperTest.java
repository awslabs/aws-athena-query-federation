/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class PaginationHelperTest
{

    @Test
    void testValidateAndParsePaginationArguments_ValidInput()
    {
        assertEquals(0, PaginationHelper.validateAndParsePaginationArguments(null, 10));
        assertEquals(5, PaginationHelper.validateAndParsePaginationArguments("5", 10));
        assertEquals(0, PaginationHelper.validateAndParsePaginationArguments("0", -1));
    }

    @Test
    void testValidateAndParsePaginationArguments_InvalidNextToken()
    {
        assertThrows(IllegalArgumentException.class, () ->
                PaginationHelper.validateAndParsePaginationArguments("invalid", 10));
        assertThrows(IllegalArgumentException.class, () ->
                PaginationHelper.validateAndParsePaginationArguments("-1", 10));
    }

    @Test
    void testValidateAndParsePaginationArguments_InvalidPageSize()
    {
        assertThrows(IllegalArgumentException.class, () ->
                PaginationHelper.validateAndParsePaginationArguments("0", -2));
    }

    @Test
    void testManualPagination_NormalCase()
    {
        List<TableName> allTables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2"),
                new TableName("schema2", "table3"),
                new TableName("schema2", "table4"),
                new TableName("schema3", "table5")
        );

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, 1, 2, "testCatalog");
        List<TableName> resultTables = new ArrayList<>(response.getTables());

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(2, response.getTables().size());
        assertEquals("table2", resultTables.get(0).getTableName());
        assertEquals("table3", resultTables.get(1).getTableName());
        assertEquals("3", response.getNextToken());
    }

    @Test
    void testManualPagination_UnlimitedPageSize()
    {
        List<TableName> allTables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2"),
                new TableName("schema2", "table3")
        );

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, 0, -1, "testCatalog");

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(3, response.getTables().size());
        assertNull(response.getNextToken());
    }

    @Test
    void testManualPagination_StartTokenPastEnd()
    {
        List<TableName> allTables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2")
        );

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, 3, 1, "testCatalog");

        assertEquals("testCatalog", response.getCatalogName());
        assertTrue(response.getTables().isEmpty());
        assertNull(response.getNextToken());
    }

    @Test
    void testManualPagination_LastPage()
    {
        List<TableName> allTables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2"),
                new TableName("schema2", "table3")
        );

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, 2, 2, "testCatalog");
        List<TableName> resultTables = new ArrayList<>(response.getTables());

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(1, response.getTables().size());
        assertEquals("table3", resultTables.get(0).getTableName());
        assertNull(response.getNextToken());
    }
}
