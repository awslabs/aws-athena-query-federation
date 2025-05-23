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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
        assertThrows(AthenaConnectorException.class, () ->
                PaginationHelper.validateAndParsePaginationArguments("invalid", 10));
        assertThrows(AthenaConnectorException.class, () ->
                PaginationHelper.validateAndParsePaginationArguments("-1", 10));
    }

    @Test
    void testValidateAndParsePaginationArguments_InvalidPageSize()
    {
        assertThrows(AthenaConnectorException.class, () ->
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

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, "1", 2, "testCatalog");
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

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, "0", -1, "testCatalog");

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

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, "3", 1, "testCatalog");

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

        ListTablesResponse response = PaginationHelper.manualPagination(allTables, "2", 2, "testCatalog");
        List<TableName> resultTables = new ArrayList<>(response.getTables());

        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(1, response.getTables().size());
        assertEquals("table3", resultTables.get(0).getTableName());
        assertNull(response.getNextToken());
    }

    @Test
    void testManualPagination_SortedResults()
    {
        // Create a list of tables in unsorted order
        List<TableName> allTables = Arrays.asList(
                new TableName("schema2", "tableC"),
                new TableName("schema1", "tableA"),
                new TableName("schema3", "tableB"),
                new TableName("schema1", "tableD"),
                new TableName("schema2", "tableE")
        );

        // Get first page with 3 items
        ListTablesResponse response = PaginationHelper.manualPagination(allTables, "0", 3, "testCatalog");

        // Verify response
        assertEquals("testCatalog", response.getCatalogName());
        assertEquals(3, response.getTables().size());
        assertEquals("3", response.getNextToken());

        // Convert to ArrayList to access elements
        List<TableName> resultTables = new ArrayList<>(response.getTables());

        // Verify sorting (should be alphabetical by table name regardless of schema)
        assertEquals("tableA", resultTables.get(0).getTableName());
        assertEquals("tableB", resultTables.get(1).getTableName());
        assertEquals("tableC", resultTables.get(2).getTableName());

        // Get second page
        response = PaginationHelper.manualPagination(allTables, "3", 3, "testCatalog");
        resultTables = new ArrayList<>(response.getTables());

        // Verify remaining sorted items
        assertEquals(2, resultTables.size());
        assertEquals("tableD", resultTables.get(0).getTableName());
        assertEquals("tableE", resultTables.get(1).getTableName());
        assertNull(response.getNextToken()); // Should be null as we've reached the end
    }

    @Test
    void testCalculateNextToken_UnlimitedPageSize()
    {
        List<TableName> tables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2")
        );
        String nextToken = PaginationHelper.calculateNextToken(0, Integer.MAX_VALUE, tables);
        assertNull(nextToken);
    }

    @Test
    void testCalculateNextToken_EmptyList() {
        List<TableName> emptyList = Collections.emptyList();
        String nextToken = PaginationHelper.calculateNextToken(5, 10, emptyList);
        assertNull(nextToken);
    }

    @Test
    void testCalculateNextToken_PartialPage()
    {
        // When returned tables is less than pageSize (reaching end of list)
        List<TableName> tables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2")
        );
        String nextToken = PaginationHelper.calculateNextToken(10, 5, tables);
        assertNull(nextToken);
    }

    @Test
    void testCalculateNextToken_FullPage()
    {
        // When returned tables equals pageSize (more tables might exist)
        List<TableName> tables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2"),
                new TableName("schema1", "table3")
        );
        String nextToken = PaginationHelper.calculateNextToken(0, 3, tables);
        assertEquals("3", nextToken);
    }

    @Test
    void testCalculateNextToken_NonZeroStartToken()
    {
        List<TableName> tables = Arrays.asList(
                new TableName("schema1", "table4"),
                new TableName("schema1", "table5")
        );
        String nextToken = PaginationHelper.calculateNextToken(5, 2, tables);
        assertEquals("7", nextToken);
    }

    @Test
    void testCalculateNextToken_LargeValues()
    {
        // Test with large numbers to ensure no integer overflow
        List<TableName> tables = Arrays.asList(
                new TableName("schema1", "table1"),
                new TableName("schema1", "table2")
        );
        String nextToken = PaginationHelper.calculateNextToken(Integer.MAX_VALUE - 5, 2, tables);
        assertEquals(String.valueOf(Integer.MAX_VALUE - 3L), nextToken);
    }
}
