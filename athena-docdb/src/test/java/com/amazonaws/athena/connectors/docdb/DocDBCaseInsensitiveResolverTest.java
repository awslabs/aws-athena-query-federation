/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DocDBCaseInsensitiveResolverTest
{
    private static final String ENABLE_CASE_INSENSITIVE_MATCH = "enable_case_insensitive_match";
    private static final String TEST_DB = "TestDB";
    private static final String TEST_DB_LOWERCASE = "testdb";
    private static final String TEST_TABLE = "TestTable";
    private static final String TEST_TABLE_LOWERCASE = "testtable";
    private static final String NONEXISTENT = "nonexistent";
    private static final String CASE_INSENSITIVE_MATCH_ENABLED = Boolean.TRUE.toString();
    private static final String CASE_INSENSITIVE_MATCH_DISABLED = Boolean.FALSE.toString();

    @Mock
    private MongoClient mockClient;

    @Mock
    private MongoDatabase mockDatabase;

    @Mock
    private MongoIterable<String> mockDatabaseNames;

    @Mock
    private MongoIterable<String> mockCollectionNames;

    private Map<String, String> configOptions;

    @Before
    public void setup()
    {
        MockitoAnnotations.openMocks(this);
        configOptions = new HashMap<>();
        when(mockClient.listDatabaseNames()).thenReturn(mockDatabaseNames);
        when(mockDatabase.listCollectionNames()).thenReturn(mockCollectionNames);
    }

    @Test
    public void getSchemaNameCaseInsensitiveMatch_withCaseInsensitiveMatchDisabled_returnsOriginalSchemaName() 
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_DISABLED);
        
        String result = DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
            configOptions, mockClient, TEST_DB);

        verify(mockClient, never()).listDatabaseNames();
        assertEquals(TEST_DB, result);
    }

    @Test
    public void getSchemaNameCaseInsensitiveMatch_withCaseInsensitiveMatchEnabled_returnsMatchedSchemaName()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockDatabaseNames.spliterator()).thenReturn(
            Arrays.asList(TEST_DB, "otherDB").spliterator());

        String result = DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
            configOptions, mockClient, TEST_DB_LOWERCASE);

        assertEquals(TEST_DB, result);
    }

    @Test
    public void getSchemaNameCaseInsensitiveMatch_withNoMatchFound_throwsIllegalArgumentException()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockDatabaseNames.spliterator()).thenReturn(
            Arrays.asList("DB1", "DB2").spliterator());

        try {
            DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
                configOptions, mockClient, NONEXISTENT);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertTrue("Exception message should contain Schema name is empty or more than 1",
                    ex.getMessage().contains("Schema name is empty or more than 1 for case insensitive match"));
        }
    }

    @Test
    public void getSchemaNameCaseInsensitiveMatch_withMultipleMatchesFound_throwsIllegalArgumentException()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockDatabaseNames.spliterator()).thenReturn(
            Arrays.asList(TEST_DB, "TESTDB", TEST_DB_LOWERCASE).spliterator());

        try {
            DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
                configOptions, mockClient, TEST_DB_LOWERCASE);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertTrue("Exception message should contain Schema name is empty or more than 1",
                    ex.getMessage().contains("Schema name is empty or more than 1 for case insensitive match"));
        }
    }

    @Test
    public void getTableNameCaseInsensitiveMatch_withCaseInsensitiveMatchDisabled_returnsOriginalTableName()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_DISABLED);
        
        String result = DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
            configOptions, mockDatabase, TEST_TABLE);

        verify(mockDatabase, never()).listCollectionNames();
        assertEquals(TEST_TABLE, result);
    }

    @Test
    public void getTableNameCaseInsensitiveMatch_withCaseInsensitiveMatchEnabled_returnsMatchedTableName()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockCollectionNames.spliterator()).thenReturn(
            Arrays.asList(TEST_TABLE, "otherTable").spliterator());

        String result = DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
            configOptions, mockDatabase, TEST_TABLE_LOWERCASE);

        assertEquals(TEST_TABLE, result);
    }

    @Test
    public void getTableNameCaseInsensitiveMatch_withNoMatchFound_throwsIllegalArgumentException()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockCollectionNames.spliterator()).thenReturn(
            Arrays.asList("Table1", "Table2").spliterator());

        try {
            DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
                configOptions, mockDatabase, NONEXISTENT);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertTrue("Exception message should contain Table name is empty or more than 1",
                    ex.getMessage().contains("Table name is empty or more than 1 for case insensitive match"));
        }
    }

    @Test
    public void getTableNameCaseInsensitiveMatch_withMultipleMatchesFound_throwsIllegalArgumentException()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockCollectionNames.spliterator()).thenReturn(
            Arrays.asList(TEST_TABLE, "TESTTABLE", TEST_TABLE_LOWERCASE).spliterator());

        try {
            DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
                configOptions, mockDatabase, TEST_TABLE_LOWERCASE);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertTrue("Exception message should contain Table name is empty or more than 1",
                    ex.getMessage().contains("Table name is empty or more than 1 for case insensitive match"));
        }
    }

    @Test
    public void getSchemaNameCaseInsensitiveMatch_withEmptyList_throwsIllegalArgumentException()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockDatabaseNames.spliterator()).thenReturn(
            Collections.<String>emptyList().spliterator());

        try {
            DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
                configOptions, mockClient, TEST_DB_LOWERCASE);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertTrue("Exception message should contain schema name is empty",
                    ex.getMessage().contains("Schema name is empty"));
        }
    }

    @Test
    public void getTableNameCaseInsensitiveMatch_withEmptyList_throwsIllegalArgumentException()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED);
        when(mockCollectionNames.spliterator()).thenReturn(
            Collections.<String>emptyList().spliterator());

        try {
            DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
                configOptions, mockDatabase, TEST_TABLE_LOWERCASE);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertTrue("Exception message should contain table name is empty",
                    ex.getMessage().contains("Table name is empty"));
        }
    }
}
