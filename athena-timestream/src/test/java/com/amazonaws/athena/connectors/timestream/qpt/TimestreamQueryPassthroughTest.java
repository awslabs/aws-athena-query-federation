/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.qpt;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.timestream.qpt.TimestreamQueryPassthrough.QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class TimestreamQueryPassthroughTest {

    private static final String VALID_QUERY = "SELECT * FROM table1 WHERE col1 = 'value'";
    private static final String VALID_SCHEMA_FUNCTION = "system.query";
    private static final String EMPTY_STRING = "";
    private static final String INSERT_QUERY = "INSERT INTO table1 VALUES (1)";
    private static final String SELECT_WITH_INSERT_QUERY = "SELECT * FROM table1 WHERE col1 IN (SELECT * FROM table2) INSERT INTO table3 VALUES (1)";
    private static final String SELECT_WITH_UPDATE_QUERY = "SELECT * FROM table1 UPDATE table2 SET col1 = 'value'";
    private static final String SELECT_WITH_DELETE_QUERY = "SELECT * FROM table1 WHERE col1 IN (SELECT * FROM table2) DELETE FROM table3";
    private static final String SELECT_WITH_CREATE_QUERY = "SELECT * FROM table1 CREATE TABLE table2 (col1 INT)";
    private static final String SELECT_WITH_DROP_QUERY = "SELECT * FROM table1 DROP TABLE table2";
    private static final String SELECT_WITH_ALTER_QUERY = "SELECT * FROM table1 ALTER TABLE table2 ADD COLUMN col1 INT";
    private static final String SELECT_QUERY_WITH_WHITESPACE = "   SELECT * FROM table1   ";
    private static final String SELECT_QUERY_LOWERCASE = "select * from table1";
    private static final String COMPLEX_SELECT_QUERY = "SELECT col1, col2, COUNT(*) as cnt FROM table1 WHERE col1 > 100 GROUP BY col1, col2 HAVING cnt > 5 ORDER BY col1";

    private final TimestreamQueryPassthrough queryPassthrough = new TimestreamQueryPassthrough();
    private Map<String, String> baseArguments;

    @Before
    public void setUp() {
        baseArguments = createArguments();
    }

    private Map<String, String> createArguments() {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(QUERY, VALID_QUERY);
        arguments.put(SCHEMA_FUNCTION_NAME, VALID_SCHEMA_FUNCTION);
        return arguments;
    }

    @Test
    public void getFunctionSchema_WhenCalled_ReturnsSystemSchemaName() {
        assertEquals("Function schema should be system", TimestreamQueryPassthrough.SCHEMA_NAME, queryPassthrough.getFunctionSchema());
    }

    @Test
    public void getFunctionName_WhenCalled_ReturnsQueryFunctionName() {
        assertEquals("Function name should be query", TimestreamQueryPassthrough.NAME, queryPassthrough.getFunctionName());
    }

    @Test
    public void getFunctionArguments_WhenCalled_ReturnsSingleQueryKeyArgument() {
        List<String> arguments = queryPassthrough.getFunctionArguments();
        assertNotNull("Function arguments list should not be null", arguments);
        assertEquals("Should have exactly one argument", 1, arguments.size());
        assertEquals("First argument should be QUERY key", QUERY, arguments.get(0));
    }

    @Test
    public void verify_WithValidQueryAndSchemaFunction_AcceptsArguments() {
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithEmptyArguments_ThrowsAthenaConnectorException() {
        queryPassthrough.verify(new HashMap<>());
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithMissingQuery_ThrowsAthenaConnectorException() {
        baseArguments.remove(QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithEmptyQuery_ThrowsAthenaConnectorException() {
        baseArguments.put(QUERY, EMPTY_STRING);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithQueryNotStartingWithSelect_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, INSERT_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithQueryContainingInsert_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, SELECT_WITH_INSERT_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithQueryContainingUpdate_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, SELECT_WITH_UPDATE_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithQueryContainingDelete_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, SELECT_WITH_DELETE_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithQueryContainingCreate_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, SELECT_WITH_CREATE_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithQueryContainingDrop_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, SELECT_WITH_DROP_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithQueryContainingAlter_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, SELECT_WITH_ALTER_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test
    public void verify_WithLeadingTrailingWhitespace_AcceptsQuery() {
        baseArguments.put(QUERY, SELECT_QUERY_WITH_WHITESPACE);
        queryPassthrough.verify(baseArguments);
    }

    @Test
    public void verify_WithLowercaseSelectKeyword_AcceptsQuery() {
        baseArguments.put(QUERY, SELECT_QUERY_LOWERCASE);
        queryPassthrough.verify(baseArguments);
    }

    @Test
    public void verify_WithGroupByHavingOrderBy_AcceptsQuery() {
        baseArguments.put(QUERY, COMPLEX_SELECT_QUERY);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithNullQueryValue_ThrowsAthenaConnectorException() {
        baseArguments.put(QUERY, null);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void verify_WithWhitespaceOnlyQuery_ThrowsUnsupportedOperationException() {
        baseArguments.put(QUERY, "   \t\n  ");
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithMissingSchemaFunctionName_ThrowsAthenaConnectorException() {
        baseArguments.remove(SCHEMA_FUNCTION_NAME);
        queryPassthrough.verify(baseArguments);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithWrongSchemaFunctionName_ThrowsAthenaConnectorException() {
        baseArguments.put(SCHEMA_FUNCTION_NAME, "wrong.schema");
        queryPassthrough.verify(baseArguments);
    }
}

