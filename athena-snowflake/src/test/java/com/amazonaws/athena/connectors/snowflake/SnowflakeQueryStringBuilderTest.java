/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SnowflakeQueryStringBuilderTest {

    private SnowflakeQueryStringBuilder queryBuilder;

    @Mock
    private Connection mockConnection;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        queryBuilder = new SnowflakeQueryStringBuilder("\"", null);
    }

    @Test
    void testGetFromClauseWithSplit() {
        String result = queryBuilder.getFromClauseWithSplit(null, "public", "users", null);
        assertEquals(" FROM \"public\".\"users\" ", result);
    }

    @Test
    void testGetPartitionWhereClauses() {
        List<String> result = queryBuilder.getPartitionWhereClauses(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void testBuildSqlString_NoConstraints() throws SQLException {
        Schema tableSchema = new Schema(List.of(new Field("id", new FieldType(true, new ArrowType.Int(32, true), null), null)));

        Constraints constraints = mock(Constraints.class);
        when(constraints.getLimit()).thenReturn(0L);

        String sql = queryBuilder.buildSqlString(mockConnection, null, "public", "users", tableSchema, constraints, null);
        assertTrue(sql.contains("SELECT \"id\" FROM \"public\".\"users\" "));
    }

    @Test
    void testBuildSqlString_WithConstraints() throws SQLException {
        Schema tableSchema = new Schema(List.of(new Field("id", new FieldType(true, new ArrowType.Int(32, true), null), null)));

        Constraints constraints = mock(Constraints.class);
        when(constraints.getLimit()).thenReturn(10L);

        String sql = queryBuilder.buildSqlString(mockConnection, null, "public", "users", tableSchema, constraints, null);
        assertTrue(sql.contains("LIMIT 10"));
    }

    @Test
    void testQuote() {
        String result = queryBuilder.quote("users");
        assertEquals("\"users\"", result);
    }

    @Test
    void testSingleQuote() {
        String result = queryBuilder.singleQuote("O'Reilly");
        assertEquals("'O''Reilly'", result);
    }

    @Test
    void testToPredicate_SingleValue() {
        List<TypeAndValue> accumulator = new ArrayList<>();
        String predicate = queryBuilder.toPredicate("age", "=", 30, new ArrowType.Int(32, true));
        assertEquals("age = 30", predicate);
    }

    @Test
    void testGetObjectForWhereClause_Int() {
        Object result = queryBuilder.getObjectForWhereClause("age", 42, new ArrowType.Int(32, true));
        assertEquals(42L, result);
    }

    @Test
    void testGetObjectForWhereClause_Decimal() {
        Object result = queryBuilder.getObjectForWhereClause("price", new BigDecimal("99.99"), new ArrowType.Decimal(10, 2));
        assertEquals(new BigDecimal("99.99"), result);
    }

    @Test
    void testGetObjectForWhereClause_Date() {
        Object result = queryBuilder.getObjectForWhereClause("date", "2023-03-15T00:00", new ArrowType.Date(DateUnit.DAY));
        assertEquals("2023-03-15 00:00:00", result);
    }

    @Test
    void testToPredicateWithUnsupportedType() {
        assertThrows(UnsupportedOperationException.class, () ->
                queryBuilder.getObjectForWhereClause("unsupported", "value", new ArrowType.Struct())
        );
    }
}
