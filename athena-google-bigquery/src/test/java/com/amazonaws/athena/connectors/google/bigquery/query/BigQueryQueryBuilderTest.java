/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery.query;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.google.cloud.bigquery.QueryParameterValue;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BigQueryQueryBuilderTest
{
    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final String TEST_TABLE_NAME = "test_table";
    private static final TableName TEST_TABLE = new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);
    
    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    private static final ArrowType BIGINT_TYPE = new ArrowType.Int(64, false);
    private static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_ACTIVE = "active";
    private static final String COLUMN_SCORE = "score";
    
    private static final int TEST_LIMIT_VALUE = 100;

    
    private static final String SQL_SHOULD_CONTAIN_SELECT = "SQL should contain SELECT";
    private static final String SQL_SHOULD_CONTAIN_ALL_COLUMNS = "SQL should contain all columns";
    private static final String SQL_SHOULD_CONTAIN_FROM_CLAUSE = "SQL should contain FROM clause";

    private BlockAllocator allocator;
    private BigQueryQueryFactory queryFactory;
    private Schema testSchema;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        queryFactory = new BigQueryQueryFactory();
        testSchema = createTestSchema();
    }

    @After
    public void tearDown()
    {
            allocator.close();
    }

    @Test
    public void shouldLoadStringTemplateSuccessfully()
    {
        String templateName = BigQueryQueryBuilder.getTemplateName();
        
        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should match", "select_query", templateName);
    }

    @Test
    public void shouldGenerateBasicSelectQueryWithProjectionAndTableName()
    {
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withProjection(testSchema)
                .withTableName(TEST_TABLE);
        
        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertEquals("SQL should match expected format", "SELECT `id`,`name`,`active`,`score` from `test_schema`.`test_table`", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`id`"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`name`"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`active`"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`score`"));
        assertTrue(SQL_SHOULD_CONTAIN_FROM_CLAUSE, sql.contains("from `test_schema`.`test_table`"));
    }

    @Test
    public void shouldGenerateSelectQueryWithOrderByClause()
    {
        List<OrderByField> orderByFields = createOrderByFields();
        Constraints constraints = createConstraintsWithOrderBy(orderByFields);
        
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withProjection(testSchema)
                .withTableName(TEST_TABLE)
                .withOrderByClause(constraints);
        
        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain ORDER BY clause", sql.contains("ORDER BY"));
        assertTrue("SQL should contain name ordering", sql.contains("`name` ASC NULLS FIRST"));
        assertTrue("SQL should contain score ordering", sql.contains("`score` DESC NULLS LAST"));
    }

    @Test
    public void shouldGenerateSelectQueryWithLimitClause()
    {
        Constraints constraints = createConstraintsWithLimit();
        
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withProjection(testSchema)
                .withTableName(TEST_TABLE)
                .withLimitClause(constraints);
        
        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain LIMIT clause", sql.contains("LIMIT"));
        assertTrue("SQL should contain limit value", sql.contains(String.valueOf(TEST_LIMIT_VALUE)));
    }

    @Test
    public void shouldGenerateSelectQueryWithEmptyProjection()
    {
        Schema emptySchema = new Schema(Collections.emptyList());
        
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withProjection(emptySchema)
                .withTableName(TEST_TABLE);
        
        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue(SQL_SHOULD_CONTAIN_FROM_CLAUSE, sql.contains("from `test_schema`.`test_table`"));
    }

    @Test
    public void shouldReturnCorrectValuesFromBuilderGetters()
    {
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withProjection(testSchema)
                .withTableName(TEST_TABLE);

        assertEquals("Schema name should match", TEST_SCHEMA_NAME, builder.getSchemaName());
        assertEquals("Table name should match", TEST_TABLE_NAME, builder.getTableName());
        assertEquals("Projection should have correct number of columns", 4, builder.getProjection().size());
        assertTrue("Projection should contain id", builder.getProjection().contains(COLUMN_ID));
        assertTrue("Projection should contain name", builder.getProjection().contains(COLUMN_NAME));
        assertTrue("Projection should contain active", builder.getProjection().contains(COLUMN_ACTIVE));
        assertTrue("Projection should contain score", builder.getProjection().contains(COLUMN_SCORE));

        List<QueryParameterValue> parameters = builder.getParameterValues();
        assertNotNull("Parameters should not be null", parameters);
    }

    @Test
    public void shouldBuildQuerySuccessfullyWithAllComponents()
    {
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withProjection(testSchema)
                .withTableName(TEST_TABLE);
        
        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue("SQL should contain schema name", sql.contains(TEST_SCHEMA_NAME));
        assertTrue("SQL should contain table name", sql.contains(TEST_TABLE_NAME));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`id`"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`name`"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`active`"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("`score`"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenBuilderCreatedWithNullTemplate()
    {
        new BigQueryQueryBuilder(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenBuildingWithoutSchemaName()
    {
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withProjection(testSchema);
        builder.build();
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenCreatingTableNameWithNullValue()
    {
        new TableName(null, TEST_TABLE_NAME);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenBuildingWithoutProjection()
    {
        BigQueryQueryBuilder builder = queryFactory.createQueryBuilder()
                .withTableName(TEST_TABLE);
        builder.build();
    }

    private Schema createTestSchema()
    {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(COLUMN_ID, new FieldType(true, BIGINT_TYPE, null), null));
        fields.add(new Field(COLUMN_NAME, new FieldType(true, STRING_TYPE, null), null));
        fields.add(new Field(COLUMN_ACTIVE, new FieldType(true, BOOLEAN_TYPE, null), null));
        fields.add(new Field(COLUMN_SCORE, new FieldType(true, BIGINT_TYPE, null), null));
        return new Schema(fields);
    }

    private List<OrderByField> createOrderByFields()
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField(COLUMN_SCORE, OrderByField.Direction.DESC_NULLS_LAST));
        return orderByFields;
    }

    private Constraints createConstraintsWithOrderBy(List<OrderByField> orderByFields)
    {
        return new Constraints(new HashMap<>(), Collections.emptyList(), orderByFields, DEFAULT_NO_LIMIT,Collections.emptyMap(), null);
    }

    private Constraints createConstraintsWithLimit()
    {
        return new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), TEST_LIMIT_VALUE,Collections.emptyMap(), null);
    }
} 