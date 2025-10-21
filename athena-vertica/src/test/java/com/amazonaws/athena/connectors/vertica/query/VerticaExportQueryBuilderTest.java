/*-
 * #%L
 * athena-vertica
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
package com.amazonaws.athena.connectors.vertica.query;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.ST;

import java.sql.ResultSet;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class VerticaExportQueryBuilderTest {

    private static final String TEST_QUERY_ID = "query-123";
    private static final String TEST_BUCKET = "s3://test-bucket";
    private static final String PREPARED_SQL = "SELECT * FROM prepared_table";
    private static final String TEMPLATE_NAME = "templateVerticaExportQuery";
    private static final String QPT_TEMPLATE_NAME = "templateVerticaExportQPTQuery";
    private static final String EXPECTED_SQL_TEMPLATE = "SELECT <colNames> FROM <table> TO <s3ExportBucket> QUERYID <queryID>";

    @Mock private ST template;
    @Mock private ResultSet resultSet;
    @Mock private Schema schema;

    private VerticaExportQueryBuilder builder;

    @Before
    public void setUp() {
        builder = new VerticaExportQueryBuilder(template);
        when(template.render()).thenReturn(EXPECTED_SQL_TEMPLATE);
    }

    @Test
    public void constructor_NullTemplate_ShouldThrowException() {
        assertThrows(NullPointerException.class, () -> new VerticaExportQueryBuilder(null));
    }

    @Test
    public void getTemplateName_ShouldReturnExpectedName() {
        assertEquals(TEMPLATE_NAME, VerticaExportQueryBuilder.getTemplateName());
    }

    @Test
    public void getQptTemplateName_ShouldReturnExpectedName() {
        assertEquals(QPT_TEMPLATE_NAME, VerticaExportQueryBuilder.getQptTemplateName());
    }

    @Test
    public void withPreparedStatementSQL_ValidSQL_ShouldSetCorrectly() {
        builder.withPreparedStatementSQL(PREPARED_SQL);
        assertEquals(PREPARED_SQL, builder.getPreparedStatementSQL());
    }

    @Test
    public void withColumns_NoTimestamp_ShouldSetCorrectColumnNames() throws Exception {
        mockResultSet(new String[]{"id", "name"}, new String[]{"integer", "varchar"});
        mockSchema(new String[]{"id", "name"});

        builder.withColumns(resultSet, schema);
        assertEquals("id,name", builder.getColNames());
    }

    @Test
    public void withColumns_WithTimestamp_ShouldTransformTimestampCorrectly() throws Exception {
        mockResultSet(new String[]{"id", "created_at"}, new String[]{"integer", "timestamp"});
        mockSchema(new String[]{"id", "created_at"});

        builder.withColumns(resultSet, schema);
        assertEquals("id,CAST(created_at AS VARCHAR) AS created_at", builder.getColNames());
    }

    @Test
    public void withS3ExportBucket_ValidBucket_ShouldSetCorrectly() {
        builder.withS3ExportBucket(TEST_BUCKET);
        assertEquals(TEST_BUCKET, builder.getS3ExportBucket());
    }

    @Test
    public void withQueryID_ValidQueryID_ShouldSetCorrectly() {
        builder.withQueryID(TEST_QUERY_ID);
        assertEquals(TEST_QUERY_ID, builder.getQueryID());
    }

    @Test
    public void buildSetAwsRegionSql_ValidRegion_ShouldReturnExpectedStatement() {
        assertEquals("ALTER SESSION SET AWSRegion='us-west-2'", builder.buildSetAwsRegionSql("us-west-2"));
    }

    @Test
    public void buildSetAwsRegionSql_NullOrEmpty_ShouldReturnDefaultRegion() {
        assertEquals("ALTER SESSION SET AWSRegion='us-east-1'", builder.buildSetAwsRegionSql(null));
        assertEquals("ALTER SESSION SET AWSRegion='us-east-1'", builder.buildSetAwsRegionSql(""));
    }

    @Test
    public void build_ValidSetup_ShouldReturnExpectedTemplate() {
        builder.withPreparedStatementSQL(PREPARED_SQL)
                .withS3ExportBucket(TEST_BUCKET)
                .withQueryID(TEST_QUERY_ID);

        String result = builder.build();
        assertEquals(EXPECTED_SQL_TEMPLATE, result);
    }

    @Test
    public void withPreparedStatementSQL_EmptySQL_ShouldSetEmpty() {
        builder.withPreparedStatementSQL("");
        assertEquals("Empty SQL should be set correctly", "", builder.getPreparedStatementSQL());
        assertNotNull("Prepared SQL should not be null", builder.getPreparedStatementSQL());
    }

    @Test
    public void withS3ExportBucket_EmptyBucket_ShouldSetEmpty() {
        builder.withS3ExportBucket("");
        assertEquals("Empty bucket should be set correctly", "", builder.getS3ExportBucket());
        assertNotNull("S3 export bucket should not be null", builder.getS3ExportBucket());
    }

    @Test
    public void withQueryID_EmptyQueryID_ShouldSetEmpty() {
        builder.withQueryID("");
        assertEquals("Empty query ID should be set correctly", "", builder.getQueryID());
        assertNotNull("Query ID should not be null", builder.getQueryID());
    }

    @Test(expected = NullPointerException.class)
    public void withColumns_NullResultSet_ShouldThrowException() throws Exception {
        mockSchema(new String[]{"id", "name"});
        builder.withColumns(null, schema);
    }

    @Test(expected = NullPointerException.class)
    public void withColumns_NullSchema_ShouldThrowException() throws Exception {
        mockResultSet(new String[]{"id", "name"}, new String[]{"integer", "varchar"});
        builder.withColumns(resultSet, null);
    }

    @Test
    public void fromTable_ValidSchemaAndTable_ShouldSetTable() {
        builder.fromTable("testSchema", "testTable");
        assertNotNull("Table should not be null", builder.getTable());
        assertTrue("Table should be a string", builder.getTable() instanceof String);
        String table = builder.getTable();
        assertFalse("Table should contain schema and table info", table.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void fromTable_NullTable_ShouldThrowException() {
        builder.fromTable("testSchema", null);
    }

    @Test
    public void fromTable_EmptySchemaAndTable() {
        builder.fromTable("", "");
        assertNotNull("Table should not be null even with empty inputs", builder.getTable());
        assertTrue("Table should be a string", builder.getTable() instanceof String);
        String table = builder.getTable();
        assertNotNull("Table string should not be null", table);
    }

    /**
     * Utility method for mocking ResultSet behavior.
     */
    private void mockResultSet(String[] columnNames, String[] typeNames) throws Exception {
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("COLUMN_NAME")).thenReturn(columnNames[0], columnNames[1]);
        when(resultSet.getString("TYPE_NAME")).thenReturn(typeNames[0], typeNames[1]);
    }

    /**
     * Utility method for mocking Schema behavior.
     */
    private void mockSchema(String[] fieldNames) {
        Field field1 = mock(Field.class);
        Field field2 = mock(Field.class);
        when(field1.getName()).thenReturn(fieldNames[0]);
        when(field2.getName()).thenReturn(fieldNames[1]);
        when(schema.getFields()).thenReturn(Arrays.asList(field1, field2));
    }
}
