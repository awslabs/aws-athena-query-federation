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

import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

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

    @Test
    public void withQueryPlan_SimpleClause() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRrsAxLpAwrZAzrWAwoFEgMKARYSwAMSvQMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaJhokCAEaBAoCEAEiDBoKEggKBBICCAMiACIMGgoKCGIGRU1QMDAxGgoSCAoEEgIIAyIAEgtFTVBMT1lFRV9JRDILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_NotClause() {
        // SQL: SELECT employee_name,bonus FROM basic_write_nonexist WHERE bonus != '5000'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSGRoXCAEQARoRbm90X2VxdWFsOmFueV9hbnka0wMS0AMKtwM6tAMKBhIECgITFBKRAxKOAwoCCgAS4QIK3gIKAgoAErcCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKFAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQ6AhABCgQ6AhABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABGAI6HgoGcHVibGljChRiYXNpY193cml0ZV9ub25leGlzdBokGiIIARoECgIQASIMGgoSCAoEEgIICSIAIgoaCAoGYgQ1MDAwGgoSCAoEEgIIAiIAGgoSCAoEEgIICSIAEg1FTVBMT1lFRV9OQU1FEgVCT05VUzILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_name\", \"bonus\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"bonus\" <> '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectFewColumns() {
        // SQL: SELECT employee_id, employee_name, salary FROM basic_write_nonexist
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "GusDEugDCsEDOr4DCgcSBQoDFhcYEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaChIICgQSAggDIgAaChIICgQSAggFIgAaChIICgQSAggLIgASC0VNUExPWUVFX0lEEg1FTVBMT1lFRV9OQU1FEgZTQUxBUlkyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);

        String expectedQuery =
                "SELECT \"employee_id\", \"employee_name\", \"salary\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_WithLimit() {
        // SQL: SELECT employee_id, employee_name FROM basic_write_nonexist LIMIT 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "GrUDErIDCpMDGpADCgIKABKFAzqCAwoGEgQKAhMUEuECCt4CCgIKABK3AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2UShQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaCBIGCgISACIAGgoSCAoEEgIIAiIAGAAgChILRU1QTE9ZRUVfSUQSDUVNUExPWUVFX05BTUUyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\", \"employee_name\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "LIMIT 10";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_WithOrderBy() {
        // SQL: SELECT employee_id, salary FROM basic_write_nonexist ORDER BY salary DESC
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "GroDErcDCp8DKpwDCgIKABKFAzqCAwoGEgQKAhMUEuECCt4CCgIKABK3AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2UShQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaCBIGCgISACIAGgoSCAoEEgIICCIAGg4KChIICgQSAggBIgAQAxILRU1QTE9ZRUVfSUQSBlNBTEFSWTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);

        String expectedQuery =
                "SELECT \"employee_id\", \"salary\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "ORDER BY \"salary\" DESC";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithStringFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRq7AxK4AwqoAzqlAwoFEgMKARMSkQMSjgMKAgoAEuECCt4CCgIKABK3AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2UShQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaJBoiCAEaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lEMgsQSioHaXN0aG11cw");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithNumericFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE hash1 > 1000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAEQARoKZ3Q6YW55X2FueRrnAxLkAwrUAzrRAwoFEgMKARYSuwMSuAMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaIRofCAEaBAoCEAEiDBoKEggKBBICCA0iACIHGgUKAzjoBxoKEggKBBICCAMiABILRU1QTE9ZRUVfSUQyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"hash1\" > 1000";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithLimitAndOffset() {
        // SQL: SELECT employee_id FROM basic_write_nonexist LIMIT 5 OFFSET 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "GpkDEpYDCoYDGoMDCgIKABL4Ajr1AgoFEgMKARMS4QIK3gIKAgoAErcCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKFAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQ6AhABCgQ6AhABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABGAI6HgoGcHVibGljChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAYCiAFEgtFTVBMT1lFRV9JRDILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "OFFSET 10\n" +
                        "LIMIT 5";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectHavingTimestampzField() {
        // SQL : SELECT "timestamp" FROM "basic_write_nonexist"
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "GosDEogDCvoCOvcCCgUSAwoBExLhAgreAgoCCgAStwIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEoUBCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWCAQIQAQoFigICGAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoeCgZwdWJsaWMKFGJhc2ljX3dyaXRlX25vbmV4aXN0GgoSCAoEEgIIBiIAEgl0aW1lc3RhbXAyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithDecimalFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE debit = 500
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRqOBBKLBAr7Azr4AwoFEgMKARYS4gMS3wMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaSBpGCAEaBAoCEAEiHRobWhkKCcIBBggCEAwgARIKEggKBBICCBAiABgCIh0aGwoZwgEWChBQwwAAAAAAAAAAAAAAAAAAEAwYAhoKEggKBBICCAMiABILRU1QTE9ZRUVfSUQyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE CAST(\"debit\" AS DECIMAL(12, 2)) = 500.00";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithAndClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' AND salary > 5000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBIVGhMIAhACGg1lcXVhbDphbnlfYW55EhIaEAgCEAMaCmd0OmFueV9hbnkahgcSgwcK0QU6zgUKFxIVChMTFBUWFxgZGhscHR4fICEiIyQlEtADEs0DCgIKABLhAgreAgoCCgAStwIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEoUBCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWCAQIQAQoFigICGAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoeCgZwdWJsaWMKFGJhc2ljX3dyaXRlX25vbmV4aXN0GmMaYQgBGgQKAhABIiYaJBoiCAIaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMSIvGi0aKwgDGgQKAhABIhgaFloUCgQqAhABEgoSCAoEEgIICCIAGAIiBxoFCgMoiCcaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" = 'EMP001'AND \"salary\" > '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithOrClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' OR employee_id = 'EMP002'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIPGg0IARABGgdvcjpib29sEhUaEwgCEAIaDWVxdWFsOmFueV9hbnka/QYS+gYKyAU6xQUKFxIVChMTFBUWFxgZGhscHR4fICEiIyQlEscDEsQDCgIKABLhAgreAgoCCgAStwIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEoUBCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWCAQIQAQoFigICGAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoeCgZwdWJsaWMKFGJhc2ljX3dyaXRlX25vbmV4aXN0GloaWAgBGgQKAhABIiYaJBoiCAIaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMSImGiQaIggCGgQKAhABIgoaCBIGCgISACIAIgwaCgoIYgZFTVAwMDIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" IN ('EMP001', 'EMP002')";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectTimestampField() {
        // SQL: SELECT employee_id, "timestamp", salary FROM basic_write_nonexist;
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "GrgDErUDCpIDOo8DCgcSBQoDExQVEuECCt4CCgIKABK3AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2UShQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaCBIGCgISACIAGgoSCAoEEgIIBiIAGgoSCAoEEgIICCIAEgtFTVBMT1lFRV9JRBIJdGltZXN0YW1wEgZTQUxBUlkyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"salary\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithComplexFilter() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE (employee_id = 'EMP001' AND salary > 5000) OR (employee_id = 'EMP002' AND salary < 3000)
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIPGg0IARABGgdvcjpib29sEhAaDggBEAIaCGFuZDpib29sEhUaEwgCEAMaDWVxdWFsOmFueV9hbnkSEhoQCAIQBBoKZ3Q6YW55X2FueRISGhAIAhAFGgpsdDphbnlfYW55Gv0HEvoHCsgGOsUGChcSFQoTExQVFhcYGRobHB0eHyAhIiMkJRLHBBLEBAoCCgAS4QIK3gIKAgoAErcCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKFAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQ6AhABCgQ6AhABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABGAI6HgoGcHVibGljChRiYXNpY193cml0ZV9ub25leGlzdBrZARrWAQgBGgQKAhABImUaYxphCAIaBAoCEAEiJhokGiIIAxoECgIQASIKGggSBgoCEgAiACIMGgoKCGIGRU1QMDAxIi8aLRorCAQaBAoCEAEiGBoWWhQKBCoCEAESChIICgQSAggIIgAYAiIHGgUKAyiIJyJlGmMaYQgCGgQKAhABIiYaJBoiCAMaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMiIvGi0aKwgFGgQKAhABIhgaFloUCgQqAhABEgoSCAoEEgIICCIAGAIiBxoFCgMouBcaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" = 'EMP001' AND \"salary\" > '5000'OR \"employee_id\" = 'EMP002' AND \"salary\" < '3000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithCount() {
        // SQL: SELECT COUNT(*) OVER() AS TotalCount, * FROM "basic_write_nonexist"
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "CiUIARIhL2Z1bmN0aW9uc19hZ2dyZWdhdGVfZ2VuZXJpYy55YW1sEg4aDAgBEAEaBmNvdW50Ohq+BhK7Bgr9BDr6BAoYEhYKFBMUFRYXGBkaGxwdHh8gISIjJCUmEuECCt4CCgIKABK3AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2UShQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaGCoWCAEiAiIAKgIiADADOgQ6AhACUAFgAhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgASClRPVEFMQ09VTlQSC2VtcGxveWVlX2lkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEgl0aW1lc3RhbXASCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0EgVjb3VudBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNlMgsQSioHaXN0aG11cw==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f19\", \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_CountExpression() {
        // SQL : SELECT COUNT(*) FROM basic_write_nonexist
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "CiUIARIhL2Z1bmN0aW9uc19hZ2dyZWdhdGVfZ2VuZXJpYy55YW1sEg4aDAgBEAEaBmNvdW50OhqLAxKIAwr9AiL6AgoCCgAS4QIK3gIKAgoAErcCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKFAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQ6AhABCgQ6AhABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABGAI6HgoGcHVibGljChRiYXNpY193cml0ZV9ub25leGlzdBoAIg4KDAgBIAMqBDoCEAIwARIGRVhQUiQwMgsQSioHaXN0aG11cw==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT COUNT(*) AS \"$f0\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DoubleFilter() {
        // SQL : SELECT price FROM basic_write_nonexist WHERE price >= 99.99
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEQARoLZ3RlOmFueV9hbnka3wMS3AMK0gM6zwMKBRIDCgEUErsDErgDCgIKABLuAgrrAgoCCgASxAIKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKLAQoEWgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQ6AhABCgQ6AhABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABGAI6HgoGcHVibGljChRiYXNpY193cml0ZV9ub25leGlzdBpBGj8IARoECgIQASIKGggSBgoCEgAiACIpGidaJQoEWgIQAhIbChnCARYKEA8nAAAAAAAAAAAAAAAAAAAQBBgCGAIaCBIGCgISACIAEgVQUklDRTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"price\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"price\" >= 99.99";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_BooleanFilter() {
        // SQL : SELECT * FROM basic_write_nonexist WHERE is_active=false
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRrDBhLABgqOBTqLBQoXEhUKExMUFRYXGBkaGxwdHh8gISIjJCUSjQMSigMKAgoAEuECCt4CCgIKABK3AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2UShQEKBGICEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaIBoeCAEaBAoCEAEiDBoKEggKBBICCAEiACIGGgQKAggAGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABILZW1wbG95ZWVfaWQSCWlzX2FjdGl2ZRINZW1wbG95ZWVfbmFtZRIJam9iX3RpdGxlEgdhZGRyZXNzEglqb2luX2RhdGUSCXRpbWVzdGFtcBIIZHVyYXRpb24SBnNhbGFyeRIFYm9udXMSBWhhc2gxEgVoYXNoMhIEY29kZRIFZGViaXQSBWNvdW50EgZhbW91bnQSB2JhbGFuY2USBHJhdGUSCmRpZmZlcmVuY2UyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE NOT \"is_active\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_FloatFilter() {
        // SQL : SELECT * FROM basic_write_nonexist WHERE float_value < 500
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAEQARoKbHQ6YW55X2FueRqcBxKZBwrTBTrQBQoZEhcKFRUWFxgZGhscHR4fICEiIyQlJicoKRK4AxK1AwoCCgASgQMK/gIKAgoAEtcCCgtmbG9hdF92YWx1ZQoFcHJpY2UKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEpEBCgRaAhABCgRaAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWCAQIQAQoFigICGAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoeCgZwdWJsaWMKFGJhc2ljX3dyaXRlX25vbmV4aXN0GisaKQgBGgQKAhABIgoaCBIGCgISACIAIhMaEVoPCgRaAhACEgUKAyj0AxgCGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABoKEggKBBICCBQiABILZmxvYXRfdmFsdWUSBXByaWNlEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"float_value\", \"price\", \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"float_value\" < 500.0";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DecimalFilter() {
        // SQL : SELECT debit FROM basic_write_nonexist WHERE debit > 10000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAEQARoKZ3Q6YW55X2FueRr7AxL4AwruAzrrAwoFEgMKARUS1QMS0gMKAgoAEoEDCv4CCgIKABLXAgoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKRAQoEWgIQAQoEWgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQ6AhABCgQ6AhABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABGAI6HgoGcHVibGljChRiYXNpY193cml0ZV9ub25leGlzdBpIGkYIARoECgIQASIdGhtaGQoJwgEGCAIQDCABEgoSCAoEEgIIDyIAGAIiHRobChnCARYKEEBCDwAAAAAAAAAAAAAAAAAQDBgCGgoSCAoEEgIIDyIAEgVERUJJVDILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"debit\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE CAST(\"debit\" AS DECIMAL(12, 2)) > 10000.00";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DateDayFilter() {
        // SQL : SELECT "date" FROM basic_write_nonexist WHERE "date"= DATE'2023-02-01'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRreAxLbAwrSAzrPAwoFEgMKARYSuwMSuAMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaIRofCAEaBAoCEAEiChoIEgYKAhIAIgAiCRoHCgWAAb2XARoIEgYKAhIAIgASBGRhdGUyCxBKKgdpc3RobXVz");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery = "SELECT \"date\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"date\" = 19389";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DateMillisecondFilter() {
        // SQL : SELECT * FROM basic_write_nonexist WHERE "timestamp" = TIMESTAMP'2023-01-01 00:00:00';
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRrFBxLCBwr2BTrzBQoaEhgKFhYXGBkaGxwdHh8gISIjJCUmJygpKisSzgMSywMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaNBoyCAEaBAoCEAEiDBoKEggKBBICCAkiACIaGhhaFgoFigICGAESCwoJcICAtaCIpfwCGAIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAGgoSCAoEEgIIFCIAGgoSCAoEEgIIFSIAEgRkYXRlEgtmbG9hdF92YWx1ZRIFcHJpY2USC2VtcGxveWVlX2lkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEgl0aW1lc3RhbXASCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0EgVjb3VudBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNlMgsQSioHaXN0aG11cw==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery = "SELECT \"date\", \"float_value\", \"price\", \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"timestamp\" = 1672531200000";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SimpleColumnWithoutAlias() {
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "mock_plan_string");

        try (MockedStatic<SubstraitSqlUtils> mockedSubstrait = Mockito.mockStatic(SubstraitSqlUtils.class)) {

            // Create SqlSelect with SqlIdentifier (no alias)
            SqlIdentifier employeeName = new SqlIdentifier("employee_name", SqlParserPos.ZERO);
            SqlIdentifier joinDate = new SqlIdentifier("join_date", SqlParserPos.ZERO);
            SqlNodeList selectList = new SqlNodeList(java.util.Arrays.asList(employeeName, joinDate), SqlParserPos.ZERO);
            SqlIdentifier tableName = new SqlIdentifier("basic_write_nonexist", SqlParserPos.ZERO);

            SqlSelect mockSqlSelect = new SqlSelect(
                    SqlParserPos.ZERO,
                    null, selectList, tableName, null, null, null, null, null, null, null, null
            );

            java.util.List<Field> fields = java.util.Arrays.asList(
                    new Field("employee_name", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null),
                    new Field("join_date", new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null ), null)
            );
            Schema mockSchema = new Schema(fields);
            mockedSubstrait.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(Mockito.anyString(), Mockito.any(SqlDialect.class)))
                    .thenReturn(mockSqlSelect);
            mockedSubstrait.when(() -> SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(Mockito.anyString(), Mockito.any(SqlDialect.class)))
                    .thenReturn(mockSchema);
            VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
            String result = queryBuilder.getQueryFromPlan();
            assertEquals("SELECT \"employee_name\", CAST(\"join_date\" AS VARCHAR) AS \"join_date\"\n" +
                    "FROM \"basic_write_nonexist\"", result);
        }
    }

    /**
     * Utility method for mocking ResultSet behavior.
     */
    private void mockResultSet(String[] columnNames, String[] typeNames) throws Exception {
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("COLUMN_NAME")).thenReturn(columnNames[0], columnNames[1]);
        when(resultSet.getString("TYPE_NAME")).thenReturn(typeNames[0], typeNames[1]);
    }

    private ST createValidTemplate() {
        STGroup stGroup = new STGroupFile("Vertica.stg");
        return stGroup.getInstanceOf("templateVerticaExportSubstraitQuery");
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
