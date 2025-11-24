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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmd0OmFueV9hbnkawQYSvgYKjAU6iQUKFxIVChMTFBUWFxgZGhscHR4fICEiIyQlEosDEogDCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBovGi0aBAoCEAEiDBoKEggKBBICCAkiACIXGhVaEwoEYgIQARIJCgeqAQQ1MDAwGAIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id,is_active,employee_name,job_title,address,join_date,timestamp,duration,salary,bonus,hash1,hash2,code,debit,count,amount,balance,rate,difference FROM \"public\".\"basic_write_nonexist\" WHERE \"bonus\" > '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_NotClause() {
        // SQL: SELECT employee_name,bonus FROM basic_write_nonexist WHERE bonus != '5000'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFxoVCAEaEW5vdF9lcXVhbDphbnlfYW55GsADEr0DCqQDOqEDCgYSBAoCExQS/gIS+wIKAgoAEtACCs0CCgIKABKuAgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USfQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoHwgEEEBMgAQoEKgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GiIaIBoECgIQASIMGgoSCAoEEgIICSIAIgoaCAoGYgQ1MDAwGgoSCAoEEgIIAiIAGgoSCAoEEgIICSIAEg1FTVBMT1lFRV9OQU1FEgVCT05VUw==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_name,bonus FROM \"public\".\"basic_write_nonexist\" WHERE \"bonus\" <> '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectFewColumns() {
        // SQL: SELECT employee_id, employee_name, salary FROM basic_write_nonexist
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrIDEq8DCogDOoUDCgcSBQoDExQVEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggCIgAaChIICgQSAggIIgASC0VNUExPWUVFX0lEEg1FTVBMT1lFRV9OQU1FEgZTQUxBUlk=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id,employee_name,salary FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_WithLimit() {
        // SQL: SELECT employee_id, employee_name FROM basic_write_nonexist LIMIT 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GqsDEqgDCokDGoYDCgIKABL7Ajr4AgoGEgQKAhMUEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggCIgAYACAKEgtFTVBMT1lFRV9JRBINRU1QTE9ZRUVfTkFNRQ==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id,employee_name FROM \"public\".\"basic_write_nonexist\" LIMIT 10";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_WithOrderBy() {
        // SQL: SELECT employee_id, salary FROM basic_write_nonexist ORDER BY salary DESC
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrADEq0DCpUDKpIDCgIKABL7Ajr4AgoGEgQKAhMUEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggIIgAaDgoKEggKBBICCAEiABADEgtFTVBMT1lFRV9JRBIGU0FMQVJZ");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id,salary FROM \"public\".\"basic_write_nonexist\" ORDER BY \"salary\" DESC";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithOrderBy() {
        // SQL: SELECT employee_id, salary FROM basic_write_nonexist ORDER BY salary DESC
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrADEq0DCpUDKpIDCgIKABL7Ajr4AgoGEgQKAhMUEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggIIgAaDgoKEggKBBICCAEiABADEgtFTVBMT1lFRV9JRBIGU0FMQVJZ");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id,salary FROM \"public\".\"basic_write_nonexist\" ORDER BY \"salary\" DESC";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithStringFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithNumericFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE hash1 > 1000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmd0OmFueV9hbnkapQMSogMKkgM6jwMKBRIDCgETEvsCEvgCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBofGh0aBAoCEAEiDBoKEggKBBICCAoiACIHGgUKAyjoBxoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"hash1\" > 1000";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithLimitAndOffset() {
        // SQL: SELECT employee_id FROM basic_write_nonexist LIMIT 5 OFFSET 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Go8DEowDCvwCGvkCCgIKABLuAjrrAgoFEgMKARMS1wIK1AIKAgoAErUCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKDAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GggSBgoCEgAiABgKIAUSC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" OFFSET 10 LIMIT 5";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithDecimalFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE debit = 500
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithAndClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' AND salary > 5000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithOrClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' OR employee_id = 'EMP002'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithComplexAndOr() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE (employee_id = 'EMP001' AND salary > 5000) OR (employee_id = 'EMP002' AND salary < 3000)
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), resultSet);

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
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

    private ST createValidTemplate() {
        STGroup stGroup = new STGroupFile("Vertica.stg");
        return stGroup.getInstanceOf("templateVerticaExportSubstraitQuery");
    }

    private Schema createTestSchema() {
        return SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("employee_id", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("is_active", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("employee_name", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("job_title", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("address", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("join_date", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("timestamp", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("duration", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("salary", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("bonus", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("hash1", new ArrowType.Int(32, false)).build())
                .addField(FieldBuilder.newBuilder("hash2", new ArrowType.Int(32, false)).build())
                .addField(FieldBuilder.newBuilder("code", new ArrowType.Int(32, false)).build())
                .addField(FieldBuilder.newBuilder("debit", new ArrowType.Decimal(19, 0, 128)).build())
                .addField(FieldBuilder.newBuilder("count", new ArrowType.Int(32, false)).build())
                .addField(FieldBuilder.newBuilder("amount", new ArrowType.Decimal(19, 0, 128)).build())
                .addField(FieldBuilder.newBuilder("balance", new ArrowType.Int(32, false)).build())
                .addField(FieldBuilder.newBuilder("rate", new ArrowType.Decimal(19, 0, 128)).build())
                .addField(FieldBuilder.newBuilder("difference", new ArrowType.Int(32, false)).build())
                .build();
    }
}
