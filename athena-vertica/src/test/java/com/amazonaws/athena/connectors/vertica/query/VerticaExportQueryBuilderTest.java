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

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.Ranges;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.substrait.SubstraitTypeAndValue;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        assertEquals("\"id\",\"name\"", builder.getColNames());
    }

    @Test
    public void withColumns_WithTimestamp_ShouldTransformTimestampCorrectly() throws Exception {
        mockResultSet(new String[]{"id", "created_at"}, new String[]{"integer", "timestamp"});
        mockSchema(new String[]{"id", "created_at"});

        builder.withColumns(resultSet, schema);
        assertEquals("\"id\",CAST(\"created_at\" AS VARCHAR) AS \"created_at\"", builder.getColNames());
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
    }

    @Test
    public void withQueryPlan_WithLimit_OrderBy_WhereClause() {
        // SQL: SELECT * FROM "basic_write_nonexist" where job_title IN ('Project Lead' , 'Software Engineer') AND is_active='true' ORDER BY employee_id limit 2
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBIPGg0IARACGgdvcjpib29sEhUaEwgCEAMaDWVxdWFsOmFueV9hbnkaowkSoAkKwgcavwcKAgoAErQHKrEHCgIKABKaBzqXBwobEhkKFxcYGRobHB0eHyAhIiMkJSYnKCkqKywtEuUEEuIECgIKABKlAwqiAwoCCgAS+wIKCnZhcl9iaW5hcnkKCWludF9maWVsZAoIdGlueV9pbnQKCXNtYWxsX2ludAoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USnQEKBGoCEAEKBCoCEAEKBBICEAEKBBoCEAEKBGICEAEKBAoCEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaswEasAEIARoECgIQASJxGm8abQgCGgQKAhABIi4aLBoqCAMaBAoCEAEiDBoKEggKBBICCAciACISGhAKDmIMUHJvamVjdCBMZWFkIjMaMRovCAMaBAoCEAEiDBoKEggKBBICCAciACIXGhUKE2IRU29mdHdhcmUgRW5naW5lZXIiMxoxGi8IAxoECgIQASIMGgoSCAoEEgIIBSIAIhcaFVoTCgQKAhACEgkKB6oBBHRydWUYAhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgAaChIICgQSAggUIgAaChIICgQSAggVIgAaChIICgQSAggWIgAaDgoKEggKBBICCAQiABACGAAgAhIKdmFyX2JpbmFyeRIJaW50X2ZpZWxkEgh0aW55X2ludBIJc21hbGxfaW50EgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT);
        String expectedQuery =
                "SELECT \"var_binary\", \"int_field\", \"tiny_int\", \"small_int\", \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"job_title\" IN ('Project Lead', 'Software Engineer') AND \"is_active\" = 1\n" +
                        "ORDER BY \"employee_id\"\n" +
                        "LIMIT 2";
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
                        "WHERE \"employee_id\" = 'EMP001' AND \"salary\" > '5000'";
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
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
        String actualResult = queryBuilder.getQueryFromPlan();
        assertEquals(expectedQuery, actualResult);
    }

    private ST createValidTemplate() {
        STGroup stGroup = new STGroupFile("Vertica.stg");
        return stGroup.getInstanceOf("templateVerticaExportSubstraitQuery");
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

    /**
     * Verifies single-quote escaping (doubling) in VARCHAR literals rendered by handleDataTypesForSqlTemplate.
     * Reverting the fix would produce {@code 'O'Brien'} instead of {@code 'O''Brien'}, causing SQL syntax errors.
     */
    @Test
    public void handleDataTypesForSqlTemplate_VarcharWithEmbeddedQuote_ShouldEscapeCorrectly() throws Exception {
        // Arrange: create an ST template with a {param0} placeholder using custom delimiters
        // (matching how withQueryPlan creates templates with '{' and '}' delimiters)
        ST sqlTemplate = new ST("SELECT * FROM t WHERE name = {param0}", '{', '}');

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.VARCHAR, "O'Brien", "name"));

        // Act: call the package-private method directly
        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        // Assert: the rendered SQL must contain the properly escaped value with doubled quotes
        assertTrue("VARCHAR value with embedded single quote must be escaped as doubled quote. Got: " + renderedSql,
                renderedSql.contains("'O''Brien'"));
        assertFalse("Unescaped single quote in VARCHAR literal would cause SQL injection. Got: " + renderedSql,
                renderedSql.equals("SELECT * FROM t WHERE name = 'O'Brien'"));
    }

    /**
     * Verifies VARBINARY renders as a valid {@code X'<hex>'} literal via bytesToVerticaHexLiteral.
     * Without the fix, StringTemplate produces malformed output from raw {@code byte[]} arrays.
     */
    @Test
    public void handleDataTypesForSqlTemplate_Varbinary_ShouldRenderAsValidHexLiteral() throws Exception {
        // Arrange: create an ST template with a {param0} placeholder using custom delimiters
        // (matching how withQueryPlan creates templates with '{' and '}' delimiters)
        ST sqlTemplate = new ST("SELECT * FROM t WHERE payload = {param0}", '{', '}');

        byte[] expectedBytes = {0x01, 0x02, 0x03, (byte) 0xFF};
        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.VARBINARY, expectedBytes, "payload"));

        // Act: call the package-private method directly
        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        // Assert: the rendered SQL must be a valid Vertica X'<hex>' literal with the correct bytes,
        // not StringTemplate's default (malformed) rendering of a raw byte[]
        String expectedSql = "SELECT * FROM t WHERE payload = X'010203FF'";
        assertEquals("VARBINARY parameter must render as a valid Vertica hex literal", expectedSql, renderedSql);
    }

    /**
     * Verifies mixed-type parameter binding (VARCHAR, INTEGER, DECIMAL, DOUBLE, BIGINT)
     * renders correctly in a single StringTemplate pass, including quote-escaping in VARCHAR.
     */
    @Test
    public void handleDataTypesForSqlTemplate_MultipleTypesWithDifferentValues_ShouldBindCorrectly() throws Exception {
        // Arrange: create a template with 6 param placeholders matching the accumulator size
        ST sqlTemplate = new ST(
                "SELECT * FROM t WHERE name = {param0} AND id = {param1} AND amount = {param2} AND rate = {param3} AND total = {param4} AND owner = {param5}",
                '{', '}');

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.VARCHAR, "test_value", "name"));
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.INTEGER, "12345", "id"));
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.DECIMAL, "500.75", "amount"));
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.DOUBLE, "99.99", "rate"));
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.BIGINT, "9876543210", "total"));
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.VARCHAR, "O'Brien", "owner"));

        // Act: call the package-private method directly
        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        // Assert: verify the complete rendered output matches what each type should produce
        String expectedSql = "SELECT * FROM t WHERE name = 'test_value' AND id = 12345 AND amount = 500.75 AND rate = 99.99 AND total = 9876543210 AND owner = 'O''Brien'";
        assertEquals("Multi-type parameters should each render with correct type-specific formatting", expectedSql, renderedSql);

        // Also verify each parameter value individually for clear failure diagnostics
        assertTrue("VARCHAR param should be wrapped in single quotes", renderedSql.contains("name = 'test_value'"));
        assertTrue("INTEGER param should render as unquoted integer", renderedSql.contains("id = 12345"));
        assertTrue("DECIMAL param should render as unquoted decimal", renderedSql.contains("amount = 500.75"));
        assertTrue("DOUBLE param should render as unquoted double", renderedSql.contains("rate = 99.99"));
        assertTrue("BIGINT param should render as unquoted long", renderedSql.contains("total = 9876543210"));
        assertTrue("VARCHAR param with embedded quote should escape correctly alongside other types", renderedSql.contains("owner = 'O''Brien'"));
    }

    @Test
    public void handleDataTypesForSqlTemplate_Timestamp_ShouldRenderTimestampLiteral() throws Exception {
        ST sqlTemplate = new ST("SELECT * FROM t WHERE ts = {param0}", '{', '}');

        String timestampValue = "2024-01-15 10:30:00";
        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.TIMESTAMP, timestampValue, "ts"));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        String expectedSql = "SELECT * FROM t WHERE ts = TIMESTAMP '2024-01-15 10:30:00'";
        assertEquals(expectedSql, renderedSql);
    }

    @Test
    public void handleDataTypesForSqlTemplate_Date_ShouldRenderDateLiteral() throws Exception {
        ST sqlTemplate = new ST("SELECT * FROM t WHERE d = {param0}", '{', '}');

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.DATE, "2024-01-15", "d"));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        String expectedSql = "SELECT * FROM t WHERE d = DATE '2024-01-15'";
        assertEquals(expectedSql, renderedSql);
    }

    @Test
    public void handleDataTypesForSqlTemplate_TimestampFractionalSeconds_ShouldTruncateToMicroseconds() throws Exception {
        ST sqlTemplate = new ST("SELECT * FROM t WHERE ts = {param0}", '{', '}');

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.TIMESTAMP, "2024-01-15 10:30:00.123456789", "ts"));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        String expectedSql = "SELECT * FROM t WHERE ts = TIMESTAMP '2024-01-15 10:30:00.123456'";
        assertEquals(expectedSql, renderedSql);
    }

    @Test
    public void handleDataTypesForSqlTemplate_TimestampIsoT_ShouldRenderTimestampLiteral() throws Exception {
        ST sqlTemplate = new ST("SELECT * FROM t WHERE ts = {param0}", '{', '}');

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.TIMESTAMP, "2024-01-15T10:30:00", "ts"));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        String expectedSql = "SELECT * FROM t WHERE ts = TIMESTAMP '2024-01-15 10:30:00'";
        assertEquals(expectedSql, renderedSql);
    }

    @Test
    public void handleDataTypesForSqlTemplate_TimestampWithLocalTimeZone_ShouldRenderTimestampLiteral() throws Exception {
        ST sqlTemplate = new ST("SELECT * FROM t WHERE ts = {param0}", '{', '}');

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, "2024-01-15 10:30:00", "ts"));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());
        testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate);

        String renderedSql = sqlTemplate.render();

        String expectedSql = "SELECT * FROM t WHERE ts = TIMESTAMP '2024-01-15 10:30:00'";
        assertEquals(expectedSql, renderedSql);
    }

    @Test
    public void handleDataTypesForSqlTemplate_TimestampParseError_ShouldThrowAthenaConnectorException() throws Exception {
        ST sqlTemplate = new ST("SELECT * FROM t WHERE ts = {param0}", '{', '}');

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        accumulator.add(new SubstraitTypeAndValue(SqlTypeName.TIMESTAMP, "not-a-timestamp", "ts"));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(createValidTemplate());

        assertThrows(AthenaConnectorException.class, () ->
                testBuilder.handleDataTypesForSqlTemplate(accumulator, sqlTemplate));
    }

    // ==================== withConstraints (legacy non-substrait path) ====================
    // Backward-compatibility coverage for the legacy (non-substrait) withConstraints predicate
    // rendering: an empty or null constraint summary appends no WHERE clause, and a single
    // equality predicate renders one. All tests here are allocator-free (ValueSet/Range/Marker
    // are mocked).

    @Test
    public void withConstraints_EmptyConstraints_ShouldReturnEmptyString() {
        // Arrange: allocator-free Constraints with empty summary map (no predicates)
        Constraints emptyConstraints = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        Schema testSchema = new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Utf8()),
                Field.nullable("name", new ArrowType.Utf8())));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(template);

        // Act
        testBuilder.withConstraints(emptyConstraints, testSchema);

        // Assert: empty constraints produce empty rendered output (no WHERE clause)
        assertEquals("", testBuilder.getConstraintValues());
    }

    @Test
    public void withConstraints_NullSummary_ShouldReturnEmptyString() {
        // Arrange: Constraints with null summary
        Constraints nullSummaryConstraints = new Constraints(
                null, Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        Schema testSchema = new Schema(Arrays.asList(
                Field.nullable("id", new ArrowType.Utf8()),
                Field.nullable("name", new ArrowType.Utf8())));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(template);

        // Act
        testBuilder.withConstraints(nullSummaryConstraints, testSchema);

        // Assert: null summary produces empty rendered output (no WHERE clause)
        assertEquals("", testBuilder.getConstraintValues());
    }

    @Test
    public void withConstraints_SingleEqualityPredicate_ShouldRenderWhereClause() {
        // Arrange: mock a SortedRangeSet that represents a single equality (id = 42)
        ArrowType intType = new ArrowType.Int(32, true);

        Marker lowMarker = mock(Marker.class);
        when(lowMarker.getValue()).thenReturn(42);

        Range singleRange = mock(Range.class);
        when(singleRange.isSingleValue()).thenReturn(true);
        when(singleRange.getLow()).thenReturn(lowMarker);

        Ranges ranges = mock(Ranges.class);
        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(singleRange));

        SortedRangeSet sortedRangeSet = mock(SortedRangeSet.class);
        when(sortedRangeSet.isNone()).thenReturn(false);
        when(sortedRangeSet.isNullAllowed()).thenReturn(false);
        when(sortedRangeSet.getRanges()).thenReturn(ranges);
        // Span is accessed to check if entire range is unbounded (IS NOT NULL pattern).
        // lenient() is used because getSpan() is only reached conditionally, so these stubs
        // avoid UnnecessaryStubbingException while staying valid if the traversal changes.
        Range spanRange = mock(Range.class);
        Marker spanLow = mock(Marker.class);
        Marker spanHigh = mock(Marker.class);
        Mockito.lenient().when(spanLow.isLowerUnbounded()).thenReturn(false);
        Mockito.lenient().when(spanHigh.isUpperUnbounded()).thenReturn(false);
        Mockito.lenient().when(spanRange.getLow()).thenReturn(spanLow);
        Mockito.lenient().when(spanRange.getHigh()).thenReturn(spanHigh);
        Mockito.lenient().when(sortedRangeSet.getSpan()).thenReturn(spanRange);

        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("id", sortedRangeSet);

        Constraints constraints = new Constraints(
                summary, Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        Schema testSchema = new Schema(Collections.singletonList(
                Field.nullable("id", intType)));

        VerticaExportQueryBuilder testBuilder = new VerticaExportQueryBuilder(template);

        // Act
        testBuilder.withConstraints(constraints, testSchema);

        // Assert: the rendered WHERE clause should contain the equality predicate with value 42
        String result = testBuilder.getConstraintValues();
        // Trailing space before ')' matches PredicateBuilder's rendered output (not a typo).
        assertEquals("WHERE (\"id\" = 42 )", result);
    }
}
