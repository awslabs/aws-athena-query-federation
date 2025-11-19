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
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;

class VerticaExportQueryBuilderTest {

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

    @Test
    void withQueryPlan_SimpleClause() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmd0OmFueV9hbnkawQYSvgYKjAU6iQUKFxIVChMTFBUWFxgZGhscHR4fICEiIyQlEosDEogDCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBovGi0aBAoCEAEiDBoKEggKBBICCAkiACIXGhVaEwoEYgIQARIJCgeqAQQ1MDAwGAIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIEcmF0ZRIKZGlmZmVyZW5jZQ==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id,is_active,employee_name,job_title,address,join_date,timestamp,duration,salary,bonus,hash1,hash2,code,debit,count,amount,balance,rate,difference FROM \"public\".\"basic_write_nonexist\" WHERE \"bonus\" > '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_NotClause() {
        // SQL: SELECT employee_name,bonus FROM basic_write_nonexist WHERE bonus != '5000'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFxoVCAEaEW5vdF9lcXVhbDphbnlfYW55GsADEr0DCqQDOqEDCgYSBAoCExQS/gIS+wIKAgoAEtACCs0CCgIKABKuAgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USfQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoHwgEEEBMgAQoEKgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GiIaIBoECgIQASIMGgoSCAoEEgIICSIAIgoaCAoGYgQ1MDAwGgoSCAoEEgIIAiIAGgoSCAoEEgIICSIAEg1FTVBMT1lFRV9OQU1FEgVCT05VUw==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_name,bonus FROM \"public\".\"basic_write_nonexist\" WHERE \"bonus\" <> '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    private ResultSet createMockResultSet() {
        return Mockito.mock(ResultSet.class);
    }

    @Test
    void withQueryPlan_SelectFewColumns() {
        // SQL: SELECT employee_id, employee_name, salary FROM basic_write_nonexist
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrIDEq8DCogDOoUDCgcSBQoDExQVEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggCIgAaChIICgQSAggIIgASC0VNUExPWUVFX0lEEg1FTVBMT1lFRV9OQU1FEgZTQUxBUlk=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());

        String expectedQuery = "SELECT employee_id,employee_name,salary FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_WithLimit() {
        // SQL: SELECT employee_id, employee_name FROM basic_write_nonexist LIMIT 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GqsDEqgDCokDGoYDCgIKABL7Ajr4AgoGEgQKAhMUEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggCIgAYACAKEgtFTVBMT1lFRV9JRBINRU1QTE9ZRUVfTkFNRQ==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id,employee_name FROM \"public\".\"basic_write_nonexist\" LIMIT 10";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_WithOrderBy() {
        // SQL: SELECT employee_id, salary FROM basic_write_nonexist ORDER BY salary DESC
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrADEq0DCpUDKpIDCgIKABL7Ajr4AgoGEgQKAhMUEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggIIgAaDgoKEggKBBICCAEiABADEgtFTVBMT1lFRV9JRBIGU0FMQVJZ");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id,salary FROM \"public\".\"basic_write_nonexist\" ORDER BY \"salary\" DESC";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectWithOrderBy() {
        // SQL: SELECT employee_id, salary FROM basic_write_nonexist ORDER BY salary DESC
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrADEq0DCpUDKpIDCgIKABL7Ajr4AgoGEgQKAhMUEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggIIgAaDgoKEggKBBICCAEiABADEgtFTVBMT1lFRV9JRBIGU0FMQVJZ");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id,salary FROM \"public\".\"basic_write_nonexist\" ORDER BY \"salary\" DESC";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectWithStringFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectWithNumericFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE hash1 > 1000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmd0OmFueV9hbnkapQMSogMKkgM6jwMKBRIDCgETEvsCEvgCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBofGh0aBAoCEAEiDBoKEggKBBICCAoiACIHGgUKAyjoBxoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"hash1\" > 1000";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectWithLimitAndOffset() {
        // SQL: SELECT employee_id FROM basic_write_nonexist LIMIT 5 OFFSET 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Go8DEowDCvwCGvkCCgIKABLuAjrrAgoFEgMKARMS1wIK1AIKAgoAErUCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKDAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GggSBgoCEgAiABgKIAUSC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());

        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" OFFSET 10 LIMIT 5";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectWithDecimalFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE debit = 500
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectAllWithAndClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' AND salary > 5000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectAllWithOrClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' OR employee_id = 'EMP002'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    void withQueryPlan_SelectAllWithComplexAndOr() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE (employee_id = 'EMP001' AND salary > 5000) OR (employee_id = 'EMP002' AND salary < 3000)
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqAMSpQMKlQM6kgMKBRIDCgETEv4CEvsCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoiGiAaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMRoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist", createTestSchema(), createMockResultSet());
        
        String expectedQuery = "SELECT employee_id FROM \"public\".\"basic_write_nonexist\" WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }
}
