/*-
 * #%L
 * athena-saphana
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.SAPHANA_QUOTE_CHARACTER;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SaphanaQueryStringBuilderTest
{
    @Test
    public void testQueryBuilder()
    {
        Split split = Mockito.mock(Split.class);
        String expectedString1 = " FROM \"default\".\"table\" PARTITION (p0) ";
        String expectedString2 = " FROM \"default\".\"schema\".\"table\" PARTITION (p0) ";
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "p0"));
        Mockito.when(split.getProperty(Mockito.eq(BLOCK_PARTITION_COLUMN_NAME))).thenReturn("p0");
        SaphanaQueryStringBuilder builder = new SaphanaQueryStringBuilder(SAPHANA_QUOTE_CHARACTER, new SaphanaFederationExpressionParser(SAPHANA_QUOTE_CHARACTER));
        String fromClauseWithSplit1 = builder.getFromClauseWithSplit("default", "", "table", split);
        String fromClauseWithSplit2 = builder.getFromClauseWithSplit("default", "schema", "table", split);
        Assert.assertEquals(expectedString1, fromClauseWithSplit1);
        Assert.assertEquals(expectedString2, fromClauseWithSplit2);
    }
    @Test
    public void testGetPartitionWhereClauses()
    {
        List<String> expectedPartitionWhereClauseList1 = new ArrayList<>();
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "p0"));
        Mockito.when(split.getProperty(Mockito.eq(BLOCK_PARTITION_COLUMN_NAME))).thenReturn("p0");

        SaphanaQueryStringBuilder builder = new SaphanaQueryStringBuilder(SAPHANA_QUOTE_CHARACTER, new SaphanaFederationExpressionParser(SAPHANA_QUOTE_CHARACTER));
        List<String> partitionWhereClauseList1 = builder.getPartitionWhereClauses(split);
        Assert.assertEquals(expectedPartitionWhereClauseList1, partitionWhereClauseList1);
    }

    @Test
    public void testGetSqlDialect()
    {
        SaphanaQueryStringBuilder builder = new SaphanaQueryStringBuilder(SAPHANA_QUOTE_CHARACTER, new SaphanaFederationExpressionParser(SAPHANA_QUOTE_CHARACTER));
        Assert.assertEquals(SAPHanaSqlDialect.DEFAULT, builder.getSqlDialect());
    }

    @ParameterizedTest
    @MethodSource("provideSqlTestCases")
    public void testPrepareStatementWithCalciteSql_Success(final String base64EncodedPlan) throws Exception {
        Split split = Mockito.mock(Split.class);
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        QueryPlan queryPlan = new QueryPlan("v1", base64EncodedPlan);
        Constraints constraintsWithQueryPlan = mock(Constraints.class);
        SaphanaQueryStringBuilder builder = new SaphanaQueryStringBuilder(SAPHANA_QUOTE_CHARACTER, new SaphanaFederationExpressionParser(SAPHANA_QUOTE_CHARACTER));

        when(constraintsWithQueryPlan.getQueryPlan()).thenReturn(queryPlan);

        PreparedStatement result = builder.buildSql(mockConnection, "", "", "", null, constraintsWithQueryPlan, split);
        assertNotNull(result);
    }

    private static Stream<Arguments> provideSqlTestCases() {
        return Stream.of(
                // SELECT employee_id, employee_name, job_title, salary FROM "ADMIN"."ORACLE_BASIC_DBTABLE_GAMMA_EU_WEST_1_INTEG";
                Arguments.of("GrgEErUECoMEOoAECggSBgoEFBUWFxLFAwrCAwoCCgAS2gIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlCg5wYXJ0aXRpb25fbmFtZRKYAQoHugEECAEYAQoEOgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFggECEAEKBYoCAhgBCgRiAhABCgnCAQYIBBATIAEKCcIBBggEEBMgAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAMQEyABCgQ6AhABCgnCAQYIAxATIAEKBDoCEAEKCcIBBggDEBMgAQoEOgIQAQoEYgIQARgCOl8KMU9SQUNMRV9CQVNJQ19EQlRBQkxFX1NDSEVNQV9HQU1NQV9FVV9XRVNUXzFfSU5URUcKKk9SQUNMRV9CQVNJQ19EQlRBQkxFX0dBTU1BX0VVX1dFU1RfMV9JTlRFRxoIEgYKAhIAIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggIIgASC2VtcGxveWVlX2lkEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSBnNhbGFyeTILEEoqB2lzdGhtdXM="),

                // SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
                Arguments.of("Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRrsAxLpAwrZAzrWAwoFEgMKARYSwAMSvQMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaJhokCAEaBAoCEAEiDBoKEggKBBICCAMiACIMGgoKCGIGRU1QMDAxGgoSCAoEEgIIAyIAEgtFTVBMT1lFRV9JRDILEEoqB2lzdGhtdXM="),

                // SELECT employee_id FROM basic_write_nonexist WHERE hash1 > 1000
                Arguments.of("Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAEQARoKZ3Q6YW55X2FueRrnAxLkAwrUAzrRAwoFEgMKARYSuwMSuAMKAgoAEo4DCosDCgIKABLkAgoEZGF0ZQoLZmxvYXRfdmFsdWUKBXByaWNlCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKYAQoFggECEAEKBFoCEAEKBFoCEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYIBAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOh4KBnB1YmxpYwoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaIRofCAEaBAoCEAEiDBoKEggKBBICCA0iACIHGgUKAzjoBxoKEggKBBICCAMiABILRU1QTE9ZRUVfSUQyCxBKKgdpc3RobXVz"),
                // SELECT * FROM "warehouse"."call_center" ORDER BY "cc_rec_start_date" LIMIT 100
                Arguments.of("GucFEuQFCuEFGt4FCgIKABLTBSrQBQoCCgASuQUKtgUKAgoAEpUFChFjY19jYWxsX2NlbnRlcl9zawoRY2NfY2FsbF9jZW50ZXJfaWQKEWNjX3JlY19zdGFydF9kYXRlCg9jY19yZWNfZW5kX2RhdGUKEWNjX2Nsb3NlZF9kYXRlX3NrCg9jY19vcGVuX2RhdGVfc2sKB2NjX25hbWUKCGNjX2NsYXNzCgxjY19lbXBsb3llZXMKCGNjX3NxX2Z0CghjY19ob3VycwoKY2NfbWFuYWdlcgoJY2NfbWt0X2lkCgxjY19ta3RfY2xhc3MKC2NjX21rdF9kZXNjChFjY19tYXJrZXRfbWFuYWdlcgoLY2NfZGl2aXNpb24KEGNjX2RpdmlzaW9uX25hbWUKCmNjX2NvbXBhbnkKD2NjX2NvbXBhbnlfbmFtZQoQY2Nfc3RyZWV0X251bWJlcgoOY2Nfc3RyZWV0X25hbWUKDmNjX3N0cmVldF90eXBlCg9jY19zdWl0ZV9udW1iZXIKB2NjX2NpdHkKCWNjX2NvdW50eQoIY2Nfc3RhdGUKBmNjX3ppcAoKY2NfY291bnRyeQoNY2NfZ210X29mZnNldAoRY2NfdGF4X3BlcmNlbnRhZ2UKB3BhcnRfaWQSzgEKBCoCEAEKBGICEAEKBYIBAhABCgWCAQIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEKgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoJwgEGCAIQBSABCgnCAQYIAhAFIAEKBGICEAEYAjoYCgl3YXJlaG91c2UKC2NhbGxfY2VudGVyGg4KChIICgQSAggCIgAQAhgAIGQ=")
                );
    }
}
