/*-
 * #%L
 * athena-clickhouse
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
package com.amazonaws.athena.connectors.clickhouse;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;

@RunWith(MockitoJUnitRunner.class)
public class ClickHouseRecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_CONNECTION_STRING = "clickhouse://jdbc:clickhouse://localhost/user=A&password=B";
    private static final String DATABASE_CONNECTION_FAILED_MESSAGE = "Database connection failed";
    public static final String RESULT_SHOULD_NOT_BE_NULL = "Result should not be null";
    public static final String RESULT_SHOULD_MATCH_MOCKED_PREPARED_STATEMENT = "Result should match mocked PreparedStatement";

    @Mock
    private S3Client mockS3Client;
    @Mock
    private SecretsManagerClient mockSecretsManager;
    @Mock
    private AthenaClient mockAthenaClient;
    @Mock
    private JdbcConnectionFactory mockJdbcConnectionFactory;
    @Mock
    private JdbcSplitQueryBuilder mockJdbcSplitQueryBuilder;
    @Mock
    private Connection mockConnection;
    @Mock
    private PreparedStatement mockPreparedStatement;
    @Mock
    private Constraints mockConstraints;
    @Mock
    private Schema mockSchema;
    @Mock
    private Split mockSplit;
    private ClickHouseRecordHandler handler;
    private TableName tableName;

    @Before
    public void setUp()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                TEST_CATALOG,
                ClickHouseConstants.NAME,
                TEST_CONNECTION_STRING
        );
        Map<String, String> configOptions = new HashMap<>();
        this.handler = new ClickHouseRecordHandler(
                databaseConnectionConfig,
                mockS3Client,
                mockSecretsManager,
                mockAthenaClient,
                mockJdbcConnectionFactory,
                mockJdbcSplitQueryBuilder,
                configOptions
        );
        this.tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
    }

    @Test
    public void testBuildSplitSqlWithQueryPassThroughEnabled()
            throws SQLException
    {
        Mockito.when(mockConstraints.isQueryPassThrough()).thenReturn(true);

        // Create a spy to mock the buildQueryPassthroughSql method
        ClickHouseRecordHandler spyHandler = Mockito.spy(handler);
        Mockito.doReturn(mockPreparedStatement).when(spyHandler).buildQueryPassthroughSql(
                any(Connection.class), any(Constraints.class));

        PreparedStatement result = spyHandler.buildSplitSql(
                mockConnection,
                TEST_CATALOG,
                tableName,
                mockSchema,
                mockConstraints,
                mockSplit
        );

        // Verify
        Assert.assertNotNull(RESULT_SHOULD_NOT_BE_NULL, result);
        Assert.assertEquals(RESULT_SHOULD_MATCH_MOCKED_PREPARED_STATEMENT, mockPreparedStatement, result);

        // Verify that setFetchSize was called with ClickHouseConstants.FETCH_SIZE
        Mockito.verify(mockPreparedStatement, Mockito.times(1)).setFetchSize(ClickHouseConstants.FETCH_SIZE);

        // Verify that buildQueryPassthroughSql was called
        Mockito.verify(spyHandler, Mockito.times(1)).buildQueryPassthroughSql(
                eq(mockConnection), eq(mockConstraints));

        // Verify that jdbcSplitQueryBuilder.buildSql was NOT called (QPT path taken)
        Mockito.verify(mockJdbcSplitQueryBuilder, Mockito.never()).buildSql(
                any(Connection.class),
                isNull(),
                anyString(),
                anyString(),
                any(Schema.class),
                any(Constraints.class),
                any(Split.class)
        );
    }

    @Test
    public void testBuildSplitSqlWithQueryPassThroughDisabled()
            throws SQLException
    {
        Mockito.when(mockConstraints.isQueryPassThrough()).thenReturn(false);
        Mockito.when(mockJdbcSplitQueryBuilder.buildSql(
                any(Connection.class),
                isNull(),
                eq(TEST_SCHEMA),
                eq(TEST_TABLE),
                any(Schema.class),
                any(Constraints.class),
                any(Split.class)
        )).thenReturn(mockPreparedStatement);

        // Execute
        PreparedStatement result = handler.buildSplitSql(
                mockConnection,
                TEST_CATALOG,
                tableName,
                mockSchema,
                mockConstraints,
                mockSplit
        );

        // Verify
        Assert.assertNotNull(RESULT_SHOULD_NOT_BE_NULL, result);
        Assert.assertEquals(RESULT_SHOULD_MATCH_MOCKED_PREPARED_STATEMENT, mockPreparedStatement, result);

        // Verify that setFetchSize was called with ClickHouseConstants.FETCH_SIZE
        Mockito.verify(mockPreparedStatement, Mockito.times(1)).setFetchSize(ClickHouseConstants.FETCH_SIZE);

        // Verify that jdbcSplitQueryBuilder.buildSql was called with correct parameters
        Mockito.verify(mockJdbcSplitQueryBuilder, Mockito.times(1)).buildSql(
                eq(mockConnection),
                isNull(),
                eq(TEST_SCHEMA),
                eq(TEST_TABLE),
                eq(mockSchema),
                eq(mockConstraints),
                eq(mockSplit)
        );
    }

    @Test(expected = SQLException.class)
    public void testBuildSplitSqlWithSQLExceptionFromJdbcSplitQueryBuilder()
            throws SQLException
    {
        Mockito.when(mockConstraints.isQueryPassThrough()).thenReturn(false);
        Mockito.when(mockJdbcSplitQueryBuilder.buildSql(
                any(Connection.class),
                isNull(),
                eq(TEST_SCHEMA),
                eq(TEST_TABLE),
                any(Schema.class),
                any(Constraints.class),
                any(Split.class)
        )).thenThrow(new SQLException(DATABASE_CONNECTION_FAILED_MESSAGE));

        // Execute and verify exception
        handler.buildSplitSql(
                mockConnection,
                TEST_CATALOG,
                tableName,
                mockSchema,
                mockConstraints,
                mockSplit
        );
    }
}
