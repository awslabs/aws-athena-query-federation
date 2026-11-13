/*-
 * #%L
 * athena-clickhouse
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
package com.amazonaws.athena.connectors.clickhouse;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

public class ClickHouseMuxJdbcRecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String CLICKHOUSE_CATALOG = "clickhouse";
    private static final String UNSUPPORTED_CATALOG = "unsupportedCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "tableName";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_CONNECTION_STRING = "clickhouse://jdbc:clickhouse://hostname/${testSecret}";
    private static final String DATABASE_CONNECTION_FAILED_MESSAGE = "Database connection failed";
    private static final String EXPECTED_SQL_EXCEPTION_MESSAGE = "Expected SQLException to be thrown";
    private Map<String, JdbcRecordHandler> recordHandlerMap;
    private ClickHouseRecordHandler recordHandler;
    private JdbcRecordHandler jdbcRecordHandler;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private QueryStatusChecker queryStatusChecker;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Constraints constraints;
    private Schema schema;
    private Split split;
    private Connection jdbcConnection;
    private TableName tableName;
    private ReadRecordsRequest readRecordsRequest;


    @Before
    public void setup()
    {
        this.recordHandler = Mockito.mock(ClickHouseRecordHandler.class);
        this.recordHandlerMap = Collections.singletonMap(ClickHouseConstants.NAME, this.recordHandler);
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        this.constraints = Mockito.mock(Constraints.class);
        this.schema = Mockito.mock(Schema.class);
        this.split = Mockito.mock(Split.class);
        this.jdbcConnection = Mockito.mock(Connection.class);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, ClickHouseConstants.NAME,
                TEST_CONNECTION_STRING, TEST_SECRET);
        this.jdbcRecordHandler = new ClickHouseMuxRecordHandler(this.amazonS3, this.secretsManager, this.athena, this.jdbcConnectionFactory, databaseConnectionConfig, this.recordHandlerMap, com.google.common.collect.ImmutableMap.of());
        this.tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        this.readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn(CLICKHOUSE_CATALOG);
    }

    @Test
    public void readWithConstraint_withSupportedCatalog_invokesDelegateReadWithConstraint()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
        Mockito.verify(this.recordHandler, Mockito.times(1)).readWithConstraint(Mockito.eq(blockSpiller), Mockito.eq(readRecordsRequest), Mockito.eq(queryStatusChecker));
    }

    @Test(expected = RuntimeException.class)
    public void readWithConstraint_withUnsupportedCatalog_throwsRuntimeException()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn(UNSUPPORTED_CATALOG);
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
    }

    @Test
    public void buildSplitSql_withSupportedCatalog_invokesDelegateBuildSplitSql()
            throws SQLException
    {
        this.jdbcRecordHandler.buildSplitSql(jdbcConnection, CLICKHOUSE_CATALOG, tableName, schema, constraints, split);
        Mockito.verify(this.recordHandler, Mockito.times(1)).buildSplitSql(Mockito.eq(jdbcConnection), Mockito.eq(CLICKHOUSE_CATALOG), Mockito.eq(tableName), Mockito.eq(schema), Mockito.eq(constraints), Mockito.eq(split));
    }

    @Test
    public void buildSplitSql_withQueryPassthrough_invokesDelegateBuildSplitSql()
            throws SQLException
    {
        mockAndAssertBuildSplitSql(true);
    }

    @Test
    public void buildSplitSql_withNormalQuery_invokesDelegateBuildSplitSql()
            throws SQLException
    {
        mockAndAssertBuildSplitSql(false);
    }

    @Test
    public void buildSplitSql_whenRecordHandlerThrowsSQLException_propagatesSQLException()
            throws SQLException
    {
        // Mock SQLException from the record handler
        Mockito.when(this.recordHandler.buildSplitSql(Mockito.eq(jdbcConnection), Mockito.eq(CLICKHOUSE_CATALOG), Mockito.eq(tableName), Mockito.eq(schema), Mockito.eq(constraints), Mockito.eq(split)))
                .thenThrow(new SQLException(DATABASE_CONNECTION_FAILED_MESSAGE));

        try {
            this.jdbcRecordHandler.buildSplitSql(jdbcConnection, CLICKHOUSE_CATALOG, tableName, schema, constraints, split);
            Assert.fail(EXPECTED_SQL_EXCEPTION_MESSAGE);
        } catch (SQLException e) {
            Assert.assertTrue("Expected SQLException", e instanceof SQLException);
            Assert.assertEquals("Exception message should match", DATABASE_CONNECTION_FAILED_MESSAGE, e.getMessage());
        }
        assertRecordHandlerCalled(this.recordHandler);
    }

    private void mockAndAssertBuildSplitSql(boolean isQueryPassThrough) throws SQLException
    {
        Mockito.when(constraints.isQueryPassThrough()).thenReturn(isQueryPassThrough);
        PreparedStatement mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.recordHandler.buildSplitSql(Mockito.eq(jdbcConnection), Mockito.eq(CLICKHOUSE_CATALOG), Mockito.eq(tableName), Mockito.eq(schema), Mockito.eq(constraints), Mockito.eq(split)))
                .thenReturn(mockPreparedStatement);

        PreparedStatement result = this.jdbcRecordHandler.buildSplitSql(jdbcConnection, CLICKHOUSE_CATALOG, tableName, schema, constraints, split);

        Assert.assertEquals("PreparedStatement should match", mockPreparedStatement, result);
        assertRecordHandlerCalled(this.recordHandler);
    }

    private void assertRecordHandlerCalled(ClickHouseRecordHandler recordHandler) {
        try {
            Mockito.verify(recordHandler, Mockito.times(1)).buildSplitSql(
                    Mockito.any(Connection.class), Mockito.eq(CLICKHOUSE_CATALOG),
                    Mockito.any(TableName.class), Mockito.any(Schema.class),
                    Mockito.any(Constraints.class), Mockito.any(Split.class));
        } catch (SQLException e) {
            // This should not happen in test verification
            Assert.fail("Unexpected SQLException during verification: " + e.getMessage());
        }
    }
}
