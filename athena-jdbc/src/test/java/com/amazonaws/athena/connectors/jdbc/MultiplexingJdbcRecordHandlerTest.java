/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class MultiplexingJdbcRecordHandlerTest
{
    private Map<String, JdbcRecordHandler> recordHandlerMap;
    private JdbcRecordHandler fakeJdbcRecordHandler;
    private JdbcRecordHandler jdbcRecordHandler;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private QueryStatusChecker queryStatusChecker;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private DatabaseConnectionConfig databaseConnectionConfig;

    @Before
    public void setup()
    {
        this.fakeJdbcRecordHandler = Mockito.mock(JdbcRecordHandler.class);
        this.recordHandlerMap = Collections.singletonMap("fakedatabase", this.fakeJdbcRecordHandler);
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "fakedatabase",
                "fakedatabase://jdbc:fakedatabase://hostname/${testSecret}", "testSecret");
        this.jdbcRecordHandler = new MultiplexingJdbcRecordHandler(this.amazonS3, this.secretsManager, this.athena, this.jdbcConnectionFactory, databaseConnectionConfig, this.recordHandlerMap, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void readWithConstraint()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        when(readRecordsRequest.getCatalogName()).thenReturn("fakedatabase");
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
        Mockito.verify(this.fakeJdbcRecordHandler, Mockito.times(1)).readWithConstraint(Mockito.eq(blockSpiller), Mockito.eq(readRecordsRequest), Mockito.eq(queryStatusChecker));
    }

    @Test(expected = RuntimeException.class)
    public void readWithConstraintWithUnsupportedCatalog()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        when(readRecordsRequest.getCatalogName()).thenReturn("unsupportedCatalog");
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        when(readRecordsRequest.getCatalogName()).thenReturn("fakedatabase");
        Connection jdbcConnection = Mockito.mock(Connection.class);
        TableName tableName = new TableName("testSchema", "tableName");
        Schema schema = Mockito.mock(Schema.class);
        Constraints constraints = Mockito.mock(Constraints.class);
        Split split = Mockito.mock(Split.class);
        this.jdbcRecordHandler.buildSplitSql(jdbcConnection, "fakedatabase", tableName, schema, constraints, split);
        Mockito.verify(this.fakeJdbcRecordHandler, Mockito.times(1)).buildSplitSql(Mockito.eq(jdbcConnection), Mockito.eq("fakedatabase"), Mockito.eq(tableName), Mockito.eq(schema), Mockito.eq(constraints), Mockito.eq(split));
    }

    @Test
    public void testConstructor_withTooManyHandlers_shouldThrowException() {
        recordHandlerMap = new HashMap<>();
        for (int i = 0; i < 101; i++) {
            recordHandlerMap.put("catalog" + i, fakeJdbcRecordHandler);
        }

        AthenaConnectorException exception = assertThrows(AthenaConnectorException.class, () ->
                new MultiplexingJdbcRecordHandler(
                        amazonS3,
                        secretsManager,
                        athena,
                        jdbcConnectionFactory,
                        databaseConnectionConfig,
                        recordHandlerMap,
                        com.google.common.collect.ImmutableMap.of()
                )
        );
        assertTrue(exception.getMessage().contains("Max 100 catalogs supported in multiplexer"));
    }

    @Test
    public void testConstructor_withEmptyHandlerMap_shouldThrowException() {
        recordHandlerMap = new HashMap<>();
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                new MultiplexingJdbcRecordHandler(
                        amazonS3,
                        secretsManager,
                        athena,
                        jdbcConnectionFactory,
                        databaseConnectionConfig,
                        recordHandlerMap,
                        com.google.common.collect.ImmutableMap.of()
                )
        );
        assertTrue(exception.getMessage().contains("recordHandlerMap must not be empty"));
    }

    @Test
    public void testConstructor_withNullHandlerMap_shouldThrowException() {
        Exception exception = assertThrows(NullPointerException.class, () ->
                new MultiplexingJdbcRecordHandler(
                        amazonS3,
                        secretsManager,
                        athena,
                        jdbcConnectionFactory,
                        databaseConnectionConfig,
                        null,
                        com.google.common.collect.ImmutableMap.of()
                )
        );
        assertTrue(exception.getMessage().contains("recordHandlerMap must not be empty"));
    }
}
