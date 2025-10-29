/*-
 * #%L
 * athena-redshift
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
package com.amazonaws.athena.connectors.redshift;

import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

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

public class RedshiftMuxRecordHandlerTest {

    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_ENGINE = "redshift";
    private static final String TEST_CONNECTION_STRING = "redshift://jdbc:redshift://hostname/${testSecret}";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "tableName";
    private static final String UNSUPPORTED_CATALOG = "unsupportedCatalog";
    private static final String TEST_DB_CONNECTION_STRING = "jdbc:redshift://hostname/testdb";
    private static final String EXPECTED_ENGINE = RedshiftConstants.REDSHIFT_NAME;

    private Map<String, JdbcRecordHandler> recordHandlerMap;
    private RedshiftRecordHandler redshiftRecordHandler;
    private JdbcRecordHandler jdbcRecordHandler;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;
    private QueryStatusChecker queryStatusChecker;
    private JdbcConnectionFactory jdbcConnectionFactory;

    private RedshiftMuxRecordHandlerFactory factory;
    private Map<String, String> configOptions;
    private DatabaseConnectionConfig mockConfig;

    @Before
    public void setUp() {

        this.redshiftRecordHandler = Mockito.mock(RedshiftRecordHandler.class);
        this.recordHandlerMap = Collections.singletonMap(TEST_ENGINE, this.redshiftRecordHandler);
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE,
                TEST_CONNECTION_STRING, TEST_SECRET);
        this.jdbcRecordHandler = new RedshiftMuxRecordHandler(this.amazonS3, this.secretsManager, this.athena, this.jdbcConnectionFactory, databaseConnectionConfig, this.recordHandlerMap, com.google.common.collect.ImmutableMap.of());
        this.factory = new RedshiftMuxRecordHandlerFactory();
        this.configOptions = new HashMap<>();
        this.mockConfig = new DatabaseConnectionConfig(
                TEST_CATALOG, RedshiftConstants.REDSHIFT_NAME, TEST_DB_CONNECTION_STRING
        );
    }

    @Test
    public void readWithConstraint()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
        Mockito.verify(this.redshiftRecordHandler, Mockito.times(1)).readWithConstraint(Mockito.eq(blockSpiller), Mockito.eq(readRecordsRequest), Mockito.eq(queryStatusChecker));
    }

    @Test(expected = RuntimeException.class)
    public void readWithConstraintWithUnsupportedCatalog()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn(UNSUPPORTED_CATALOG);
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn(TEST_ENGINE);
        Connection jdbcConnection = Mockito.mock(Connection.class);
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = Mockito.mock(Schema.class);
        Constraints constraints = Mockito.mock(Constraints.class);
        Split split = Mockito.mock(Split.class);
        this.jdbcRecordHandler.buildSplitSql(jdbcConnection, TEST_ENGINE, tableName, schema, constraints, split);
        Mockito.verify(this.redshiftRecordHandler, Mockito.times(1)).buildSplitSql(Mockito.eq(jdbcConnection), Mockito.eq(TEST_ENGINE), Mockito.eq(tableName), Mockito.eq(schema), Mockito.eq(constraints), Mockito.eq(split));
    }

    @Test
    public void testGetEngine() {
        assertEquals("Engine name should match RedshiftConstants.REDSHIFT_NAME",
                EXPECTED_ENGINE, this.factory.getEngine());
    }

    @Test
    public void testCreateJdbcRecordHandler() {
        JdbcRecordHandler recordHandler = this.factory.createJdbcRecordHandler(this.mockConfig, this.configOptions);

        assertThat(recordHandler)
                .as("Record handler should be an instance of RedshiftRecordHandler")
                .isInstanceOf(RedshiftRecordHandler.class);
    }
}
