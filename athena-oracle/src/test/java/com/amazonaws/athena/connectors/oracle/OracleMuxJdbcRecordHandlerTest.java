/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.oracle.OracleMuxRecordHandler;
import com.amazonaws.athena.connectors.oracle.OracleRecordHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

public class OracleMuxJdbcRecordHandlerTest
{
    private Map<String, JdbcRecordHandler> recordHandlerMap;
    private OracleRecordHandler oracleRecordHandler;
    private JdbcRecordHandler jdbcRecordHandler;
    private AmazonS3 amazonS3;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private QueryStatusChecker queryStatusChecker;
    private JdbcConnectionFactory jdbcConnectionFactory;

    @Before
    public void setup()
    {
        this.oracleRecordHandler = Mockito.mock(OracleRecordHandler.class);
        this.recordHandlerMap = Collections.singletonMap("oracle", this.oracleRecordHandler);
        this.amazonS3 = Mockito.mock(AmazonS3.class);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.queryStatusChecker = Mockito.mock(QueryStatusChecker.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", "oracle",
                "oracle://jdbc:oracle:thin:${testSecret}@//127.0.0.1:1521/orcl", "testSecret");
        this.jdbcRecordHandler = new OracleMuxRecordHandler(this.amazonS3, this.secretsManager, this.athena, this.jdbcConnectionFactory, databaseConnectionConfig, this.recordHandlerMap);
    }

    @Test
    public void readWithConstraint()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn("oracle");
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
        Mockito.verify(this.oracleRecordHandler, Mockito.times(1)).readWithConstraint(Mockito.eq(blockSpiller), Mockito.eq(readRecordsRequest), Mockito.eq(queryStatusChecker));
    }

    @Test(expected = RuntimeException.class)
    public void readWithConstraintWithUnsupportedCatalog()
            throws Exception
    {
        BlockSpiller blockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn("unsupportedCatalog");
        this.jdbcRecordHandler.readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        ReadRecordsRequest readRecordsRequest = Mockito.mock(ReadRecordsRequest.class);
        Mockito.when(readRecordsRequest.getCatalogName()).thenReturn("oracle");
        Connection jdbcConnection = Mockito.mock(Connection.class);
        TableName tableName = new TableName("testSchema", "tableName");
        Schema schema = Mockito.mock(Schema.class);
        Constraints constraints = Mockito.mock(Constraints.class);
        Split split = Mockito.mock(Split.class);
        this.jdbcRecordHandler.buildSplitSql(jdbcConnection, "oracle", tableName, schema, constraints, split);
        Mockito.verify(this.oracleRecordHandler, Mockito.times(1)).buildSplitSql(Mockito.eq(jdbcConnection), Mockito.eq("oracle"), Mockito.eq(tableName), Mockito.eq(schema), Mockito.eq(constraints), Mockito.eq(split));
    }
}
