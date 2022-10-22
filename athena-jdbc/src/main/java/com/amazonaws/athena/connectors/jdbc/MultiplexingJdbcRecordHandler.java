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
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandlerFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Record handler multiplexer that supports multiple engines e.g. MySQL, PostGreSql and Redshift in same Lambda.
 *
 * Uses catalog name and associations to database types to route operations.
 */
public class MultiplexingJdbcRecordHandler
        extends JdbcRecordHandler
{
    private static final int MAX_CATALOGS_TO_MULTIPLEX = 100;
    private final Map<String, JdbcRecordHandler> recordHandlerMap;

    public MultiplexingJdbcRecordHandler(JdbcRecordHandlerFactory jdbcRecordHandlerFactory)
    {
        super(jdbcRecordHandlerFactory.getEngine());
        this.recordHandlerMap = Validate.notEmpty(JDBCUtil.createJdbcRecordHandlerMap(System.getenv(), jdbcRecordHandlerFactory), "Could not find any delegatee.");
    }

    @VisibleForTesting
    protected MultiplexingJdbcRecordHandler(final AmazonS3 amazonS3, final AWSSecretsManager secretsManager, final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory,
            final DatabaseConnectionConfig databaseConnectionConfig, final Map<String, JdbcRecordHandler> recordHandlerMap)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory);
        this.recordHandlerMap = Validate.notEmpty(recordHandlerMap, "recordHandlerMap must not be empty");

        if (this.recordHandlerMap.size() > MAX_CATALOGS_TO_MULTIPLEX) {
            throw new RuntimeException("Max 100 catalogs supported in multiplexer.");
        }
    }

    private void validateMultiplexer(final String catalogName)
    {
        if (this.recordHandlerMap.get(catalogName) == null) {
            throw new RuntimeException(String.format(MultiplexingJdbcMetadataHandler.CATALOG_NOT_REGISTERED_ERROR_TEMPLATE, catalogName));
        }
    }

    @Override
    public void readWithConstraint(
            final BlockSpiller blockSpiller,
            final ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        validateMultiplexer(readRecordsRequest.getCatalogName());
        this.recordHandlerMap.get(readRecordsRequest.getCatalogName()).readWithConstraint(blockSpiller, readRecordsRequest, queryStatusChecker);
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException
    {
        return this.recordHandlerMap.get(catalogName).buildSplitSql(jdbcConnection, catalogName, tableName, schema, constraints, split);
    }
}
