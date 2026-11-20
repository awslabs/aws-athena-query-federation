/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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

package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.CredentialsProviderFactory;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcExportTypes;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;

public class SqlServerRecordHandler extends JdbcRecordHandler
{
    private static final int FETCH_SIZE = 1000;
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    public SqlServerRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SqlServerConstants.NAME, configOptions), configOptions);
    }

    public SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, SqlServerMetadataHandler.JDBC_PROPERTIES,
                new DatabaseConnectionInfo(SqlServerConstants.DRIVER_CLASS, SqlServerConstants.DEFAULT_PORT), configOptions), configOptions);
    }

    public SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(),
                AthenaClient.create(), jdbcConnectionFactory, new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    SqlServerRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, final S3Client amazonS3, final SecretsManagerClient secretsManager,
                           final AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints,
                                           Split split) throws SQLException
    {
        PreparedStatement preparedStatement;

        if (constraints.isQueryPassThrough()) {
            preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
        }
        else {
            preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(),
                    schema, constraints, split);
        }
        // Disable fetching all rows.
        preparedStatement.setFetchSize(FETCH_SIZE);
        return preparedStatement;
    }
    
    /**
     * Fetches the export result in batches instead of reading all of it into memory.
     */
    @Override
    protected PreparedStatement prepareExportStatement(Connection connection, String query) throws SQLException
    {
        PreparedStatement preparedStatement = super.prepareExportStatement(connection, query);
        preparedStatement.setFetchSize(FETCH_SIZE);
        return preparedStatement;
    }

    /**
     * Maps SQL Server columns for the export. TINYINT is unsigned (0 to 255), so it widens to SMALLINT, as on the
     * normal read path. DATETIMEOFFSET becomes a UTC timestamp that keeps the instant. The rest use
     * {@link JdbcExportTypes#defaultArrowType}, so TIME, SQL_VARIANT and the other types the spill cannot write
     * become VARCHAR.
     */
    @Override
    protected Schema deriveExportSchema(ResultSet resultSet) throws SQLException
    {
        ResultSetMetaData metadata = resultSet.getMetaData();
        List<Field> fields = new ArrayList<>();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            fields.add(Field.nullable(metadata.getColumnLabel(column), exportArrowType(metadata, column)));
        }
        return new Schema(fields);
    }

    private ArrowType exportArrowType(ResultSetMetaData metadata, int column) throws SQLException
    {
        switch (metadata.getColumnType(column)) {
            case Types.TINYINT:
                return new ArrowType.Int(16, true);
            case microsoft.sql.Types.DATETIMEOFFSET:
                return JdbcExportTypes.TIMESTAMP_UTC;
            default:
                return JdbcExportTypes.defaultArrowType(metadata, column, configOptions);
        }
    }

    @Override
    protected Object readExportValue(ResultSet resultSet, int column, Field field) throws SQLException
    {
        if (resultSet.getMetaData().getColumnType(column) == microsoft.sql.Types.DATETIMEOFFSET) {
            return JdbcExportTypes.readOffsetTimestamp(resultSet, column);
        }
        return JdbcExportTypes.readValue(resultSet, column, field.getType());
    }

    @Override
    public CredentialsProvider createCredentialsProvider(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        return CredentialsProviderFactory.createCredentialProvider(
                getDatabaseConnectionConfig().getSecret(),
                getCachableSecretsManager(),
                new SqlServerOAuthCredentialsProvider(),
                requestOverrideConfiguration
        );
    }
}
