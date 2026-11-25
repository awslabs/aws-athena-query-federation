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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import oracle.jdbc.OracleTypes;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_NAME;

/**
 * Data handler, user must have necessary permissions to read from necessary tables.
 */
public class OracleRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleRecordHandler.class);
    private static final int FETCH_SIZE = 1000;
    // The driver reports FLOAT(b) and an unconstrained NUMBER with this scale.
    private static final int ORACLE_FLOAT_SCALE = -127;
    private static final String ORACLE_QUOTE_CHARACTER = "\"";

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link OracleMuxCompositeHandler} instead.
     */
    public OracleRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(ORACLE_NAME, configOptions), configOptions);
    }

    public OracleRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, null, new DatabaseConnectionInfo(ORACLE_DRIVER_CLASS, ORACLE_DEFAULT_PORT), configOptions), configOptions);
    }

    public OracleRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                jdbcConnectionFactory, new OracleQueryStringBuilder(ORACLE_QUOTE_CHARACTER, new OracleFederationExpressionParser(ORACLE_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    OracleRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, final S3Client amazonS3, final SecretsManagerClient secretsManager,
                        final AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException
    {
        PreparedStatement preparedStatement;

        if (constraints.isQueryPassThrough()) {
            preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
        }
        else {
            preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
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
     * Maps Oracle columns for the export:
     * <ul>
     *   <li>NUMBER with no precision becomes {@code Decimal(38, default_scale)}; FLOAT(b), which the driver reports
     *       as NUMBER with scale -127, and BINARY_DOUBLE become DOUBLE; BINARY_FLOAT becomes FLOAT;</li>
     *   <li>DATE and TIMESTAMP, which the driver reports as TIMESTAMP, and TIMESTAMP WITH (LOCAL) TIME ZONE become
     *       UTC timestamps, so DATE keeps its time of day;</li>
     *   <li>the rest use {@link JdbcExportTypes#defaultArrowType}, so INTERVAL, ROWID and the other types the spill
     *       cannot write become VARCHAR, CLOB text and BLOB binary.</li>
     * </ul>
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
        int jdbcType = metadata.getColumnType(column);
        switch (jdbcType) {
            case OracleTypes.BINARY_FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case OracleTypes.BINARY_DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return JdbcExportTypes.TIMESTAMP_UTC;
            case Types.NUMERIC:
                if (metadata.getScale(column) == ORACLE_FLOAT_SCALE && metadata.getPrecision(column) > 0) {
                    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                }
                break;
            default:
                break;
        }
        return JdbcExportTypes.defaultArrowType(metadata, column, configOptions);
    }

    @Override
    protected Object readExportValue(ResultSet resultSet, int column, Field field) throws SQLException
    {
        if (resultSet.getMetaData().getColumnType(column) == OracleTypes.TIMESTAMPTZ) {
            return JdbcExportTypes.readOffsetTimestamp(resultSet, column);
        }
        return JdbcExportTypes.readValue(resultSet, column, field.getType());
    }

    @Override
    public CredentialsProvider createCredentialsProvider(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        return new OracleCredentialsProvider(
                getSecret(secretName, requestOverrideConfiguration),
                getDatabaseConnectionConfig().getJdbcConnectionString());
    }
}
