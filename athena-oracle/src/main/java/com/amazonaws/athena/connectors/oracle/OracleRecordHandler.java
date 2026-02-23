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
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Map;

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
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, null, new DatabaseConnectionInfo(ORACLE_DRIVER_CLASS, ORACLE_DEFAULT_PORT)), configOptions);
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

    @Override
    public CredentialsProvider createCredentialsProvider(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        return new OracleCredentialsProvider(
                getSecret(secretName, requestOverrideConfiguration),
                getDatabaseConnectionConfig().getJdbcConnectionString());
    }

    private String resolveColumnName(String fieldName, ResultSet resultSet)
    {
        try {
            // Try with Calcite-style alias first (columnName + "0")
            resultSet.findColumn(fieldName + "0");
            return fieldName + "0";
        }
        catch (Exception e) {
            // Try original name
            try {
                resultSet.findColumn(fieldName);
                return fieldName;
            }
            catch (Exception e2) {
                // Column doesn't exist in ResultSet
                return null;
            }
        }
    }

    private Extractor createNullExtractor(Types.MinorType fieldType)
    {
        switch (fieldType) {
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) -> dst.isSet = 0;
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) -> dst.isSet = 0;
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) -> dst.isSet = 0;
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) -> dst.isSet = 0;
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) -> dst.isSet = 0;
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) -> dst.isSet = 0;
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) -> dst.isSet = 0;
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) -> dst.isSet = 0;
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) -> dst.isSet = 0;
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) -> dst.isSet = 0;
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) -> dst.isSet = 0;
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) -> dst.isSet = 0;
            default:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) -> dst.isSet = 0;
        }
    }

    @Override
    protected Extractor makeExtractor(Field field, ResultSet resultSet, Map<String, String> partitionValues)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        final String originalFieldName = field.getName();
        final String fieldName = resolveColumnName(originalFieldName, resultSet);

        // If column doesn't exist in ResultSet, return a null extractor
        if (fieldName == null) {
            LOGGER.debug("Column '{}' not found in ResultSet, creating null extractor", originalFieldName);
            return createNullExtractor(fieldType);
        }

        if (partitionValues.containsKey(originalFieldName)) {
            return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = partitionValues.get(originalFieldName);
            };
        }
        LOGGER.info("Extractor: attempting to read column '{}' (resolved from '{}')", fieldName, originalFieldName);

        switch (fieldType) {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    boolean value = resultSet.getBoolean(fieldName);
                    dst.value = value ? 1 : 0;
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    dst.value = resultSet.getByte(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    dst.value = resultSet.getShort(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) ->
                {
                    dst.value = resultSet.getInt(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    dst.value = resultSet.getLong(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    dst.value = resultSet.getFloat(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    try {
                        dst.value = resultSet.getDouble(fieldName);
                    }
                    catch (java.sql.SQLException ex) {
                        // We need to use Double.parseDouble()
                        // replaceAll() use to strip commas "$25,000.00"
                        dst.value = Double.parseDouble(resultSet.getString(fieldName).replaceAll(",", "").replaceAll("\\$", ""));
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    dst.value = resultSet.getBigDecimal(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    //Issue fix for getting different date (offset by 1) for any dates prior to 1/1/1970.
                    if (resultSet.getDate(fieldName) != null) {
                        dst.value = (int) LocalDate.parse(resultSet.getDate(fieldName).toString()).toEpochDay();
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    if (resultSet.getTimestamp(fieldName) != null) {
                        dst.value = resultSet.getTimestamp(fieldName).getTime();
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
                {
                    if (null != resultSet.getString(fieldName)) {
                        dst.value = resultSet.getString(fieldName);
                    }
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    dst.value = resultSet.getBytes(fieldName);
                    dst.isSet = resultSet.wasNull() ? 0 : 1;
                };
            default:
                throw new AthenaConnectorException("Unhandled type " + fieldType,
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
        }
    }
}
