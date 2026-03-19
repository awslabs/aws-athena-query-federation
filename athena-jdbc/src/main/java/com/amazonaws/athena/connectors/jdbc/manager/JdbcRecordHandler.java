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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
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
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Abstracts JDBC record handler and provides common reusable split records handling.
 */
public abstract class JdbcRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordHandler.class);
    private final JdbcConnectionFactory jdbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private static final String CLICKHOUSE_DB = "clickhouse";

    protected final JdbcQueryPassthrough queryPassthrough = new JdbcQueryPassthrough();

    /**
     * Used only by Multiplexing handler. All invocations will be delegated to respective database handler.
     */
    protected JdbcRecordHandler(String sourceType, java.util.Map<String, String> configOptions)
    {
        super(sourceType, configOptions);
        this.jdbcConnectionFactory = null;
        this.databaseConnectionConfig = null;
    }

    protected JdbcRecordHandler(
        S3Client amazonS3,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        DatabaseConnectionConfig databaseConnectionConfig,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig.getEngine(), configOptions);
        this.jdbcConnectionFactory = Validate.notNull(jdbcConnectionFactory, "jdbcConnectionFactory must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
    }

    protected JdbcConnectionFactory getJdbcConnectionFactory()
    {
        return jdbcConnectionFactory;
    }

    protected DatabaseConnectionConfig getDatabaseConnectionConfig()
    {
        return databaseConnectionConfig;
    }

    protected CredentialsProvider getCredentialProvider()
    {
        return getCredentialProvider(null);
    }

    @Override
    public String getDatabaseConnectionSecret()
    {
        DatabaseConnectionConfig databaseConnectionConfig = getDatabaseConnectionConfig();
        if (Objects.nonNull(databaseConnectionConfig)) {
            return databaseConnectionConfig.getSecret();
        }
        return null;
    }

    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.info("{}: Catalog: {}, table {}, splits {}", readRecordsRequest.getQueryId(), readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                readRecordsRequest.getSplit().getProperties());
        try (Connection connection = this.jdbcConnectionFactory.getConnection(getCredentialProvider(getRequestOverrideConfig(readRecordsRequest)))) {
            String databaseProductName = connection.getMetaData().getDatabaseProductName();

            // clickhouse does not support disabling auto-commit
            if (!CLICKHOUSE_DB.equalsIgnoreCase(databaseProductName)) {
                connection.setAutoCommit(false); // For consistency. This is needed to be false to enable streaming for some database types.
            }

            enableCaseSensitivelyLookUpSession(connection); // For certain connectors, we require to apply session config first to enable case

            try (PreparedStatement preparedStatement = buildSplitSql(connection, readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSchema(), readRecordsRequest.getConstraints(), readRecordsRequest.getSplit());
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();
                Map<String, String> colNameRemapping = getColumnNameRemapping(readRecordsRequest);

                GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints());
                for (Field next : readRecordsRequest.getSchema().getFields()) {
                    if (next.getType() instanceof ArrowType.List) {
                        rowWriterBuilder.withFieldWriterFactory(next.getName(), makeFactory(next));
                    }
                    else {
                        rowWriterBuilder.withExtractor(next.getName(), makeExtractor(next, resultSet, partitionValues, colNameRemapping));
                    }
                }

                GeneratedRowWriter rowWriter = rowWriterBuilder.build();
                int rowsReturnedFromDatabase = 0;
                while (resultSet.next()) {
                    if (!queryStatusChecker.isQueryRunning()) {
                        return;
                    }
                    blockSpiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, resultSet) ? 1 : 0);
                    rowsReturnedFromDatabase++;
                }
                LOGGER.info("{} rows returned by database.", rowsReturnedFromDatabase);

                // clickhouse does not support commit/rollback, so skip commit() for clickhouse
                if (!CLICKHOUSE_DB.equalsIgnoreCase(databaseProductName)) {
                    connection.commit();
                }
                disableCaseSensitivelyLookUpSession(connection); // For certain connectors, we require to apply session config first to enable case
            }
        }
    }

    /**
     * Create a field extractor for complex List type.
     * @param field Field's metadata information.
     * @return Extractor for the List type.
     */
    protected FieldWriterFactory makeFactory(Field field)
    {
        return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                (FieldWriter) (Object context, int rowNum) ->
                {
                    Array arrayField = ((ResultSet) context).getArray(field.getName());
                    if (!((ResultSet) context).wasNull()) {
                        List<Object> fieldValue = new ArrayList<>(Arrays.asList((Object[]) arrayField.getArray()));
                        BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                    }
                    return true;
                };
    }

    protected boolean enableCaseSensitivelyLookUpSession(Connection connection)
    {
        return false;
    }

    protected boolean disableCaseSensitivelyLookUpSession(Connection connection)
    {
        return false;
    }

    /**
     * Creates an Extractor for the given field.
     */
    @VisibleForTesting
    protected Extractor makeExtractor(Field field, ResultSet resultSet, Map<String, String> partitionValues)
    {
        return makeExtractor(field, resultSet, partitionValues, Map.of());
    }
    
    private Extractor makeExtractor(Field field, ResultSet resultSet, Map<String, String> partitionValues, Map<String, String> colNameRemapping)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        final String fieldName = colNameRemapping.getOrDefault(field.getName(), field.getName());

        if (partitionValues.containsKey(fieldName)) {
            return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = partitionValues.get(fieldName);
            };
        }

        // Check if column exists in ResultSet - if not, return null extractor
        try {
            resultSet.findColumn(fieldName);
        }
        catch (SQLException e) {
            LOGGER.debug("Column {} not found in ResultSet, returning null extractor", fieldName);
            return makeNullExtractor(fieldType);
        }

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

    /**
     * Builds split SQL string and returns prepared statement.
     *
     * @param jdbcConnection jdbc connection. See {@link Connection}
     * @param catalogName Athena provided catalog name.
     * @param tableName database table name.
     * @param schema table schema.
     * @param constraints constraints to push down to the database.
     * @param split table split.
     * @return prepared statement with sql. See {@link PreparedStatement}
     * @throws SQLException JDBC database exception.
     */
    public abstract PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException;

    public PreparedStatement buildQueryPassthroughSql(Connection jdbcConnection, Constraints constraints) throws SQLException
    {
        PreparedStatement preparedStatement;
        queryPassthrough.verify(constraints.getQueryPassthroughArguments());
        String clientPassQuery = constraints.getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);
        preparedStatement = jdbcConnection.prepareStatement(clientPassQuery);
        return preparedStatement;
    }
    
    protected Map<String, String> getColumnNameRemapping(ReadRecordsRequest request)
    {
        if (request.getConstraints() != null 
                && request.getConstraints().getQueryPlan() != null 
                && request.getConstraints().getQueryPlan().getSubstraitPlan() != null) {
            // Get renamed → original mapping from SubstraitSqlUtils
            Map<String, String> renamedToOriginal = SubstraitSqlUtils.getColumnRemapping(
                    request.getConstraints().getQueryPlan().getSubstraitPlan(), getSqlDialect());
            
            // Invert to original → renamed mapping (filtering out null values for computed expressions)
            Map<String, String> originalToRenamed = new java.util.HashMap<>();
            for (Map.Entry<String, String> entry : renamedToOriginal.entrySet()) {
                if (entry.getValue() != null) {
                    originalToRenamed.put(entry.getValue(), entry.getKey());
                }
            }
            return originalToRenamed;
        }
        return Map.of();
    }
    
    protected SqlDialect getSqlDialect() 
    {
        return AnsiSqlDialect.DEFAULT;
    }
    
    private Extractor makeNullExtractor(Types.MinorType fieldType)
    {
        switch (fieldType) {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) -> dst.isSet = 0;
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) -> dst.isSet = 0;
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) -> dst.isSet = 0;
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) -> dst.isSet = 0;
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) -> dst.isSet = 0;
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) -> dst.isSet = 0;
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) -> dst.isSet = 0;
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) -> dst.isSet = 0;
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) -> dst.isSet = 0;
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) -> dst.isSet = 0;
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) -> dst.isSet = 0;
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) -> dst.isSet = 0;
            default:
                throw new AthenaConnectorException("Unhandled type " + fieldType,
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
        }
    }
}
