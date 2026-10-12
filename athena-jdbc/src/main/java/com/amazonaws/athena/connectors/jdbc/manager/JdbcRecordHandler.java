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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Abstracts JDBC record handler and provides common reusable split records handling.
 */
public abstract class JdbcRecordHandler
        extends RecordHandler
        implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordHandler.class);
    private final JdbcConnectionFactory jdbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private static final String CLICKHOUSE_DB = "clickhouse";
    /**
     * Lambda environment variable. When {@code true}, a row that cannot be converted is logged with the
     * column name, a truncated string form of that cell, and the primary-key values. Off by default so
     * row data is not written to logs or added to the exception Athena returns. See GitHub issue 364.
     */
    static final String ENABLE_PROBLEM_ROW_LOGGING = "enable_problem_row_logging";
    private static final int MAX_LOGGED_VALUE_LENGTH = 256;
    private static final String MYSQL_PRODUCT_NAME = "MySQL";
    /**
     * count of rows read from a MySQL streaming result. Connector/J rejects {@code ResultSet.getRow()}
     * when {@code fetchSize} is {@code Integer.MIN_VALUE}. Other drivers use {@code getRow()}.
     */
    private int mysqlStreamedRow;

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

            // MySQL streaming (fetchSize = Integer.MIN_VALUE) rejects another statement while the query ResultSet is open.
            boolean logProblemRows = isProblemRowLoggingEnabled();
            List<String> primaryKeyColumns = logProblemRows
                    ? lookupPrimaryKeyColumns(connection, readRecordsRequest.getTableName())
                    : Collections.emptyList();
            mysqlStreamedRow = 0;
            boolean trackMysqlStreamedRow = logProblemRows && MYSQL_PRODUCT_NAME.equalsIgnoreCase(databaseProductName);

            try (PreparedStatement preparedStatement = buildSplitSql(connection, readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSchema(), readRecordsRequest.getConstraints(), readRecordsRequest.getSplit());
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();
                Map<String, String> colNameRemapping = getColumnNameRemapping(readRecordsRequest);

                GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints());
                for (Field next : readRecordsRequest.getSchema().getFields()) {
                    registerRowField(rowWriterBuilder, next, resultSet, partitionValues, colNameRemapping, logProblemRows, primaryKeyColumns);
                }

                GeneratedRowWriter rowWriter = rowWriterBuilder.build();
                int rowsReturnedFromDatabase = 0;
                while (resultSet.next()) {
                    if (trackMysqlStreamedRow) {
                        mysqlStreamedRow++;
                    }
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
     * Registers one schema field on the row writer. When problem-row logging is enabled, conversion
     * failures are logged and then rethrown unchanged.
     */
    protected void registerRowField(
            GeneratedRowWriter.RowWriterBuilder rowWriterBuilder,
            Field field,
            ResultSet resultSet,
            Map<String, String> partitionValues,
            Map<String, String> colNameRemapping,
            boolean logProblemRows,
            List<String> primaryKeyColumns)
    {
        String fieldName = colNameRemapping.getOrDefault(field.getName(), field.getName());
        if (field.getType() instanceof ArrowType.List) {
            FieldWriterFactory factory = makeFactory(field);
            if (logProblemRows) {
                factory = wrapListFactory(factory, fieldName, resultSet, primaryKeyColumns);
            }
            rowWriterBuilder.withFieldWriterFactory(field.getName(), factory);
            return;
        }

        Extractor extractor = makeExtractor(field, resultSet, partitionValues, colNameRemapping);
        if (logProblemRows && !partitionValues.containsKey(fieldName)) {
            extractor = wrapExtractor(
                    extractor, Types.getMinorTypeForArrowType(field.getType()), fieldName, resultSet, primaryKeyColumns);
        }
        rowWriterBuilder.withExtractor(field.getName(), extractor);
    }

    protected boolean isProblemRowLoggingEnabled()
    {
        if (configOptions == null) {
            return false;
        }
        return Boolean.parseBoolean(configOptions.getOrDefault(ENABLE_PROBLEM_ROW_LOGGING, "false"));
    }

    /**
     * Reads primary-key column names for diagnostics. Failures here are logged without row data and do not fail the query.
     */
    protected List<String> lookupPrimaryKeyColumns(Connection connection, TableName tableName)
    {
        Map<Short, String> ordered = new TreeMap<>();
        try (ResultSet keys = connection.getMetaData().getPrimaryKeys(
                connection.getCatalog(), tableName.getSchemaName(), tableName.getTableName())) {
            while (keys != null && keys.next()) {
                ordered.put(keys.getShort("KEY_SEQ"), keys.getString("COLUMN_NAME"));
            }
            if (ordered.isEmpty()) {
                LOGGER.warn("enable_problem_row_logging is set but no primary key metadata was found for {}.{}",
                        tableName.getSchemaName(), tableName.getTableName());
            }
        }
        catch (SQLException e) {
            LOGGER.warn("enable_problem_row_logging is set but primary key metadata could not be read for {}.{}: {}",
                    tableName.getSchemaName(), tableName.getTableName(), e.getMessage());
        }
        return new ArrayList<>(ordered.values());
    }

    private FieldWriterFactory wrapListFactory(FieldWriterFactory factory, String fieldName, ResultSet resultSet, List<String> primaryKeyColumns)
    {
        return (vector, extractor, constraint) -> {
            FieldWriter delegate = factory.create(vector, extractor, constraint);
            return (context, rowNum) -> {
                boolean[] written = {false};
                runGuarded(fieldName, resultSet, primaryKeyColumns, () -> written[0] = delegate.write(context, rowNum));
                return written[0];
            };
        };
    }

    private Extractor wrapExtractor(
            Extractor extractor,
            Types.MinorType fieldType,
            String fieldName,
            ResultSet resultSet,
            List<String> primaryKeyColumns)
    {
        switch (fieldType) {
            case BIT:
                BitExtractor bitExtractor = (BitExtractor) extractor;
                return (BitExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> bitExtractor.extract(context, dst));
            case TINYINT:
                TinyIntExtractor tinyIntExtractor = (TinyIntExtractor) extractor;
                return (TinyIntExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> tinyIntExtractor.extract(context, dst));
            case SMALLINT:
                SmallIntExtractor smallIntExtractor = (SmallIntExtractor) extractor;
                return (SmallIntExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> smallIntExtractor.extract(context, dst));
            case INT:
                IntExtractor intExtractor = (IntExtractor) extractor;
                return (IntExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> intExtractor.extract(context, dst));
            case BIGINT:
                BigIntExtractor bigIntExtractor = (BigIntExtractor) extractor;
                return (BigIntExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> bigIntExtractor.extract(context, dst));
            case FLOAT4:
                Float4Extractor float4Extractor = (Float4Extractor) extractor;
                return (Float4Extractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> float4Extractor.extract(context, dst));
            case FLOAT8:
                Float8Extractor float8Extractor = (Float8Extractor) extractor;
                return (Float8Extractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> float8Extractor.extract(context, dst));
            case DECIMAL:
                DecimalExtractor decimalExtractor = (DecimalExtractor) extractor;
                return (DecimalExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> decimalExtractor.extract(context, dst));
            case DATEDAY:
                DateDayExtractor dateDayExtractor = (DateDayExtractor) extractor;
                return (DateDayExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> dateDayExtractor.extract(context, dst));
            case DATEMILLI:
                DateMilliExtractor dateMilliExtractor = (DateMilliExtractor) extractor;
                return (DateMilliExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> dateMilliExtractor.extract(context, dst));
            case VARCHAR:
                VarCharExtractor varCharExtractor = (VarCharExtractor) extractor;
                return (VarCharExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> varCharExtractor.extract(context, dst));
            case VARBINARY:
                VarBinaryExtractor varBinaryExtractor = (VarBinaryExtractor) extractor;
                return (VarBinaryExtractor) (context, dst) -> runGuarded(fieldName, resultSet, primaryKeyColumns, () -> varBinaryExtractor.extract(context, dst));
            default:
                return extractor;
        }
    }

    private void runGuarded(String fieldName, ResultSet resultSet, List<String> primaryKeyColumns, CheckedRunnable action)
            throws Exception
    {
        try {
            action.run();
        }
        catch (Exception e) {
            try {
                logProblemRow(fieldName, resultSet, primaryKeyColumns, e);
            }
            catch (Exception loggingFailure) {
                LOGGER.warn("Failed to log problem row for column {}: {}", fieldName, loggingFailure.toString());
            }
            throw e;
        }
    }

    private void logProblemRow(String columnName, ResultSet resultSet, List<String> primaryKeyColumns, Exception error)
    {
        reportProblemRow(columnName, readCellAsString(resultSet, columnName), formatPrimaryKey(resultSet, primaryKeyColumns), error);
    }

    /**
     * Logs the failing cell. The original exception is not modified, so the value is not returned to Athena.
     */
    @VisibleForTesting
    protected void reportProblemRow(String columnName, String stringValue, String primaryKey, Exception error)
    {
        LOGGER.warn("Incompatible database row. column={} value={} primaryKey={} error={}",
                columnName, stringValue, primaryKey, error.toString());
    }

    private String readCellAsString(ResultSet resultSet, String columnName)
    {
        try {
            String value = resultSet.getString(columnName);
            if (value == null) {
                return "null";
            }
            if (value.length() > MAX_LOGGED_VALUE_LENGTH) {
                return value.substring(0, MAX_LOGGED_VALUE_LENGTH) + "...";
            }
            return value;
        }
        catch (Exception e) {
            return "<unreadable>";
        }
    }

    private String formatPrimaryKey(ResultSet resultSet, List<String> primaryKeyColumns)
    {
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            return "unavailable(row=" + resultPosition(resultSet) + ")";
        }
        List<String> parts = new ArrayList<>();
        for (String column : primaryKeyColumns) {
            if (!isColumnProjected(resultSet, column)) {
                parts.add(column + "=not-in-projection(row=" + resultPosition(resultSet) + ")");
            }
            else {
                parts.add(column + "=" + readCellAsString(resultSet, column));
            }
        }
        return String.join(", ", parts);
    }

    private String resultPosition(ResultSet resultSet)
    {
        try {
            int row = resultSet.getRow();
            if (row > 0) {
                return Integer.toString(row);
            }
        }
        catch (SQLException e) {
            if (mysqlStreamedRow > 0) {
                return Integer.toString(mysqlStreamedRow);
            }
        }
        return "<unreadable>";
    }

    private boolean isColumnProjected(ResultSet resultSet, String columnName)
    {
        try {
            ResultSetMetaData metadata = resultSet.getMetaData();
            for (int index = 1; index <= metadata.getColumnCount(); index++) {
                if (columnName.equalsIgnoreCase(metadata.getColumnLabel(index))
                        || columnName.equalsIgnoreCase(metadata.getColumnName(index))) {
                    return true;
                }
            }
        }
        catch (SQLException e) {
            return false;
        }
        return false;
    }

    @FunctionalInterface
    private interface CheckedRunnable
    {
        void run() throws Exception;
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

    /**
     * Closes the underlying {@link JdbcConnectionFactory}, releasing any pooled connections
     * and their associated background threads. Callers that create handler instances per-request
     * (e.g., multi-tenant wrappers) should call this method when the handler is no longer needed
     * to prevent thread and memory leaks from unreleased connection pools.
     */
    @Override
    public void close() throws Exception
    {
        if (jdbcConnectionFactory != null) {
            jdbcConnectionFactory.close();
        }
    }
}
