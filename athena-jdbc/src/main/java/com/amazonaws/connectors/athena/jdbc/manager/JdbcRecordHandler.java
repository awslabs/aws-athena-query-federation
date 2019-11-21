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
package com.amazonaws.connectors.athena.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.connectors.athena.jdbc.connection.RdsSecretsCredentialProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstracts JDBC record handler and provides common reusable split records handling.
 */
public abstract class JdbcRecordHandler
        extends RecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordHandler.class);
    // TODO support all data types
    private static final ImmutableMap<Types.MinorType, ResultSetValueExtractor<Object>> VALUE_EXTRACTOR = new ImmutableMap.Builder<Types.MinorType, ResultSetValueExtractor<Object>>()
            .put(Types.MinorType.BIT, ResultSet::getBoolean)
            .put(Types.MinorType.TINYINT, ResultSet::getByte)
            .put(Types.MinorType.SMALLINT, ResultSet::getShort)
            .put(Types.MinorType.INT, ResultSet::getInt)
            .put(Types.MinorType.BIGINT, ResultSet::getLong)
            .put(Types.MinorType.FLOAT4, ResultSet::getFloat)
            .put(Types.MinorType.FLOAT8, ResultSet::getDouble)
            .put(Types.MinorType.DATEDAY, ResultSet::getDate)
            .put(Types.MinorType.DATEMILLI, ResultSet::getTimestamp)
            .put(Types.MinorType.VARCHAR, ResultSet::getString)
            .put(Types.MinorType.VARBINARY, ResultSet::getBytes)
            .put(Types.MinorType.DECIMAL, ResultSet::getBigDecimal)
            .build();
    private final JdbcConnectionFactory jdbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;

    /**
     * Used only by Multiplexing handler. All invocations will be delegated to respective database handler.
     */
    protected JdbcRecordHandler()
    {
        super(null);
        this.jdbcConnectionFactory = null;
        this.databaseConnectionConfig = null;
    }

    protected JdbcRecordHandler(final AmazonS3 amazonS3, final AWSSecretsManager secretsManager, AmazonAthena athena, final DatabaseConnectionConfig databaseConnectionConfig,
            final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig.getType().getDbName());
        this.jdbcConnectionFactory = Validate.notNull(jdbcConnectionFactory, "jdbcConnectionFactory must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
    }

    private JdbcCredentialProvider getCredentialProvider()
    {
        final String secretName = this.databaseConnectionConfig.getSecret();
        if (StringUtils.isNotBlank(secretName)) {
            return new RdsSecretsCredentialProvider(getSecret(secretName));
        }

        return null;
    }

    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
    {
        LOGGER.info("{}: Catalog: {}, table {}, splits {}", readRecordsRequest.getQueryId(), readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                readRecordsRequest.getSplit().getProperties());
        try (Connection connection = this.jdbcConnectionFactory.getConnection(getCredentialProvider())) {
            connection.setAutoCommit(false); // For consistency. This is needed to be false to enable streaming for some database types.
            try (PreparedStatement preparedStatement = buildSplitSql(connection, readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSchema(), readRecordsRequest.getConstraints(), readRecordsRequest.getSplit());
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();

                final Map<String, Types.MinorType> typeMap = new HashMap<>();
                int columnIndex = 1;
                for (Field nextField : readRecordsRequest.getSchema().getFields()) {
                    if (partitionValues.containsKey(nextField.getName())) {
                        continue; //ignore partition columns
                    }
                    Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(JdbcToArrowUtils.getArrowTypeForJdbcField(new JdbcFieldInfo(resultSetMetaData, columnIndex),
                            Calendar.getInstance()));
                    typeMap.put(nextField.getName(), minorTypeForArrowType);
                    columnIndex++;
                }

                while (resultSet.next()) {
                    if (!queryStatusChecker.isQueryRunning()) {
                        return;
                    }
                    blockSpiller.writeRows((Block block, int rowNum) -> {
                        try {
                            boolean matched;
                            for (Field nextField : readRecordsRequest.getSchema().getFields()) {
                                Object value;
                                if (partitionValues.containsKey(nextField.getName())) {
                                    value = partitionValues.get(nextField.getName());
                                }
                                else {
                                    value = getArrowValue(resultSet, nextField.getName(), typeMap.get(nextField.getName()));
                                }
                                matched = block.offerValue(nextField.getName(), rowNum, value);
                                if (!matched) {
                                    return 0;
                                }
                            }

                            return 1;
                        }
                        catch (SQLException sqlException) {
                            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException.getMessage(), sqlException);
                        }
                    });
                }

                connection.commit();
            }
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException.getMessage(), sqlException);
        }
    }

    private Object getArrowValue(final ResultSet resultSet, final String columnName, final Types.MinorType minorType)
            throws SQLException
    {
        return VALUE_EXTRACTOR.getOrDefault(minorType, (rs, col) -> {
            throw new RuntimeException("Unhandled column type " + minorType);
        })
                .call(resultSet, columnName);
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

    private interface ResultSetValueExtractor<T>
    {
        T call(ResultSet resultSet, String columnName)
                throws SQLException;
    }
}
