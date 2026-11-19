/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_QUOTE_CHARACTER;

public class PostGreSqlRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlRecordHandler.class);

    private static final int FETCH_SIZE = 1000;
    // Larger than FETCH_SIZE: export reads stream whole result sets, so fewer round trips matter more.
    private static final int EXPORT_FETCH_SIZE = 3000;
    private static final int DECIMAL_BIT_WIDTH = 128;
    private static final String DEFAULT_SCALE_KEY = "default_scale";

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link PostGreSqlMuxCompositeHandler} instead.
     */
    public PostGreSqlRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(POSTGRES_NAME, configOptions), configOptions);
    }

    public PostGreSqlRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                new GenericJdbcConnectionFactory(databaseConnectionConfig, PostGreSqlMetadataHandler.JDBC_PROPERTIES, new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, POSTGRESQL_DEFAULT_PORT), configOptions),
                new PostGreSqlQueryStringBuilder(POSTGRES_QUOTE_CHARACTER, new PostgreSqlFederationExpressionParser(POSTGRES_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    protected PostGreSqlRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager,
            AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
    {
        PreparedStatement preparedStatement;
        try {
            if (constraints.isQueryPassThrough()) {
                preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
            }
            else {
                preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
            }
            // Disable fetching all rows.
            preparedStatement.setFetchSize(FETCH_SIZE);
        }
        catch (SQLException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }

        return preparedStatement;
    }

    @Override
    protected PreparedStatement prepareExportStatement(Connection connection, String query) throws SQLException
    {
        PreparedStatement preparedStatement = super.prepareExportStatement(connection, query);
        // Without a fetch size, pgjdbc reads the whole result into memory, even with auto-commit off.
        preparedStatement.setFetchSize(EXPORT_FETCH_SIZE);
        return preparedStatement;
    }

    /**
     * Unbounded numeric becomes Decimal(38, default_scale), as on the normal read path. Types the spill cannot
     * write (time, uuid, json, interval, arrays) become VARCHAR, written as the driver's string form.
     */
    @Override
    protected Schema deriveExportSchema(ResultSet resultSet) throws SQLException
    {
        ResultSetMetaData metadata = resultSet.getMetaData();
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        List<Field> fields = new ArrayList<>();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            ArrowType type;
            if (metadata.getColumnType(column) == Types.NUMERIC && metadata.getPrecision(column) == 0 && metadata.getScale(column) <= 0) {
                type = new ArrowType.Decimal(JdbcArrowTypeConverter.DEFAULT_PRECISION,
                        Integer.parseInt(configOptions.getOrDefault(DEFAULT_SCALE_KEY, "18")), DECIMAL_BIT_WIDTH);
            }
            else {
                try {
                    type = JdbcToArrowUtils.getArrowTypeFromJdbcType(new JdbcFieldInfo(metadata, column), utc);
                }
                catch (UnsupportedOperationException e) {
                    type = ArrowType.Utf8.INSTANCE;
                }
                if (!SupportedTypes.isSupported(type) || type instanceof ArrowType.List) {
                    type = ArrowType.Utf8.INSTANCE;
                }
            }
            fields.add(Field.nullable(metadata.getColumnLabel(column), type));
        }
        return new Schema(fields);
    }
}
