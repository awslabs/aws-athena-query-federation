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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler.TABLES_AND_VIEWS;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_NAME;

/**
 * Handles metadata for PostGreSql. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class RedshiftMetadataHandler
        extends PostGreSqlMetadataHandler
{
    static final String LIST_PAGINATED_TABLES_QUERY = "SELECT a.\"TABLE_NAME\", a.\"TABLE_SCHEM\" FROM (( SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ?) UNION (SELECT tablename as \"TABLE_NAME\", schemaname as \"TABLE_SCHEM\" FROM svv_external_tables where schemaname = ?)) AS a ORDER BY a.\"TABLE_NAME\" LIMIT ? OFFSET ?";
    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftMetadataHandler.class);

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link RedshiftMuxCompositeHandler} instead.
     */
    public RedshiftMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(REDSHIFT_NAME, configOptions), configOptions);
    }

    public RedshiftMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(REDSHIFT_DRIVER_CLASS, REDSHIFT_DEFAULT_PORT)), configOptions);
    }

    @VisibleForTesting
    RedshiftMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, AWSSecretsManager secretsManager, AmazonAthena athena, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions);
    }

    @Override
    public String caseInsensitiveSchemaResolver(Connection connection, String databaseName) throws SQLException
    {
        String sql = "SELECT nspname FROM pg_namespace WHERE (nspname = ? or lower(nspname) = ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setString(2, databaseName);

        String resolvedSchemaName = null;
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                resolvedSchemaName = resultSet.getString("nspname");
                LOGGER.info("Resolved Schema from Case Insensitive look up : {}", resolvedSchemaName);
            }
        }
        return resolvedSchemaName;
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
            FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
            LimitPushdownSubType.INTEGER_CONSTANT
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
            ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
            .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                    .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                    .toArray(String[]::new))
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
            TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    protected List<TableName> getPaginatedResults(Connection connection, String databaseName, int token, int limit) throws SQLException
    {
        PreparedStatement preparedStatement = connection.prepareStatement(LIST_PAGINATED_TABLES_QUERY);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setString(2, databaseName);
        preparedStatement.setInt(3, limit);
        preparedStatement.setInt(4, token);
        LOGGER.debug("Prepared Statement for getting tables in schema {} : {}", databaseName, preparedStatement);
        return JDBCUtil.getTableMetadata(preparedStatement, TABLES_AND_VIEWS);
    }

    @Override
    protected PreparedStatement getMaterializedViewOrExternalTable(Connection connection, String tableName, String databaseName) throws SQLException
    {
        String sql = "SELECT tablename as \"TABLE_NAME\", schemaname as \"TABLE_SCHEM\" FROM svv_external_tables where schemaname = ? and lower(tablename) = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setString(2, tableName);
        LOGGER.debug("Prepared statement for getting name of External Table with Case Insensitive Look Up: {}", preparedStatement);
        return preparedStatement;
    }
}
