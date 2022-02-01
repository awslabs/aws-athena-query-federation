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

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;

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
    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link RedshiftMuxCompositeHandler} instead.
     */
    public RedshiftMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(REDSHIFT_NAME));
    }

    public RedshiftMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        super(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(REDSHIFT_DRIVER_CLASS, REDSHIFT_DEFAULT_PORT)));
    }

    @VisibleForTesting
    RedshiftMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, AWSSecretsManager secretsManager, AmazonAthena athena, JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }
}
