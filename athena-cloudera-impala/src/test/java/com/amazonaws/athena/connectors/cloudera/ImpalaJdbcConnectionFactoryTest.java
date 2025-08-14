/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.StaticCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ImpalaJdbcConnectionFactoryTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_DEFAULT_DATABASE = "default";
    private static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");

    @Test(expected = RuntimeException.class)
    public void testGetConnection() throws SQLException
    {
        String impalaConnectionString = "impala://jdbc:impala://localhost:10000/athena;AuthMech=3;UID=hive;PWD=''";
            
        DefaultCredentials expectedCredential = new DefaultCredentials("impala", "impala");
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(expectedCredential);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, ImpalaConstants.IMPALA_NAME,
                impalaConnectionString, "impala");
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo(ImpalaConstants.IMPALA_DRIVER_CLASS, ImpalaConstants.IMPALA_DEFAULT_PORT);
        Connection connection = new ImpalaJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, databaseConnectionInfo).getConnection(credentialsProvider);
        String originalURL = connection.getMetaData().getURL();
        Driver drv = DriverManager.getDriver(originalURL);
        String driverClass = drv.getClass().getName();
        assertEquals("com.cloudera.impala.jdbc.Driver", driverClass);
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnection_withNullCredentials() throws Exception
    {
        String localJdbcString = String.format("jdbc:impala://localhost:10000/%s", TEST_DEFAULT_DATABASE);
        
        // Setup the connection config with a test JDBC string
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                TEST_CATALOG,
                ImpalaConstants.IMPALA_NAME,
                localJdbcString,
                TEST_DEFAULT_DATABASE
        );

        // Setup other required parameters
        ImpalaJdbcConnectionFactory connectionFactory = getImpalaJdbcConnectionFactory(databaseConnectionConfig);

        // Attempt to get connection with null credentials - this should use the original JDBC string
        Connection connection = connectionFactory.getConnection(null);

        // Verify the connection URL matches the original JDBC string
        assertEquals(localJdbcString, connection.getMetaData().getURL());
    }

    private static ImpalaJdbcConnectionFactory getImpalaJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig)
    {
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo(
                ImpalaConstants.IMPALA_DRIVER_CLASS,
                ImpalaConstants.IMPALA_DEFAULT_PORT
        );

        // Create the connection factory and attempt to get a connection with null credentials
        return new ImpalaJdbcConnectionFactory(
                databaseConnectionConfig,
                JDBC_PROPERTIES,
                databaseConnectionInfo
        );
    }
}
