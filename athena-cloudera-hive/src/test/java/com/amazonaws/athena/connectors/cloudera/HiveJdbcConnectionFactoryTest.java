/*-
 * #%L
 * athena-cloudera-hive
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

public class HiveJdbcConnectionFactoryTest 
{
    @Test(expected = SQLException.class)
    public void testGetConnection_withCredentials() throws Exception
    {
        DefaultCredentials expectedCredential = new DefaultCredentials("hive", "hive");
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(expectedCredential);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", HiveConstants.HIVE_NAME,
                "hive2://jdbc:hive2://23.21.178.97:10000/athena;AuthMech=3;UID=hive;PWD=''", "hive");
        Map<String, String> jdbcProperties = ImmutableMap.of("databaseTerm", "SCHEMA");
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo(HiveConstants.HIVE_DRIVER_CLASS, HiveConstants.HIVE_DEFAULT_PORT);
        Connection connection =  new HiveJdbcConnectionFactory(databaseConnectionConfig, jdbcProperties, databaseConnectionInfo).getConnection(credentialsProvider);
        String originalURL = connection.getMetaData().getURL();
        Driver drv = DriverManager.getDriver(originalURL);
        String driverClass = drv.getClass().getName();
        assertEquals("com.cloudera.hive.jdbc.HS2Driver", driverClass);
    }

    @Test(expected = SQLException.class)
    public void testGetConnection_withNullCredentials() throws Exception
    {
        // Setup the connection config with a test JDBC string
        String testJdbcString = "jdbc:hive2://localhost:10000/default";
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                "testCatalog",
                HiveConstants.HIVE_NAME,
                testJdbcString,
                "default"
        );

        // Setup other required parameters
        HiveJdbcConnectionFactory connectionFactory = getHiveJdbcConnectionFactory(databaseConnectionConfig);

        // Attempt to get connection with null credentials - this should use the original JDBC string
        Connection connection = connectionFactory.getConnection(null);

        // Verify the connection URL matches the original JDBC string
        assertEquals(testJdbcString, connection.getMetaData().getURL());
    }

    private static HiveJdbcConnectionFactory getHiveJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig)
    {
        Map<String, String> jdbcProperties = ImmutableMap.of("databaseTerm", "SCHEMA");
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo(
                HiveConstants.HIVE_DRIVER_CLASS,
                HiveConstants.HIVE_DEFAULT_PORT
        );

        // Create the connection factory and attempt to get a connection with null credentials
        return new HiveJdbcConnectionFactory(
                databaseConnectionConfig,
                jdbcProperties,
                databaseConnectionInfo
        );
    }
}
