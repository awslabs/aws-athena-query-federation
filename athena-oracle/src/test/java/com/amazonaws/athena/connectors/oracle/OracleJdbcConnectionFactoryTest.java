/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredential;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.connection.StaticJdbcCredentialProvider;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleJdbcConnectionFactoryTest {

    @Test(expected = RuntimeException.class)
    public void getConnectionTest() throws SQLException {
        JdbcCredential expectedCredential = new JdbcCredential("test", "test");
        JdbcCredentialProvider jdbcCredentialProvider = new StaticJdbcCredentialProvider(expectedCredential);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", OracleConstants.ORACLE_NAME,
                "oracle://jdbc:oracle:thin:username/password@//127.0.0.1:1521/orcl", "test");
        DatabaseConnectionInfo DatabaseConnectionInfo = new DatabaseConnectionInfo(OracleConstants.ORACLE_DRIVER_CLASS,OracleConstants.ORACLE_DEFAULT_PORT);
        Connection connection =  new OracleJdbcConnectionFactory(databaseConnectionConfig, DatabaseConnectionInfo).getConnection(jdbcCredentialProvider);
        String originalURL = connection.getMetaData().getURL();
        Driver drv = DriverManager.getDriver(originalURL);
        String driverClass = drv.getClass().getName();
        Assert.assertEquals("oracle.jdbc.OracleDriver", driverClass);
    }

}
