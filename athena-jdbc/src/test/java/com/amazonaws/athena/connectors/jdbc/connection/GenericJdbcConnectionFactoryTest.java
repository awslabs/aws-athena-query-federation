/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.StaticCredentialsProvider;
import com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;

import static com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory.SECRET_NAME_PATTERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class GenericJdbcConnectionFactoryTest
{
    private static final String H2_DRIVER_CLASS = "org.h2.Driver";
    private static final int H2_DEFAULT_PORT = 9092;
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_ENGINE = "h2";
    private static final String TEST_SECRET_NAME = "testSecret";
    private static final Map<String, String> EMPTY_JDBC_PROPERTIES = ImmutableMap.of();

    /**
     * Returns a fresh in-memory H2 URL for each test to avoid state leaking between tests.
     */
    private static String uniqueH2Url()
    {
        return "jdbc:h2:mem:" + UUID.randomUUID().toString().replace('-', '_') + ";DB_CLOSE_DELAY=-1";
    }

    private static GenericJdbcConnectionFactory newDirectModeFactory(String jdbcUrl)
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, jdbcUrl);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        Map<String, String> configOptions = ImmutableMap.of(EnvironmentConstants.FAS_TOKEN, "someToken");
        return new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info, configOptions);
    }

    @Test
    public void matchSecretNamePattern()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertTrue(secretMatcher.find());
    }

    @Test
    public void matchIncorrectSecretNamePattern()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=*_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertFalse(secretMatcher.find());
    }

    @Test
    public void getConnection_pooledMode_withCredentials_stripsSecretAndReturnsConnection() throws Exception
    {
        String rawUrl = uniqueH2Url() + ";INIT=CREATE SCHEMA IF NOT EXISTS TEST\\;--${" + TEST_SECRET_NAME + "}";
        // The above URL contains a ${secret} placeholder that must be stripped before the driver receives it.
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
                TEST_CATALOG, TEST_ENGINE, rawUrl, TEST_SECRET_NAME);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info);
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(new DefaultCredentials("sa", "sa"));

        try (Connection connection = factory.getConnection(credentialsProvider)) {
            assertNotNull(connection);
            Assert.assertTrue(connection.isValid(1));
        }
        finally {
            factory.close();
        }
    }

    @Test
    public void getConnection_pooledMode_nullCredentials_usesJdbcStringAsIs() throws Exception
    {
        String rawUrl = uniqueH2Url();
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, rawUrl);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info);

        try (Connection connection = factory.getConnection(null)) {
            assertNotNull(connection);
            Assert.assertTrue(connection.isValid(1));
        }
        finally {
            factory.close();
        }
    }

    @Test
    public void constructor_withNullProperties_isAcceptedAndPoolInitializes() throws Exception
    {
        // Exercises the delegating constructor and the null-properties branch.
        String rawUrl = uniqueH2Url();
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, rawUrl);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, null, info);

        try (Connection connection = factory.getConnection(null)) {
            assertNotNull(connection);
        }
        finally {
            factory.close();
        }
    }

    @Test
    public void close_beforeAnyConnection_isNoOp()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, uniqueH2Url());
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info);

        // Does not throw when no pool was ever created (ds == null branch of close()).
        factory.close();
    }

    @Test
    public void close_afterPoolInitialized_closesPoolAndAllowsReuse() throws Exception
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, uniqueH2Url());
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info);

        try (Connection connection = factory.getConnection(null)) {
            assertNotNull(connection);
        }

        factory.close();

        // close() releases the pool but must not brick the factory: a handler that is closed and
        // then reused would otherwise fail forever with "HikariDataSource has been closed".
        try (Connection reopened = factory.getConnection(null)) {
            assertNotNull(reopened);
            Assert.assertTrue(reopened.isValid(2));
        }

        factory.close();
    }

    @Test
    public void close_isIdempotent() throws Exception
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, uniqueH2Url());
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info);

        try (Connection connection = factory.getConnection(null)) {
            assertNotNull(connection);
        }

        factory.close();
        factory.close();
    }

    @Test
    public void getConnection_pooledMode_rebuildsPoolWhenCredentialsChange() throws Exception
    {
        // HikariCP snapshots credentials when the pool is built and rejects per-call credentials,
        // so a pool created for one identity would keep serving it after a later request supplied
        // different credentials -- one tenant borrowing another's connection.
        String url = uniqueH2Url();
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
                TEST_CATALOG, TEST_ENGINE, url, TEST_SECRET_NAME);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info);

        CredentialsProvider tenantA = new StaticCredentialsProvider(new DefaultCredentials("tenantA", "secretA"));
        CredentialsProvider tenantB = new StaticCredentialsProvider(new DefaultCredentials("tenantB", "secretB"));

        try (Connection first = factory.getConnection(tenantA)) {
            assertEquals("TENANTA", first.getMetaData().getUserName());
        }

        // H2 in-memory treats the first connection's credentials as the database owner, so a
        // different user proves the pool was rebuilt rather than reused with stale credentials.
        // Reuse would have silently returned a working tenantA connection instead.
        Exception failure = assertThrows(Exception.class, () -> factory.getConnection(tenantB));
        Assert.assertTrue("expected an authentication failure for the new identity, got: " + failure,
                (failure.getMessage() + String.valueOf(failure.getCause())).contains("Wrong user name or password"));

        factory.close();
    }

    @Test
    public void getConnection_pooledMode_reusesPoolForSameCredentials() throws Exception
    {
        // The identity guard must not defeat pooling: repeated calls with the same credentials
        // have to keep serving from one pool.
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, uniqueH2Url());
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info);

        String firstPool;
        try (Connection first = factory.getConnection(null)) {
            firstPool = first.unwrap(Connection.class).toString();
            assertNotNull(firstPool);
        }
        try (Connection second = factory.getConnection(null)) {
            // Same underlying physical connection handed back by the same pool.
            assertEquals(firstPool, second.unwrap(Connection.class).toString());
        }

        factory.close();
    }

    @Test
    public void getConnection_pooledMode_distinctFactoriesDoNotBlockEachOther() throws Exception
    {
        // The pool-init lock is per instance, not per class: two unrelated factories (e.g. two
        // multiplexed catalogs) must be able to build their pools concurrently.
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        GenericJdbcConnectionFactory one = new GenericJdbcConnectionFactory(
                new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, uniqueH2Url()), EMPTY_JDBC_PROPERTIES, info);
        GenericJdbcConnectionFactory two = new GenericJdbcConnectionFactory(
                new DatabaseConnectionConfig("otherCatalog", TEST_ENGINE, uniqueH2Url()), EMPTY_JDBC_PROPERTIES, info);

        try (Connection a = one.getConnection(null); Connection b = two.getConnection(null)) {
            assertNotNull(a);
            assertNotNull(b);
            // Independent pools, so independent physical connections.
            Assert.assertNotEquals(a.toString(), b.toString());
        }

        one.close();
        two.close();
    }

    @Test
    public void constructor_directMode_missingDriver_throwsAthenaConnectorException()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, uniqueH2Url());
        DatabaseConnectionInfo info = new DatabaseConnectionInfo("not.a.real.jdbc.Driver", H2_DEFAULT_PORT);
        Map<String, String> configOptions = ImmutableMap.of(EnvironmentConstants.FAS_TOKEN, "someToken");

        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class, () ->
                new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info, configOptions));

        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(), ex.getErrorDetails().errorCode());
        Assert.assertTrue(ex.getMessage().contains("JDBC driver not found"));
    }

    @Test
    public void constructor_pooledMode_noFasToken_doesNotAttemptDriverLoad()
    {
        // When FAS_TOKEN is absent, the driver class is only loaded lazily by Hikari, so an unknown
        // driver class name in the info block does NOT throw at construction time.
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(TEST_CATALOG, TEST_ENGINE, uniqueH2Url());
        DatabaseConnectionInfo info = new DatabaseConnectionInfo("not.a.real.jdbc.Driver", H2_DEFAULT_PORT);
        Map<String, String> configOptions = ImmutableMap.of();

        // No exception expected.
        GenericJdbcConnectionFactory factory =
                new GenericJdbcConnectionFactory(config, EMPTY_JDBC_PROPERTIES, info, configOptions);
        Assert.assertNotNull(factory);
    }

    @Test
    public void getConnection_directMode_withCredentials_stripsSecretAndMergesProperties() throws Exception
    {
        String rawUrl = "jdbc:h2:mem:direct?param=${" + TEST_SECRET_NAME + "}";
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
                TEST_CATALOG, TEST_ENGINE, rawUrl, TEST_SECRET_NAME);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(H2_DRIVER_CLASS, H2_DEFAULT_PORT);
        Map<String, String> configOptions = ImmutableMap.of(EnvironmentConstants.FAS_TOKEN, "someToken");
        Map<String, String> jdbcProperties = ImmutableMap.of("existing", "value");

        GenericJdbcConnectionFactory factory =
                new GenericJdbcConnectionFactory(config, jdbcProperties, info, configOptions);
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(new DefaultCredentials("sa", "sa"));
        Connection mockConnection = Mockito.mock(Connection.class);
        ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);

        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            mockedDriverManager.when(() -> DriverManager.getConnection(any(String.class), any(Properties.class)))
                    .thenReturn(mockConnection);

            Connection result = factory.getConnection(credentialsProvider);

            assertEquals(mockConnection, result);
            String expectedStrippedUrl = "jdbc:h2:mem:direct?param=";
            mockedDriverManager.verify(() -> DriverManager.getConnection(eq(expectedStrippedUrl), propsCaptor.capture()));
        }

        Properties actualProps = propsCaptor.getValue();
        // The pre-existing property and the credentials were both merged into the properties passed to DriverManager.
        assertEquals("value", actualProps.getProperty("existing"));
        assertEquals("sa", actualProps.getProperty("user"));
        assertEquals("sa", actualProps.getProperty("password"));
    }

    @Test
    public void getConnection_directMode_nullCredentials_passesUrlThrough() throws Exception
    {
        String rawUrl = uniqueH2Url();
        GenericJdbcConnectionFactory factory = newDirectModeFactory(rawUrl);
        Connection mockConnection = Mockito.mock(Connection.class);

        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            mockedDriverManager.when(() -> DriverManager.getConnection(any(String.class), any(Properties.class)))
                    .thenReturn(mockConnection);

            Connection result = factory.getConnection(null);

            assertEquals(mockConnection, result);
            mockedDriverManager.verify(() -> DriverManager.getConnection(eq(rawUrl), any(Properties.class)));
        }
    }

    @Test
    public void getConnection_directMode_nameOrServiceNotKnown_wrapsAsInvalidInputException()
    {
        GenericJdbcConnectionFactory factory = newDirectModeFactory(uniqueH2Url());
        // Construct the SQLException BEFORE opening the MockedStatic<DriverManager> scope.
        // SQLException's constructor calls DriverManager.getLogWriter(), which Mockito would
        // otherwise treat as an interaction mid-stubbing and fail with UnfinishedStubbingException.
        SQLException driverError = new SQLException("Communications link failure: Name or service not known");

        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            mockedDriverManager.when(() -> DriverManager.getConnection(any(String.class), any(Properties.class)))
                    .thenThrow(driverError);

            AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                    () -> factory.getConnection(null));

            assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(),
                    ex.getErrorDetails().errorCode());
        }
    }

    @Test
    public void getConnection_directMode_incorrectCredentials_wrapsAsInvalidCredentialsException()
    {
        GenericJdbcConnectionFactory factory = newDirectModeFactory(uniqueH2Url());
        // See note in the sibling test — SQLException must be constructed outside the MockedStatic scope.
        SQLException driverError = new SQLException("Incorrect username or password was specified.");

        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            mockedDriverManager.when(() -> DriverManager.getConnection(any(String.class), any(Properties.class)))
                    .thenThrow(driverError);

            AthenaConnectorException ex = assertThrows(AthenaConnectorException.class,
                    () -> factory.getConnection(null));

            assertEquals(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION.toString(),
                    ex.getErrorDetails().errorCode());
        }
    }

    @Test
    public void getConnection_directMode_genericSqlException_propagates()
    {
        GenericJdbcConnectionFactory factory = newDirectModeFactory(uniqueH2Url());
        SQLException expected = new SQLException("Some other database error");

        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            mockedDriverManager.when(() -> DriverManager.getConnection(any(String.class), any(Properties.class)))
                    .thenThrow(expected);

            SQLException actual = assertThrows(SQLException.class, () -> factory.getConnection(null));
            assertEquals(expected, actual);
        }
    }
}
