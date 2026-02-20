/*-
 * #%L
 * athena-oracle
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OracleCredentialsProviderTest
{
    // Test credentials
    private static final String TEST_USER = "testUser";
    private static final String TEST_PASSWORD = "testPassword";
    private static final String TEST_SECRET = String.format(
            "{\"username\":\"%s\", \"password\":\"%s\"}",
            TEST_USER,
            TEST_PASSWORD
    );
    private static final String INVALID_SECRET_STRING = "invalid-json";

    private static final String ORACLE_JDBC_PREFIX = "oracle://jdbc:oracle:thin:${secret_name}@";
    private static final String CONNECTION_PATH = "testHost:1521/orcl";

    private static final String BASE_CONNECTION_STRING =
            ORACLE_JDBC_PREFIX + "//" + CONNECTION_PATH;

    private static final String TCPS_CONNECTION_STRING =
            ORACLE_JDBC_PREFIX + "tcps://" + CONNECTION_PATH;

    /**
     * Test subclass to simulate FIPS mode.
     */
    private static class TestOracleCredentialsProvider extends OracleCredentialsProvider
    {
        private final boolean fipsEnabled;

        public TestOracleCredentialsProvider(String secretString, String jdbcConnectionString, boolean fipsEnabled)
        {
            super(secretString, jdbcConnectionString);
            this.fipsEnabled = fipsEnabled;
        }

        @Override
        protected String getFipsEnabledEnv()
        {
            return fipsEnabled ? "true" : null;
        }

        @Override
        protected String getFipsEnabledLegacyEnv()
        {
            return null;
        }
    }

    @Test
    public void getCredentialMap_withValidSecret_returnsValidCredentials()
    {
        OracleCredentialsProvider provider = new OracleCredentialsProvider(TEST_SECRET, BASE_CONNECTION_STRING);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals(String.format("\"%s\"", TEST_PASSWORD), credMap.get(CredentialsConstants.PASSWORD));
        assertEquals(2, credMap.size()); // Should only contain user and password
    }

    @Test
    public void getCredentialMap_withTcpsConnection_returnsValidCredentialsWithSslProperties()
    {
        OracleCredentialsProvider provider = new OracleCredentialsProvider(TEST_SECRET, TCPS_CONNECTION_STRING);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals(String.format("\"%s\"", TEST_PASSWORD), credMap.get(CredentialsConstants.PASSWORD));
        assertEquals("JKS", credMap.get("javax.net.ssl.trustStoreType"));
        assertEquals("changeit", credMap.get("javax.net.ssl.trustStorePassword"));
        assertEquals("true", credMap.get("oracle.net.ssl_server_dn_match"));
    }

    @Test
    public void getCredentialMap_withFipsEnabled_returnsValidCredentialsWithCipherSuites()
    {
        String expectedCipherSuites = String.join(",",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
        );

        OracleCredentialsProvider provider = new TestOracleCredentialsProvider(TEST_SECRET, TCPS_CONNECTION_STRING, true);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(expectedCipherSuites, credMap.get("oracle.net.ssl_cipher_suites"));
    }

    @Test
    public void createOracleCredentialsProvider_withInvalidSecret_throwsAthenaConnectorException()
    {
        try {
            new OracleCredentialsProvider(INVALID_SECRET_STRING, BASE_CONNECTION_STRING);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain deserialization error",
                    ex.getMessage().contains("Could not deserialize RDS credentials into HashMap:"));
        }
    }

    @Test
    public void getCredentialMap_withNullConnectionString_returnsOnlyUserAndPassword()
    {
        OracleCredentialsProvider provider = new OracleCredentialsProvider(TEST_SECRET, null);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals(String.format("\"%s\"", TEST_PASSWORD), credMap.get(CredentialsConstants.PASSWORD));
        assertEquals(2, credMap.size());
    }

    @Test
    public void getCredentialMap_withEmptyConnectionString_returnsOnlyUserAndPassword()
    {
        OracleCredentialsProvider provider = new OracleCredentialsProvider(TEST_SECRET, "");
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals(String.format("\"%s\"", TEST_PASSWORD), credMap.get(CredentialsConstants.PASSWORD));
        assertEquals(2, credMap.size());
    }

    @Test
    public void getCredentialMap_withPasswordAlreadyQuoted_doesNotDoubleWrap()
    {
        String secretWithQuotedPassword = "{\"username\":\"user1\", \"password\":\"\\\"alreadyQuoted\\\"\"}";
        OracleCredentialsProvider provider = new OracleCredentialsProvider(secretWithQuotedPassword, BASE_CONNECTION_STRING);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals("user1", credMap.get(CredentialsConstants.USER));
        assertEquals("\"alreadyQuoted\"", credMap.get(CredentialsConstants.PASSWORD));
        assertEquals(2, credMap.size());
    }

    @Test
    public void getCredentialMap_withSpecialCharactersInPassword_wrapsInQuotes()
    {
        String specialPassword = "p@ss!w0rd#with$pecial";
        String secret = String.format("{\"username\":\"u\", \"password\":\"%s\"}", specialPassword);
        OracleCredentialsProvider provider = new OracleCredentialsProvider(secret, BASE_CONNECTION_STRING);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals("u", credMap.get(CredentialsConstants.USER));
        assertEquals(String.format("\"%s\"", specialPassword), credMap.get(CredentialsConstants.PASSWORD));
        assertEquals(2, credMap.size());
    }

    @Test
    public void getCredentialMap_withTcpsAndFipsDisabled_returnsSslPropertiesButNoCipherSuites()
    {
        OracleCredentialsProvider provider = new TestOracleCredentialsProvider(TEST_SECRET, TCPS_CONNECTION_STRING, false);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals(String.format("\"%s\"", TEST_PASSWORD), credMap.get(CredentialsConstants.PASSWORD));
        assertEquals("JKS", credMap.get("javax.net.ssl.trustStoreType"));
        assertEquals("changeit", credMap.get("javax.net.ssl.trustStorePassword"));
        assertEquals("true", credMap.get("oracle.net.ssl_server_dn_match"));
        assertNull(credMap.get("oracle.net.ssl_cipher_suites"));
    }

    @Test
    public void getCredentialMap_withTcpsUpperCase_addsSslProperties()
    {
        String connectionStringUpper = ORACLE_JDBC_PREFIX + "TCPS://" + CONNECTION_PATH;
        OracleCredentialsProvider provider = new OracleCredentialsProvider(TEST_SECRET, connectionStringUpper);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals("JKS", credMap.get("javax.net.ssl.trustStoreType"));
        assertEquals("true", credMap.get("oracle.net.ssl_server_dn_match"));
    }

    @Test
    public void getCredentialMap_withTcpsMixedCase_addsSslProperties()
    {
        String connectionStringMixed = ORACLE_JDBC_PREFIX + "Tcps://" + CONNECTION_PATH;
        OracleCredentialsProvider provider = new OracleCredentialsProvider(TEST_SECRET, connectionStringMixed);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals("JKS", credMap.get("javax.net.ssl.trustStoreType"));
        assertEquals("true", credMap.get("oracle.net.ssl_server_dn_match"));
    }

    @Test
    public void getCredentialMap_withNonTcpsProtocol_doesNotAddSslProperties()
    {
        OracleCredentialsProvider provider = new OracleCredentialsProvider(TEST_SECRET, BASE_CONNECTION_STRING);
        Map<String, String> credMap = provider.getCredentialMap();

        assertEquals(TEST_USER, credMap.get(CredentialsConstants.USER));
        assertEquals(2, credMap.size());
        assertNull(credMap.get("javax.net.ssl.trustStoreType"));
        assertNull(credMap.get("oracle.net.ssl_server_dn_match"));
    }
}
