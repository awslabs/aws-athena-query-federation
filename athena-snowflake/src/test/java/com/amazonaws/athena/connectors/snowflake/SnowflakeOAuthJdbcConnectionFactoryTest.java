/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SnowflakeOAuthJdbcConnectionFactoryTest
{
    private DatabaseConnectionConfig mockConfig;
    private DatabaseConnectionInfo mockInfo;
    private CredentialsProvider mockCredentialsProvider;
    private DefaultCredentials mockCredential;
    private SecretsManagerClient mockSecretsClient;
    private MockedStatic<SecretsManagerClient> secretsMock;
    private MockedStatic<DriverManager> driverManagerMock;
    private MockedStatic<SnowflakeOAuthJdbcConnectionFactory> httpMock;

    @Before
    public void setup()
    {
        mockConfig = mock(DatabaseConnectionConfig.class);
        when(mockConfig.getSecret()).thenReturn("my-secret");
        when(mockConfig.getJdbcConnectionString()).thenReturn("jdbc:fakedatabase://hostname/${testSecret}");

        mockInfo = mock(DatabaseConnectionInfo.class);
        when(mockInfo.getDriverClassName()).thenReturn("com.snowflake.client.jdbc.SnowflakeDriver");

        mockCredentialsProvider = mock(CredentialsProvider.class);

        mockSecretsClient = mock(SecretsManagerClient.class);
        secretsMock = mockStatic(SecretsManagerClient.class);
        secretsMock.when(SecretsManagerClient::create).thenReturn(mockSecretsClient);
    }

    @After
    public void tearDown()
    {
        if (secretsMock != null) {
            secretsMock.close();
        }
        if (driverManagerMock != null) {
            driverManagerMock.close();
        }
        if (httpMock != null) {
            httpMock.close();
        }
    }

    private void mockSecretsManager(String secretJson)
    {
        GetSecretValueResponse secretResponse = GetSecretValueResponse.builder()
                .secretString(secretJson).build();
        when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(secretResponse);
        when(mockSecretsClient.putSecretValue(any(PutSecretValueRequest.class))).thenReturn(null);
    }

    private SnowflakeOAuthJdbcConnectionFactory createFactory()
    {
        return new SnowflakeOAuthJdbcConnectionFactory(
                mockConfig,
                Map.of("key", "value"),
                mockInfo,
                Map.of("spill_bucket", "test-bucket")
        );
    }

    @Test
    public void testGetConnection() throws Exception
    {
        String secretJson = "{\"client_id\":\"cid\",\"client_secret\":\"cs\",\"token_url\":\"http://localhost/token\","
                + "\"redirect_uri\":\"http://localhost/callback\",\"auth_code\":\"auth\",\"username\":\"test\"}";

        // Mock HTTP
        httpMock = mockStatic(SnowflakeOAuthJdbcConnectionFactory.class);
        HttpURLConnection mockHttp = mock(HttpURLConnection.class);
        when(mockHttp.getOutputStream()).thenReturn(mock(OutputStream.class));
        when(mockHttp.getResponseCode()).thenReturn(200);
        when(mockHttp.getInputStream()).thenReturn(new ByteArrayInputStream(
                "{\"access_token\":\"abc123\",\"token_type\":\"Bearer\",\"expires_in\":3600,\"refresh_token\":\"refresh123\"}".getBytes()));
        httpMock.when(() -> SnowflakeOAuthJdbcConnectionFactory.getHttpURLConnection(any(), any(), any()))
                .thenReturn(mockHttp);

        mockSecretsManager(secretJson);
        driverManagerMock = mockStatic(DriverManager.class);
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(mock(Connection.class));

        Connection conn = createFactory().getConnection(mockCredentialsProvider);
        assertNotNull(conn);
    }

    @Test
    public void testGetConnectionWithExistingToken() throws Exception
    {
        String secretJson = "{\"client_id\":\"cid\",\"client_secret\":\"cs\",\"token_url\":\"http://localhost/token\","
                + "\"redirect_uri\":\"http://localhost/callback\",\"auth_code\":\"auth\","
                + "\"access_token\":\"token_from_secret\",\"expires_in\":\"3600\","
                + "\"refresh_token\":\"refresh_token_secret\",\"username\":\"test\",\"fetched_at\":\"" + (System.currentTimeMillis() / 1000 - 1000) + "\"}";

        mockSecretsManager(secretJson);
        driverManagerMock = mockStatic(DriverManager.class);
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(mock(Connection.class));

        Connection conn = createFactory().getConnection(mockCredentialsProvider);
        assertNotNull(conn);
    }

    @Test
    public void testGetConnectionWithExpiredToken() throws Exception
    {
        String secretJson = "{\"client_id\":\"cid\",\"client_secret\":\"cs\",\"token_url\":\"http://localhost/token\","
                + "\"redirect_uri\":\"http://localhost/callback\",\"auth_code\":\"auth\","
                + "\"access_token\":\"expired_token\",\"expires_in\":\"3600\","
                + "\"refresh_token\":\"refresh_token_expired\",\"username\":\"test\",\"fetched_at\":\"" + (System.currentTimeMillis() / 1000 - 4000) + "\"}";

        mockSecretsManager(secretJson);

        HttpURLConnection mockHttp = mock(HttpURLConnection.class);
        when(mockHttp.getOutputStream()).thenReturn(mock(OutputStream.class));
        when(mockHttp.getResponseCode()).thenReturn(200);
        when(mockHttp.getInputStream()).thenReturn(new ByteArrayInputStream(
                "{\"access_token\":\"new_token\",\"token_type\":\"Bearer\",\"expires_in\":3600,\"refresh_token\":\"new_refresh\"}".getBytes()));

        httpMock = mockStatic(SnowflakeOAuthJdbcConnectionFactory.class);
        httpMock.when(() -> SnowflakeOAuthJdbcConnectionFactory.getHttpURLConnection(any(), any(), any()))
                .thenReturn(mockHttp);

        driverManagerMock = mockStatic(DriverManager.class);
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(mock(Connection.class));

        Connection conn = createFactory().getConnection(mockCredentialsProvider);
        assertNotNull(conn);
    }
}
