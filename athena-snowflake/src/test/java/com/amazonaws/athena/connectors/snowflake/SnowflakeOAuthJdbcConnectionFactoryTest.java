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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

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
    private S3Client s3Client;
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
        mockCredential = mock(DefaultCredentials.class);
        when(mockCredentialsProvider.getCredential()).thenReturn(mockCredential);
        when(mockCredential.getUser()).thenReturn("testuser");

        s3Client = mock(S3Client.class);
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
        mockSecretsClient = mock(SecretsManagerClient.class);
        secretsMock = mockStatic(SecretsManagerClient.class);
        secretsMock.when(SecretsManagerClient::create).thenReturn(mockSecretsClient);

        GetSecretValueResponse secretResponse = GetSecretValueResponse.builder()
                .secretString(secretJson).build();
        when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(secretResponse);
    }

    private SnowflakeOAuthJdbcConnectionFactory createFactory()
    {
        return spy(new SnowflakeOAuthJdbcConnectionFactory(
                mockConfig,
                Map.of("key", "value"),
                mockInfo,
                Map.of("spill_bucket", "test-bucket"),
                s3Client
        ));
    }

    @Test
    public void testGetConnection() throws Exception
    {
        String secretJson = "{\"client_id\":\"cid\",\"client_secret\":\"cs\",\"token_url\":\"http://localhost/token\","
                + "\"redirect_uri\":\"http://localhost/callback\",\"auth_code\":\"auth\"}";

        // Mock HTTP
        httpMock = mockStatic(SnowflakeOAuthJdbcConnectionFactory.class);
        HttpURLConnection mockHttp = mock(HttpURLConnection.class);
        when(mockHttp.getOutputStream()).thenReturn(mock(OutputStream.class));
        when(mockHttp.getResponseCode()).thenReturn(200);
        when(mockHttp.getInputStream()).thenReturn(new ByteArrayInputStream(
                "{\"access_token\":\"abc123\",\"token_type\":\"Bearer\",\"expires_in\":3600}".getBytes()));
        httpMock.when(() -> SnowflakeOAuthJdbcConnectionFactory.getHttpURLConnection(any(), any(), any()))
                .thenReturn(mockHttp);

        // No token in S3
        when(s3Client.getObject(any(GetObjectRequest.class))).thenThrow(NoSuchKeyException.builder().build());

        mockSecretsManager(secretJson);
        driverManagerMock = mockStatic(DriverManager.class);
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(mock(Connection.class));

        Connection conn = createFactory().getConnection(mockCredentialsProvider);
        assertNotNull(conn);
    }

    @Test
    public void testGetConnectionTokenFromS3() throws Exception
    {
        JSONObject s3TokenJson = new JSONObject();
        s3TokenJson.put("access_token", "token_from_s3");
        s3TokenJson.put("expires_in", 3600);
        s3TokenJson.put("refresh_token", "refresh_token_s3");
        s3TokenJson.put("fetched_at", System.currentTimeMillis() / 1000 - 1000);

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStream.create(new ByteArrayInputStream(s3TokenJson.toString().getBytes()))
        ));

        mockSecretsManager("{\"client_id\":\"cid\",\"client_secret\":\"cs\",\"token_url\":\"http://localhost/token\","
                + "\"redirect_uri\":\"http://localhost/callback\",\"auth_code\":\"auth\"}");

        driverManagerMock = mockStatic(DriverManager.class);
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(mock(Connection.class));

        Connection conn = createFactory().getConnection(mockCredentialsProvider);
        assertNotNull(conn);
    }

    @Test
    public void testGetConnectionExpiredTokenFromS3() throws Exception
    {
        JSONObject expiredToken = new JSONObject();
        expiredToken.put("access_token", "expired_token");
        expiredToken.put("expires_in", 3600);
        expiredToken.put("refresh_token", "refresh_token_expired");
        expiredToken.put("fetched_at", System.currentTimeMillis() / 1000 - 4000);

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStream.create(new ByteArrayInputStream(expiredToken.toString().getBytes()))
        ));

        mockSecretsManager("{\"client_id\":\"cid\",\"client_secret\":\"cs\",\"token_url\":\"http://localhost/token\","
                + "\"redirect_uri\":\"http://localhost/callback\",\"auth_code\":\"auth\"}");

        HttpURLConnection mockHttp = mock(HttpURLConnection.class);
        when(mockHttp.getOutputStream()).thenReturn(mock(OutputStream.class));
        when(mockHttp.getResponseCode()).thenReturn(200);
        when(mockHttp.getInputStream()).thenReturn(new ByteArrayInputStream(
                "{\"access_token\":\"new_token\",\"token_type\":\"Bearer\",\"expires_in\":3600}".getBytes()));

        httpMock = mockStatic(SnowflakeOAuthJdbcConnectionFactory.class);
        httpMock.when(() -> SnowflakeOAuthJdbcConnectionFactory.getHttpURLConnection(any(), any(), any()))
                .thenReturn(mockHttp);

        driverManagerMock = mockStatic(DriverManager.class);
        when(DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(mock(Connection.class));

        Connection conn = createFactory().getConnection(mockCredentialsProvider);
        assertNotNull(conn);
    }
}
