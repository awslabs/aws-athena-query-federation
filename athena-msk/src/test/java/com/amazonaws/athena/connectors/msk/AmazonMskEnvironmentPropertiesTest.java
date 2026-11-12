/*-
 * #%L
 * Athena MSK Connector
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
package com.amazonaws.athena.connectors.msk;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.CUSTOM_AUTH_TYPE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.GLUE_CERTIFICATES_S3_REFERENCE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.AUTH_TYPE;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.CERTIFICATES_S3_REFERENCE;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.ENV_KAFKA_ENDPOINT;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.SECRET_MANAGER_MSK_CREDS_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AmazonMskEnvironmentPropertiesTest
{
    private static final String LOCALHOST = "localhost";
    private static final String PORT_NUMBER = "9092";
    private static final String AUTH_TYPE_SASL_SSL_SCRAM_SHA512 = "SASL_SSL_SCRAM_SHA512";
    private static final String TEST_SECRET = "testSecret";
    private static final String S3_CERTIFICATES_REFERENCE = "s3://bucket/certificates/";
    private static final String AUTH_TYPE_SSL = "SSL";

    private Map<String, String> connectionProperties;
    private AmazonMskEnvironmentProperties mskProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, LOCALHOST);
        connectionProperties.put(PORT, PORT_NUMBER);
        connectionProperties.put(CUSTOM_AUTH_TYPE, AUTH_TYPE_SASL_SSL_SCRAM_SHA512);
        connectionProperties.put(SECRET_NAME, TEST_SECRET);
        connectionProperties.put(GLUE_CERTIFICATES_S3_REFERENCE, S3_CERTIFICATES_REFERENCE);
        mskProperties = new AmazonMskEnvironmentProperties();
    }

    @Test
    public void connectionPropertiesToEnvironment_withValidProperties_shouldReturnExpectedEnvironment()
    {
        Map<String, String> mskEnvironment = mskProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals(AUTH_TYPE_SASL_SSL_SCRAM_SHA512, mskEnvironment.get(AUTH_TYPE));
        assertEquals(S3_CERTIFICATES_REFERENCE, mskEnvironment.get(CERTIFICATES_S3_REFERENCE));
        assertEquals(TEST_SECRET, mskEnvironment.get(SECRET_MANAGER_MSK_CREDS_NAME));
        assertEquals(LOCALHOST + ":" + PORT_NUMBER, mskEnvironment.get(ENV_KAFKA_ENDPOINT));
    }
    
    @Test
    public void connectionPropertiesToEnvironment_withEmptyProperties_shouldUseDefaults()
    {
        Map<String, String> propertiesWithEmpty = new HashMap<>();
        propertiesWithEmpty.put(HOST, "kafka.example.com");
        propertiesWithEmpty.put(PORT, "9093");
        propertiesWithEmpty.put(CUSTOM_AUTH_TYPE, "SSL");
        propertiesWithEmpty.put(SECRET_NAME, "");
        propertiesWithEmpty.put(GLUE_CERTIFICATES_S3_REFERENCE, "");

        Map<String, String> mskEnvironment = mskProperties.connectionPropertiesToEnvironment(propertiesWithEmpty);

        assertEquals(AUTH_TYPE_SSL, mskEnvironment.get(AUTH_TYPE));
        assertEquals("", mskEnvironment.get(CERTIFICATES_S3_REFERENCE));
        assertEquals("", mskEnvironment.get(SECRET_MANAGER_MSK_CREDS_NAME));
        assertEquals("kafka.example.com:9093", mskEnvironment.get(ENV_KAFKA_ENDPOINT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withDifferentAuthTypes_shouldReturnCorrectAuthType()
    {
        String[] authTypes = {
            "NO_AUTH",
            "SSL",
            "SASL_SSL_PLAIN",
            "SASL_PLAINTEXT_PLAIN",
            "SASL_SSL_SCRAM_SHA512",
            "SASL_SSL_AWS_MSK_IAM"
        };

        for (String authType : authTypes) {
            Map<String, String> properties = new HashMap<>();
            properties.put(HOST, LOCALHOST);
            properties.put(PORT, PORT_NUMBER);
            properties.put(CUSTOM_AUTH_TYPE, authType);

            Map<String, String> mskEnvironment = mskProperties.connectionPropertiesToEnvironment(properties);
            assertEquals(authType, mskEnvironment.get(AUTH_TYPE));
        }
    }
    
    @Test
    public void connectionPropertiesToEnvironment_withNullAuthType_shouldReturnNull()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put(HOST, LOCALHOST);
        properties.put(PORT, PORT_NUMBER);

        Map<String, String> mskEnvironment = mskProperties.connectionPropertiesToEnvironment(properties);
        assertNull(mskEnvironment.get(AUTH_TYPE));
    }
}
