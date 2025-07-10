/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.connection;

import org.junit.Test;
import software.amazon.awssdk.services.glue.model.AuthenticationConfiguration;
import software.amazon.awssdk.services.glue.model.Connection;
import software.amazon.awssdk.services.glue.model.ConnectionPropertyKey;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.KMS_KEY_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SPILL_KMS_KEY_ID;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


public class EnvironmentPropertiesTest {

    private static final String glueConnName = "my-glue-conn";
    private static final String secretArn = "arn:aws:secretsmanager:us-east-1:1234567890:secret:my-secret-abc123";
    private static final String expectedSecretName = "my-secret";
    private static final String testValue = "test";
    private static final String kmsKeyId = "kms-123";
    private static final String lambdaValue = "lambda-value";


    @Test
    public void testCreateEnvironmentWithSystemLambda() throws Exception {
        withEnvironmentVariable(DEFAULT_GLUE_CONNECTION, glueConnName)
                .and("OVERRIDE_VAR", lambdaValue)
                .execute(() -> {
                    EnvironmentProperties spyProps = spy(new EnvironmentProperties());

                    AuthenticationConfiguration authConfig = AuthenticationConfiguration.builder()
                            .secretArn(secretArn)
                            .build();

                    Map<ConnectionPropertyKey, String> connectionProps = new HashMap<>();
                    connectionProps.put(ConnectionPropertyKey.DATABASE, testValue);

                    Map<String, String> athenaProps = new HashMap<>();
                    athenaProps.put(SPILL_KMS_KEY_ID, kmsKeyId);

                    Connection glueConnection = Connection.builder()
                            .name(glueConnName)
                            .connectionProperties(connectionProps)
                            .authenticationConfiguration(authConfig)
                            .athenaProperties(athenaProps)
                            .build();

                    doReturn(glueConnection).when(spyProps).getGlueConnection(glueConnName);

                    Map<String, String> result = spyProps.createEnvironment();

                    assertEquals(glueConnName, result.get(DEFAULT_GLUE_CONNECTION));
                    assertEquals(testValue, result.get(DATABASE));
                    assertEquals(expectedSecretName, result.get(SECRET_NAME));
                    assertEquals(kmsKeyId, result.get(KMS_KEY_ID));
                    assertEquals(lambdaValue, result.get("OVERRIDE_VAR"));
                });
    }

    @Test
    public void testCreateEnvironmentWithSystemLambda_GlueConnectionFails_ThrowsRuntimeException() throws Exception
    {
        withEnvironmentVariable(DEFAULT_GLUE_CONNECTION, glueConnName)
                .execute(() -> {
                    EnvironmentProperties spyProps = spy(new EnvironmentProperties());

                    doThrow(new RuntimeException("Simulated failure"))
                            .when(spyProps).getGlueConnection(glueConnName);

                    RuntimeException thrown = assertThrows(RuntimeException.class, spyProps::createEnvironment);

                    assertEquals("Simulated failure", thrown.getMessage());
                });
    }
}
