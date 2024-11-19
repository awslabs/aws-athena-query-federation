package com.amazonaws.athena.connector.lambda.connection;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import software.amazon.awssdk.services.glue.model.AuthenticationConfiguration;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.KMS_KEY_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SPILL_KMS_KEY_ID;
import static org.junit.Assert.*;

public class EnvironmentPropertiesTest
{
    private static final String EXAMPLE_SECRET_ARN = "arn:aws:secretsmanager:us-east-1:012345678912:secret:secretname-CMyiKm";
    private static final String EXAMPLE_KMS_KEY_ID = "1234abcd-12ab-34cd-56ef-1234567890ab";

    @Test
    public void authenticationConfigurationToMapTest()
    {
        AuthenticationConfiguration auth = AuthenticationConfiguration.builder().secretArn(EXAMPLE_SECRET_ARN).build();
        Map<String, String> authMap = new EnvironmentProperties().authenticationConfigurationToMap(auth);
        assertEquals("secretname", authMap.get(SECRET_NAME));
    }

    @Test
    public void athenaPropertiesToEnvironmentTest()
    {
        Map<String, String> athenaProperties = new HashMap<>();
        athenaProperties.put(SPILL_KMS_KEY_ID, EXAMPLE_KMS_KEY_ID);
        athenaProperties = new EnvironmentProperties().athenaPropertiesToEnvironment(athenaProperties);

        assertFalse(athenaProperties.containsKey(SPILL_KMS_KEY_ID));
        assertTrue(athenaProperties.containsKey(KMS_KEY_ID));
        assertEquals(EXAMPLE_KMS_KEY_ID, athenaProperties.get(KMS_KEY_ID));
    }
}
