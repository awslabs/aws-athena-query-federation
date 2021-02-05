/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connector.integ.providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for providing the Connector's Environment vars added to the Connector's stack attributes and used
 * in the creation of the Lambda.
 */
public class ConnectorEnvironmentVarsProvider
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectorEnvironmentVarsProvider.class);

    private static final String TEST_CONFIG_ENVIRONMENT_VARS = "environment_vars";

    private ConnectorEnvironmentVarsProvider() {}

    /**
     * Gets the environment variables used for configuring the Lambda function (e.g. spill_bucket, etc...)
     * @param testConfig A Map containing the test configuration attributes extracted from a config file.
     * @return Map containing the Lambda's environment variables and their associated values if the vars
     * are included in test-config.json.
     */
    public static Map<String, String> getVars(Map<String, Object> testConfig)
    {
        Map<String, String> environmentVars = new HashMap<>();

        // Get VPC configuration.
        Object envVars = testConfig.get(TEST_CONFIG_ENVIRONMENT_VARS);
        if (envVars instanceof Map) {
            ((Map) envVars).forEach((key, value) -> {
                if ((key instanceof String) && (value instanceof String)) {
                    environmentVars.put((String) key, (String) value);
                }
            });
        }

        logger.info("Environment vars: {}", environmentVars);

        return environmentVars;
    }
}
