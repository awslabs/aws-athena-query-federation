/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.integ;

import com.amazonaws.athena.connector.integ.data.ConnectorPackagingAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorStackAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.providers.ConnectorPackagingAttributesProvider;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.util.Map;
import java.util.Optional;

/**
 * Responsible for providing the Connector's stack attributes used in creating the Connector's stack.
 */
public class ConnectorStackAttributesProvider
{
    private static final String LAMBDA_SPILL_BUCKET_TAG = "spill_bucket";
    private static final String LAMBDA_SPILL_PREFIX_TAG = "spill_prefix";
    private static final String LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG = "disable_spill_encryption";

    private final Construct scope;
    private final String id;
    private final String lambdaFunctionName;
    private final Map<String, Object> testConfig;
    private final Optional<PolicyDocument> connectorAccessPolicy;
    private final Map<String, String> environmentVariables;
    private final ConnectorPackagingAttributes connectorPackagingAttributes;
    private final Optional<ConnectorVpcAttributes> connectorVpcAttributes;

    protected ConnectorStackAttributesProvider(final Construct scope, final String id, final String lambdaFunctionName,
                                               final Map<String, Object> testConfig,
                                               final Optional<PolicyDocument> connectorAccessPolicy,
                                               final Map<String, String> environmentVariables,
                                               Optional<ConnectorVpcAttributes> vpcAttributes)
    {
        this.scope = scope;
        this.id = id;
        this.lambdaFunctionName = lambdaFunctionName;
        this.testConfig = testConfig;
        this.connectorAccessPolicy = connectorAccessPolicy;
        this.environmentVariables = environmentVariables;
        this.connectorPackagingAttributes = ConnectorPackagingAttributesProvider.getAttributes();
        this.connectorVpcAttributes = vpcAttributes;

        setUpEnvironmentVars();
    }

    /**
     * Sets defaults for environment variables (spill_bucket, spill_prefix, disable_spill_encryption) if not provided
     * by connector.
     * @return A Map containing the environment variables key-value pairs.
     * @throws RuntimeException The spill_bucket is neither specified in the environment vars nor the test config file.
     */
    private void setUpEnvironmentVars()
            throws RuntimeException
    {
        // Check for missing spill_bucket
        if (!environmentVariables.containsKey(LAMBDA_SPILL_BUCKET_TAG)) {
            // Add missing spill_bucket environment variable from test config file
            Object spillBucket = testConfig.get(LAMBDA_SPILL_BUCKET_TAG);
            if (!(spillBucket instanceof String) || ((String) spillBucket).isEmpty()) {
                throw new RuntimeException(
                        "spill_bucket must be specified in environment var or test-config.json.");
            }
            environmentVariables.put(LAMBDA_SPILL_BUCKET_TAG, (String) spillBucket);
        }

        // Check for missing spill_prefix
        if (!environmentVariables.containsKey(LAMBDA_SPILL_PREFIX_TAG)) {
            // Add missing spill_prefix environment variable
            environmentVariables.put(LAMBDA_SPILL_PREFIX_TAG, "athena-spill");
        }

        // Check for missing disable_spill_encryption environment variable
        if (!environmentVariables.containsKey(LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG)) {
            // Add missing disable_spill_encryption environment variable
            environmentVariables.put(LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG, "false");
        }
    }

    /**
     * Provides the Connector's attributes needed to create the connector's CloudFormation stack template.
     */
    protected ConnectorStackAttributes getAttributes()
    {
        return new ConnectorStackAttributes(scope, id, lambdaFunctionName, connectorAccessPolicy,
                environmentVariables, connectorPackagingAttributes, connectorVpcAttributes);
    }
}
