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
import com.amazonaws.athena.connector.integ.data.TestConfig;
import com.amazonaws.athena.connector.integ.providers.ConnectorEnvironmentVarsProvider;
import com.amazonaws.athena.connector.integ.providers.ConnectorPackagingAttributesProvider;
import com.amazonaws.athena.connector.integ.providers.ConnectorVpcAttributesProvider;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.util.Map;
import java.util.Optional;

/**
 * Responsible for providing the Connector's stack attributes used in creating the Connector's stack (Lambda function,
 * Athena catalog, etc...)
 */
public class ConnectorStackAttributesProvider
{
    private final Construct scope;
    private final String id;
    private final String lambdaFunctionName;
    private final TestConfig testConfig;
    private final Optional<PolicyDocument> connectorAccessPolicy;
    private final Map<String, String> environmentVariables;
    private final ConnectorPackagingAttributes connectorPackagingAttributes;
    private final Optional<ConnectorVpcAttributes> connectorVpcAttributes;

    protected ConnectorStackAttributesProvider(final Construct scope, final String id, final String lambdaFunctionName,
                                               final TestConfig testConfig,
                                               final Optional<PolicyDocument> connectorAccessPolicy,
                                               final Map<String, String> environmentVariables)
    {
        this.scope = scope;
        this.id = id;
        this.lambdaFunctionName = lambdaFunctionName;
        this.testConfig = testConfig;
        this.connectorAccessPolicy = connectorAccessPolicy;
        this.connectorPackagingAttributes = ConnectorPackagingAttributesProvider.getAttributes();
        this.connectorVpcAttributes = ConnectorVpcAttributesProvider.getAttributes(testConfig);
        this.environmentVariables = ConnectorEnvironmentVarsProvider.getVars(testConfig);
        this.environmentVariables.putAll(environmentVariables);
    }

    /**
     * Provides the Connector's attributes needed to create the connector's CloudFormation stack template.
     * @return Connector attributes object.
     */
    protected ConnectorStackAttributes getAttributes()
    {
        return new ConnectorStackAttributes(scope, id, lambdaFunctionName, connectorAccessPolicy,
                environmentVariables, connectorPackagingAttributes, connectorVpcAttributes);
    }
}
