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
package com.amazonaws.athena.connector.integ.data;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.util.Map;
import java.util.Optional;

/**
 * Contains the attributes needed to create the connector's CloudFormation stack template.
 */
public class ConnectorStackAttributes
{
    private final Construct scope;
    private final String id;
    private final String lambdaFunctionName;
    private final Optional<PolicyDocument> connectorAccessPolicy;
    private final Map<String, String> environmentVariables;
    private final ConnectorPackagingAttributes connectorPackagingAttributes;
    private final Optional<ConnectorVpcAttributes> connectorVpcAttributes;

    public ConnectorStackAttributes(final Construct scope, final String id, final String lambdaFunctionName,
                                    final Optional<PolicyDocument> connectorAccessPolicy,
                                    final Map<String, String> environmentVariables,
                                    final ConnectorPackagingAttributes connectorPackagingAttributes,
                                    final Optional<ConnectorVpcAttributes> connectorVpcAttributes)
    {
        this.scope = scope;
        this.id = id;
        this.lambdaFunctionName = lambdaFunctionName;
        this.connectorAccessPolicy = connectorAccessPolicy;
        this.environmentVariables = environmentVariables;
        this.connectorPackagingAttributes = connectorPackagingAttributes;
        this.connectorVpcAttributes = connectorVpcAttributes;
    }

    /**
     * Public accessor for the Stack's context scope.
     * @return Stack's context scope
     */
    public Construct getScope()
    {
        return scope;
    }

    /**
     * Public accessor for the Stack's Id/name.
     * @return Stack's Id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Public accessor for the Lambda function's name.
     * @return Lambda function's name
     */
    public String getLambdaFunctionName()
    {
        return lambdaFunctionName;
    }

    /**
     * Public accessor for the Connector-specific access policy.
     * @return Connector's access policy
     */
    public Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        return connectorAccessPolicy;
    }

    /**
     * Public accessor for the Connector's environment variables.
     * @return Connector's environment variables.
     */
    public Map<String, String> getEnvironmentVariables()
    {
        return environmentVariables;
    }

    /**
     * Public accessor for the Connector's packaging attributes:
     * 1) S3 Bucket,
     * 2) S3 Key,
     * 3) Connector's Handler.
     * @return Packaging attributes
     */
    public ConnectorPackagingAttributes getConnectorPackagingAttributes()
    {
        return connectorPackagingAttributes;
    }

    /**
     * Public accessor for the Connector's VPC attributes:
     * 1) VPC Id (e.g. vpc-xxxx),
     * 2) Security Group Id (e.g. sg-xxxx),
     * 3) Subnet Ids (e.g. subnet-xxxx),
     * 4) Subnet availability zones (e.g. us-east-1a)
     * @return VPC attributes
     */
    public Optional<ConnectorVpcAttributes> getConnectorVpcAttributes()
    {
        return connectorVpcAttributes;
    }
}
