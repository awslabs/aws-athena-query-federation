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
package com.amazonaws.athena.connector.integ.stacks;

import com.amazonaws.athena.connector.integ.data.ConnectorStackAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.google.common.collect.ImmutableList;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcAttributes;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Function;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Sets up the CloudFormation stack necessary for a Lambda Connector with a VPC configuration.
 */
public class ConnectorWithVpcStack extends ConnectorStack
{
    private final ConnectorVpcAttributes connectorVpcAttributes;
    private final String vpcId;
    private final String securityGroupId;
    private final List<String> subnetIds;
    private final List<String> availabilityZones;

    public ConnectorWithVpcStack(Builder builder)
    {
        super(builder);

        this.connectorVpcAttributes = builder.connectorVpcAttributes;
        this.vpcId = connectorVpcAttributes.getVpcId();
        this.securityGroupId = connectorVpcAttributes.getSecurityGroupId();
        this.subnetIds = connectorVpcAttributes.getPrivateSubnetIds();
        this.availabilityZones = connectorVpcAttributes.getAvailabilityZones();
    }

    /**
     * Public accessor for the Connector's VPC attributes:
     * 1) VPC Id (e.g. vpc-xxxx),
     * 2) Security Group Id (e.g. sg-xxxx),
     * 3) Subnet Ids (e.g. subnet-xxxx),
     * 4) Subnet availability zones (e.g. us-east-1a)
     * @return VPC attributes
     */
    public ConnectorVpcAttributes getConnectorVpcAttributes()
    {
        return connectorVpcAttributes;
    }

    /**
     * Builds the Lambda function stack resource injecting the VPC configuration.
     * @return Lambda function Builder.
     */
    @Override
    protected Function.Builder lambdaFunctionBuilder()
    {
        return super.lambdaFunctionBuilder()
                .vpc(Vpc.fromVpcAttributes(this, "VpcConfig", createVpcAttributes()))
                .securityGroups(Collections.singletonList(SecurityGroup
                        .fromSecurityGroupId(this, "VpcSecurityGroup", securityGroupId)));
    }

    /**
     * Creates the VPC Attributes used in the VPC configuration.
     * @return VPC attributes object.
     */
    private VpcAttributes createVpcAttributes()
    {
        return vpcAttributesBuilder().build();
    }

    /**
     * Builds the VPC Attributes.
     * @return VPC attributes Builder.
     */
    protected VpcAttributes.Builder vpcAttributesBuilder()
    {
        return VpcAttributes.builder()
                .vpcId(vpcId)
                .privateSubnetIds(subnetIds)
                .availabilityZones(availabilityZones);
    }

    /**
     * Sets the access policies used by the Lambda function.
     * @param policies A map of access policies.
     */
    @Override
    protected void setAccessPolicies(Map<String, PolicyDocument> policies)
    {
        super.setAccessPolicies(policies);
        policies.put("VpcEc2AccessPolicy", getVpcEc2AccessPolicy());
    }

    /**
     * Sets up the EC2 access policy used to create the VPC configuration.
     * @return A policy document object.
     */
    private PolicyDocument getVpcEc2AccessPolicy()
    {
        List<String> statementActionsPolicy = ImmutableList.of(
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface");

        return PolicyDocument.Builder.create()
                .statements(Collections.singletonList(PolicyStatement.Builder.create()
                        .actions(statementActionsPolicy)
                        .resources(Collections.singletonList("*"))
                        .effect(Effect.ALLOW)
                        .build()))
                .build();
    }

    public static Stack buildWithAttributes(ConnectorStackAttributes attributes)
    {
        return builder().withAttributes(attributes).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder extends ConnectorStack.Builder
    {
        private ConnectorVpcAttributes connectorVpcAttributes;

        @Override
        public Builder withAttributes(ConnectorStackAttributes attributes)
        {
            super.withAttributes(attributes);

            this.connectorVpcAttributes = attributes.getConnectorVpcAttributes()
                    .orElseThrow(() -> new RuntimeException("VPC configuration must be provided."));

            return this;
        }

        @Override
        public Stack build()
        {
            ConnectorWithVpcStack stack = new ConnectorWithVpcStack(this);
            stack.initialize();
            return stack;
        }
    }
}
