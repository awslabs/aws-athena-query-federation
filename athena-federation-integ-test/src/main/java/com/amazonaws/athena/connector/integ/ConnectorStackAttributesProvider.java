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
import com.amazonaws.athena.connector.integ.data.ConnectorVpcSubnetAttributes;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.DescribeVpcsResult;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.ec2.model.Vpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConnectorStackAttributesProvider
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectorStackAttributesProvider.class);

    private static final String CF_TEMPLATE_NAME = "packaged.yaml";
    private static final String LAMBDA_CODE_URI_TAG = "CodeUri:";
    private static final String LAMBDA_SPILL_BUCKET_PREFIX = "s3://";
    private static final String LAMBDA_HANDLER_TAG = "Handler:";
    private static final String LAMBDA_HANDLER_PREFIX = "Handler: ";
    private static final String LAMBDA_SPILL_BUCKET_TAG = "spill_bucket";
    private static final String LAMBDA_SPILL_PREFIX_TAG = "spill_prefix";
    private static final String LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG = "disable_spill_encryption";

    private final AmazonEC2 ec2Client;
    private final Construct scope;
    private final String id;
    private final String lambdaFunctionName;
    private final Optional<PolicyDocument> connectorAccessPolicy;
    private final Map<String, String> environmentVariables;
    private final ConnectorPackagingAttributes connectorPackagingAttributes;
    private final Optional<ConnectorVpcAttributes> connectorVpcAttributes;

    protected ConnectorStackAttributesProvider(final Construct scope, final String id, final String lambdaFunctionName,
                                               final Optional<PolicyDocument> connectorAccessPolicy,
                                               final Map<String, String> environmentVariables, boolean isSupportedVpcConfig)
    {
        this.ec2Client = AmazonEC2ClientBuilder.defaultClient();
        this.scope = scope;
        this.id = id;
        this.lambdaFunctionName = lambdaFunctionName;
        this.connectorAccessPolicy = connectorAccessPolicy;
        this.environmentVariables = environmentVariables;
        this.connectorPackagingAttributes = getPackagingAttributes();
        this.connectorVpcAttributes = getVpcAttributes(isSupportedVpcConfig);

        setUpEnvironmentVars();
    }

    /**
     * Extracts the packaging attributes needed in the creation of the CF Stack from packaged.yaml (S3 Bucket,
     * S3 Key, and lambdaFunctionHandler).
     * @return Connector's packaging attributes (S3 Bucket, S3 Key, and Lambda function handler).
     * @throws RuntimeException CloudFormation template (packaged.yaml) was not found.
     */
    private ConnectorPackagingAttributes getPackagingAttributes()
            throws RuntimeException
    {
        String s3Bucket = "";
        String s3Key = "";
        String lambdaFunctionHandler = "";

        try {
            for (String line : Files.readAllLines(Paths.get(CF_TEMPLATE_NAME), StandardCharsets.UTF_8)) {
                if (line.contains(LAMBDA_CODE_URI_TAG)) {
                    s3Bucket = line.substring(line.indexOf(LAMBDA_SPILL_BUCKET_PREFIX) +
                            LAMBDA_SPILL_BUCKET_PREFIX.length(), line.lastIndexOf('/'));
                    s3Key = line.substring(line.lastIndexOf('/') + 1);
                }
                else if (line.contains(LAMBDA_HANDLER_TAG)) {
                    lambdaFunctionHandler = line.substring(line.indexOf(LAMBDA_HANDLER_PREFIX) +
                            LAMBDA_HANDLER_PREFIX.length());
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Lambda connector has not been packaged via `sam package` (see README).", e);
        }

        logger.info("S3 Bucket: [{}], S3 Key: [{}], Handler: [{}]", s3Bucket, s3Key, lambdaFunctionHandler);
        return new ConnectorPackagingAttributes(s3Bucket, s3Key, lambdaFunctionHandler);
    }

    /**
     * Gets the default VPC configuration used for configuring the Lambda function.
     * @param isSupportedVpcConfig Indicates whether a VPC configuration is supported for this connector.
     * @return VPC attributes (VPC Id, Security group Id, Subnet Ids, and Availability zones) if a VPC configuration is
     * supported for this connector.
     */
    private Optional<ConnectorVpcAttributes> getVpcAttributes(boolean isSupportedVpcConfig)
    {
        if (isSupportedVpcConfig) {
            final String vpcId = getVpcId();
            final String securityGroupId = getSecurityGroupId(vpcId);
            final ConnectorVpcSubnetAttributes subnetAttributes = getSubnetAttributes(vpcId);

            logger.info("VPC Id: [{}], SG: [{}], {}", vpcId, securityGroupId, subnetAttributes);

            return Optional.of(new ConnectorVpcAttributes(vpcId, securityGroupId, subnetAttributes));
        }

        return Optional.empty();
    }

    /**
     * Gets the default VPC Id used for configuring the Lambda function (e.g. vpc-xxxx).
     * @return VPC Id
     * @throws RuntimeException No VPCs were found.
     */
    private String getVpcId()
            throws RuntimeException
    {
        DescribeVpcsResult vpcsResult = ec2Client.describeVpcs();
        for (Vpc vpc : vpcsResult.getVpcs()) {
            if (vpc.getIsDefault()) {
                return vpc.getVpcId();
            }
        }

        throw new RuntimeException("VPC Id is required for VPC configuration, but none were found.");
    }

    /**
     * Gets the Security Group Id for the default VPC Id used for configuring the Lambda function (e.g. sg-xxxx).
     * @return Security Group Id
     * @throws RuntimeException No Security Group Ids were found.
     */
    private String getSecurityGroupId(final String vpcId)
            throws RuntimeException
    {
        DescribeSecurityGroupsResult sgResult = ec2Client.describeSecurityGroups();
        for (SecurityGroup securityGroup : sgResult.getSecurityGroups()) {
            if (securityGroup.getVpcId().equals(vpcId)) {
                return securityGroup.getGroupId();
            }
        }

        throw new RuntimeException("Security Group Id is required for VPC configuration, but none were found.");
    }

    /**
     * Gets the VPC Subnet Attributes:
     * 1) Subnet Ids (e.g. subnet-xxxx),
     * 2) Availability Zones (e.g. us-east-1a).
     * @return Subnet Attributes
     */
    private ConnectorVpcSubnetAttributes getSubnetAttributes(String vpcId)
    {
        DescribeSubnetsResult subnetsResult = ec2Client.describeSubnets();
        List<String> privateSubnetIds = new ArrayList<>();
        List<String> availabilityZones = new ArrayList<>();
        for (Subnet subnet : subnetsResult.getSubnets()) {
            if (subnet.getVpcId().equals(vpcId)) {
                privateSubnetIds.add(subnet.getSubnetId());
                availabilityZones.add(subnet.getAvailabilityZone());
            }
        }

        return new ConnectorVpcSubnetAttributes(privateSubnetIds, availabilityZones);
    }

    /**
     * Sets defaults for environment variables (spill_bucket, spill_prefix, disable_spill_encryption) if not provided
     * by connector.
     * @return A Map containing the environment variables key-value pairs.
     */
    private void setUpEnvironmentVars()
    {
        // Check for missing spill_bucket
        if (!environmentVariables.containsKey(LAMBDA_SPILL_BUCKET_TAG)) {
            // Add missing spill_bucket environment variable
            environmentVariables.put(LAMBDA_SPILL_BUCKET_TAG, connectorPackagingAttributes.getS3Bucket());
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
