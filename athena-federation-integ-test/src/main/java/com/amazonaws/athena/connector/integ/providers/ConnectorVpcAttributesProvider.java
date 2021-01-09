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

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for providing the Connector's VPC attributes used in creating the Connector's stack attributes.
 */
public class ConnectorVpcAttributesProvider implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectorVpcAttributesProvider.class);

    private final AmazonEC2 ec2Client;

    public ConnectorVpcAttributesProvider()
    {
        this.ec2Client = AmazonEC2ClientBuilder.defaultClient();
    }

    /**
     * Gets the default VPC configuration used for configuring the Lambda function.
     * @return VPC attributes (VPC Id, Security group Id, Subnet Ids, and Availability zones) if a VPC configuration is
     * supported for this connector.
     * @throws RuntimeException Errors were encountered obtaining the VPC configuration.
     */
    public ConnectorVpcAttributes getAttributes()
            throws RuntimeException
    {
        final String vpcId = getDefaultVpcId();
        final String securityGroupId = getSecurityGroupId(vpcId);
        final ConnectorVpcSubnetAttributes subnetAttributes = getSubnetAttributes(vpcId);

        logger.info("VPC Id: [{}], SG: [{}], {}", vpcId, securityGroupId, subnetAttributes);

        return new ConnectorVpcAttributes(vpcId, securityGroupId, subnetAttributes);
    }

    /**
     * Gets the default VPC Id used for configuring the Lambda function (e.g. vpc-xxxx).
     * @return VPC Id
     * @throws RuntimeException No VPCs were found.
     */
    private String getDefaultVpcId()
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
    private ConnectorVpcSubnetAttributes getSubnetAttributes(final String vpcId)
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

    @Override
    public void close()
            throws Exception
    {
        ec2Client.shutdown();
    }
}
