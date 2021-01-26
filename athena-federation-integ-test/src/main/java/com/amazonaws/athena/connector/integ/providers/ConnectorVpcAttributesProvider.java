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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Responsible for providing the Connector's VPC attributes used in creating the Connector's stack attributes.
 */
public class ConnectorVpcAttributesProvider
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectorVpcAttributesProvider.class);

    private static final String TEST_CONFIG_VPC_ID = "vpc_id";
    private static final String TEST_CONFIG_SECURITY_GROUP_ID = "security_group_id";
    private static final String TEST_CONFIG_SUBNET_IDS = "subnet_ids";
    private static final String TEST_CONFIG_AVAILABILITY_ZONES = "availability_zones";

    private ConnectorVpcAttributesProvider() {}

    /**
     * Gets the VPC attributes used for configuring the Lambda function.
     * @param testConfig A Map containing the test configuration attributes extracted from a config file.
     * @return VPC attributes (VPC Id, Security group Id, Subnet Ids, and Availability zones) if a VPC configuration is
     * supported for this connector.
     * @throws RuntimeException Errors were encountered obtaining the VPC configuration.
     */
    public static Optional<ConnectorVpcAttributes> getAttributes(Map<String, Object> testConfig)
    {
        // Get VPC Id.
        Object vpcId = testConfig.get(TEST_CONFIG_VPC_ID);
        if (!(vpcId instanceof String) || ((String) vpcId).isEmpty()) {
            logger.info("VPC Id is not set in test-config.json");
            return Optional.empty();
        }

        // Get Security Group Id.
        Object securityGroupId = testConfig.get(TEST_CONFIG_SECURITY_GROUP_ID);
        if (!(securityGroupId instanceof String) || ((String) securityGroupId).isEmpty()) {
            logger.info("Security Group Id is not set in test-config.json");
            return Optional.empty();
        }

        // Get Subnet Ids.
        Object subnetIds = testConfig.get(TEST_CONFIG_SUBNET_IDS);
        if (!(subnetIds instanceof List) || ((List) subnetIds).isEmpty()) {
            logger.info("Subnet Ids are not set in test-config.json");
            return Optional.empty();
        }

        // Get Availability Zones.
        Object availabilityZones = testConfig.get(TEST_CONFIG_AVAILABILITY_ZONES);
        if (!(availabilityZones instanceof List) || ((List) availabilityZones).isEmpty()) {
            logger.info("Availability Zones are not set in test-config.json");
            return Optional.empty();
        }

        ConnectorVpcSubnetAttributes subnetAttributes = new ConnectorVpcSubnetAttributes(
                (List) subnetIds, (List) availabilityZones);

        logger.info("VPC Id: [{}], SG: [{}], {}", vpcId, securityGroupId, subnetAttributes);

        return Optional.of(new ConnectorVpcAttributes((String) vpcId, (String) securityGroupId, subnetAttributes));
    }
}
