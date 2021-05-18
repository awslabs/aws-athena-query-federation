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

import java.util.List;

/**
 * Contains the attributes for the connector's VPC Subnet configuration.
 */
public class ConnectorVpcSubnetAttributes
{
    private final List<String> privateSubnetIds;
    private final List<String> availabilityZones;

    public ConnectorVpcSubnetAttributes(List<String> privateSubnetIds, List<String> availabilityZones)
    {
        this.privateSubnetIds = privateSubnetIds;
        this.availabilityZones = availabilityZones;
    }

    /**
     * Public accessor for the Subnet Ids (e.g. subnet-xxxxx)
     * @return Subnet Ids
     */
    public List<String> getPrivateSubnetIds()
    {
        return privateSubnetIds;
    }

    /**
     * Public accessor for the VPC Subnets' availability zones (e.g. us-east-1a).
     * @return Subnets' availability zones
     */
    public List<String> getAvailabilityZones()
    {
        return availabilityZones;
    }

    @Override
    public String toString()
    {
        return String.format("Subnet Ids: %s, AZs: %s", privateSubnetIds, availabilityZones);
    }
}
