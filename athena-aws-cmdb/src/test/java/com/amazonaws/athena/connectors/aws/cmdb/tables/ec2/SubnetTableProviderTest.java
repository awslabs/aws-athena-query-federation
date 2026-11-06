/*-
 * #%L
 * athena-aws-cmdb
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
package com.amazonaws.athena.connectors.aws.cmdb.tables.ec2;

import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsResponse;
import software.amazon.awssdk.services.ec2.model.Subnet;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SubnetTableProviderTest
        extends AbstractTableProviderTest
{
    @Mock
    private Ec2Client mockEc2;

    protected String getIdField()
    {
        return "id";
    }

    protected String getIdValue()
    {
        return "123";
    }

    protected String getExpectedSchema()
    {
        return "ec2";
    }

    protected String getExpectedTable()
    {
        return "subnets";
    }

    protected int getExpectedRows()
    {
        return 2;
    }

    protected TableProvider setUpSource()
    {
        return new SubnetTableProvider(mockEc2);
    }

    @Override
    protected void setUpRead()
    {
        when(mockEc2.describeSubnets(nullable(DescribeSubnetsRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            DescribeSubnetsRequest request = (DescribeSubnetsRequest) invocation.getArguments()[0];

            assertEquals(getIdValue(), request.subnetIds().get(0));
            List<Subnet> values = new ArrayList<>();
            values.add(makeSubnet(getIdValue()));
            values.add(makeSubnet(getIdValue()));
            values.add(makeSubnet("fake-id"));
            return DescribeSubnetsResponse.builder().subnets(values).build();
        });
    }

    private Subnet makeSubnet(String id)
    {
        return Subnet.builder()
                .subnetId(id)
                .availabilityZone("availability_zone")
                .cidrBlock("cidr_block")
                .availableIpAddressCount(100)
                .defaultForAz(true)
                .mapPublicIpOnLaunch(true)
                .ownerId("owner")
                .state("state")
                .tags(Tag.builder().key("key").value("value").build())
                .vpcId("vpc").build();
    }
}
