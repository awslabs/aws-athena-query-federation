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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.Ipv6Range;
import com.amazonaws.services.ec2.model.PrefixListId;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.UserIdGroupPair;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SecurityGroupsTableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(SecurityGroupsTableProviderTest.class);

    @Mock
    private AmazonEC2 mockEc2;

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
        return "security_groups";
    }

    protected int getExpectedRows()
    {
        return 2;
    }

    protected TableProvider setUpSource()
    {
        return new SecurityGroupsTableProvider(mockEc2);
    }

    @Override
    protected void setUpRead()
    {
        when(mockEc2.describeSecurityGroups(nullable(DescribeSecurityGroupsRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    DescribeSecurityGroupsRequest request = (DescribeSecurityGroupsRequest) invocation.getArguments()[0];

                    assertEquals(getIdValue(), request.getGroupIds().get(0));
                    DescribeSecurityGroupsResult mockResult = mock(DescribeSecurityGroupsResult.class);
                    List<SecurityGroup> values = new ArrayList<>();
                    values.add(makeSecurityGroup(getIdValue()));
                    values.add(makeSecurityGroup(getIdValue()));
                    values.add(makeSecurityGroup("fake-id"));
                    when(mockResult.getSecurityGroups()).thenReturn(values);
                    return mockResult;
                });
    }

    protected void validateRow(Block block, int pos)
    {
        for (FieldReader fieldReader : block.getFieldReaders()) {
            fieldReader.setPosition(pos);
            Field field = fieldReader.getField();

            if (field.getName().equals(getIdField())) {
                assertEquals(getIdValue(), fieldReader.readText().toString());
            }
            else {
                validate(fieldReader);
            }
        }
    }

    private void validate(FieldReader fieldReader)
    {
        Field field = fieldReader.getField();
        Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
        switch (type) {
            case VARCHAR:
                if (field.getName().equals("$data$") || field.getName().equals("direction")) {
                    assertNotNull(fieldReader.readText().toString());
                }
                else {
                    assertEquals(field.getName(), fieldReader.readText().toString());
                }
                break;
            case DATEMILLI:
                assertEquals(100_000, fieldReader.readLocalDateTime().atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli());
                break;
            case BIT:
                assertTrue(fieldReader.readBoolean());
                break;
            case INT:
                assertTrue(fieldReader.readInteger() > 0);
                break;
            case STRUCT:
                for (Field child : field.getChildren()) {
                    validate(fieldReader.reader(child.getName()));
                }
                break;
            case LIST:
                validate(fieldReader.reader());
                break;
            default:
                throw new RuntimeException("No validation configured for field " + field.getName() + ":" + type + " " + field.getChildren());
        }
    }

    private SecurityGroup makeSecurityGroup(String id)
    {
        return new SecurityGroup()
                .withGroupId(id)
                .withGroupName("name")
                .withDescription("description")
                .withIpPermissions(new IpPermission()
                        .withIpProtocol("protocol")
                        .withFromPort(100)
                        .withToPort(100)
                        .withIpv4Ranges(new IpRange().withCidrIp("cidr").withDescription("description"))

                        .withIpv6Ranges(new Ipv6Range().withCidrIpv6("cidr").withDescription("description"))
                        .withPrefixListIds(new PrefixListId().withPrefixListId("prefix").withDescription("description"))
                        .withUserIdGroupPairs(new UserIdGroupPair().withGroupId("group_id").withUserId("user_id"))
                );
    }
}
