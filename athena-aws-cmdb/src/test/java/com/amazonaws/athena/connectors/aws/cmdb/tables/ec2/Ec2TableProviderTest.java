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
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.EbsInstanceBlockDevice;
import com.amazonaws.services.ec2.model.GroupIdentifier;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceBlockDeviceMapping;
import com.amazonaws.services.ec2.model.InstanceNetworkInterface;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.StateReason;
import com.amazonaws.services.ec2.model.Tag;

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
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Ec2TableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(Ec2TableProviderTest.class);

    @Mock
    private AmazonEC2 mockEc2;

    protected String getIdField()
    {
        return "instance_id";
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
        return "ec2_instances";
    }

    protected int getExpectedRows()
    {
        return 4;
    }

    protected TableProvider setUpSource()
    {
        return new Ec2TableProvider(mockEc2);
    }

    @Override
    protected void setUpRead()
    {
        when(mockEc2.describeInstances(nullable(DescribeInstancesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            DescribeInstancesRequest request = (DescribeInstancesRequest) invocation.getArguments()[0];

            assertEquals(getIdValue(), request.getInstanceIds().get(0));
            DescribeInstancesResult mockResult = mock(DescribeInstancesResult.class);
            List<Reservation> reservations = new ArrayList<>();
            reservations.add(makeReservation());
            reservations.add(makeReservation());
            when(mockResult.getReservations()).thenReturn(reservations);
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
                if (field.getName().equals("$data$")) {
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

    private Reservation makeReservation()
    {
        Reservation reservation = mock(Reservation.class);
        List<Instance> instances = new ArrayList<>();
        instances.add(makeInstance(getIdValue()));
        instances.add(makeInstance(getIdValue()));
        instances.add(makeInstance("non-matching-id"));
        when(reservation.getInstances()).thenReturn(instances);
        return reservation;
    }

    private Instance makeInstance(String id)
    {
        Instance instance = new Instance();
        instance.withInstanceId(id)
                .withImageId("image_id")
                .withInstanceType("instance_type")
                .withPlatform("platform")
                .withPrivateDnsName("private_dns_name")
                .withPrivateIpAddress("private_ip_address")
                .withPublicDnsName("public_dns_name")
                .withPublicIpAddress("public_ip_address")
                .withSubnetId("subnet_id")
                .withVpcId("vpc_id")
                .withArchitecture("architecture")
                .withInstanceLifecycle("instance_lifecycle")
                .withRootDeviceName("root_device_name")
                .withRootDeviceType("root_device_type")
                .withSpotInstanceRequestId("spot_instance_request_id")
                .withVirtualizationType("virtualization_type")
                .withKeyName("key_name")
                .withKernelId("kernel_id")
                .withCapacityReservationId("capacity_reservation_id")
                .withLaunchTime(new Date(100_000))
                .withState(new InstanceState().withCode(100).withName("name"))
                .withStateReason(new StateReason().withCode("code").withMessage("message"))
                .withEbsOptimized(true)
                .withTags(new Tag("key","value"));

        List<InstanceNetworkInterface> interfaces = new ArrayList<>();
        interfaces.add(new InstanceNetworkInterface()
                .withStatus("status")
                .withSubnetId("subnet")
                .withVpcId("vpc")
                .withMacAddress("mac_address")
                .withPrivateDnsName("private_dns")
                .withPrivateIpAddress("private_ip")
                .withNetworkInterfaceId("interface_id")
                .withGroups(new GroupIdentifier().withGroupId("group_id").withGroupName("group_name")));

        interfaces.add(new InstanceNetworkInterface()
                .withStatus("status")
                .withSubnetId("subnet")
                .withVpcId("vpc")
                .withMacAddress("mac")
                .withPrivateDnsName("private_dns")
                .withPrivateIpAddress("private_ip")
                .withNetworkInterfaceId("interface_id")
                .withGroups(new GroupIdentifier().withGroupId("group_id").withGroupName("group_name")));

        instance.withNetworkInterfaces(interfaces)
                .withSecurityGroups(new GroupIdentifier().withGroupId("group_id").withGroupName("group_name"))
                .withBlockDeviceMappings(new InstanceBlockDeviceMapping().withDeviceName("device_name").withEbs(new EbsInstanceBlockDevice().withVolumeId("volume_id")));

        return instance;
    }
}
