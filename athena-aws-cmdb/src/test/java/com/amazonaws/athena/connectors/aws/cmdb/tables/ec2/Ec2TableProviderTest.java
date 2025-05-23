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
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.EbsInstanceBlockDevice;
import software.amazon.awssdk.services.ec2.model.GroupIdentifier;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceBlockDeviceMapping;
import software.amazon.awssdk.services.ec2.model.InstanceNetworkInterface;
import software.amazon.awssdk.services.ec2.model.InstanceState;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.StateReason;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Ec2TableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(Ec2TableProviderTest.class);

    @Mock
    private Ec2Client mockEc2;

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

            assertEquals(getIdValue(), request.instanceIds().get(0));
            List<Reservation> reservations = new ArrayList<>();
            reservations.add(makeReservation());
            reservations.add(makeReservation());
            return DescribeInstancesResponse.builder().reservations(reservations).build();
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
        List<Instance> instances = new ArrayList<>();
        instances.add(makeInstance(getIdValue()));
        instances.add(makeInstance(getIdValue()));
        instances.add(makeInstance("non-matching-id"));
        return Reservation.builder().instances(instances).build();
    }

    private Instance makeInstance(String id)
    {
        Instance.Builder instance = Instance.builder()
                .instanceId(id)
                .imageId("image_id")
                .instanceType("instance_type")
                .platform("platform")
                .privateDnsName("private_dns_name")
                .privateIpAddress("private_ip_address")
                .publicDnsName("public_dns_name")
                .publicIpAddress("public_ip_address")
                .subnetId("subnet_id")
                .vpcId("vpc_id")
                .architecture("architecture")
                .instanceLifecycle("instance_lifecycle")
                .rootDeviceName("root_device_name")
                .rootDeviceType("root_device_type")
                .spotInstanceRequestId("spot_instance_request_id")
                .virtualizationType("virtualization_type")
                .keyName("key_name")
                .kernelId("kernel_id")
                .capacityReservationId("capacity_reservation_id")
                .launchTime(new Date(100_000).toInstant())
                .state(InstanceState.builder().code(100).name("name").build())
                .stateReason(StateReason.builder().code("code").message("message").build())
                .ebsOptimized(true)
                .tags(Tag.builder().key("key").value("value").build());

        List<InstanceNetworkInterface> interfaces = new ArrayList<>();
        interfaces.add(InstanceNetworkInterface.builder()
                .status("status")
                .subnetId("subnet")
                .vpcId("vpc")
                .macAddress("mac_address")
                .privateDnsName("private_dns")
                .privateIpAddress("private_ip")
                .networkInterfaceId("interface_id")
                .groups(GroupIdentifier.builder().groupId("group_id").groupName("group_name").build()).build());

        interfaces.add(InstanceNetworkInterface.builder()
                .status("status")
                .subnetId("subnet")
                .vpcId("vpc")
                .macAddress("mac")
                .privateDnsName("private_dns")
                .privateIpAddress("private_ip")
                .networkInterfaceId("interface_id")
                .groups(GroupIdentifier.builder().groupId("group_id").groupName("group_name").build()).build());

        instance.networkInterfaces(interfaces)
                .securityGroups(GroupIdentifier.builder().groupId("group_id").groupName("group_name").build())
                .blockDeviceMappings(InstanceBlockDeviceMapping.builder().deviceName("device_name").ebs(EbsInstanceBlockDevice.builder().volumeId("volume_id").build()).build());

        return instance.build();
    }
}
