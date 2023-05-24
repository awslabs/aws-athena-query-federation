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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceNetworkInterface;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.StateReason;
import com.amazonaws.services.ec2.model.Tag;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps your EC2 instances to a table.
 */
public class Ec2TableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private AmazonEC2 ec2;

    public Ec2TableProvider(AmazonEC2 ec2)
    {
        this.ec2 = ec2;
    }

    /**
     * @See TableProvider
     */
    @Override
    public String getSchema()
    {
        return "ec2";
    }

    /**
     * @See TableProvider
     */
    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "ec2_instances");
    }

    /**
     * @See TableProvider
     */
    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    /**
     * Calls DescribeInstances on the AWS EC2 Client returning all instances that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific ec2 instance) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        boolean done = false;
        DescribeInstancesRequest request = new DescribeInstancesRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("instance_id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setInstanceIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        while (!done) {
            DescribeInstancesResult response = ec2.describeInstances(request);

            for (Reservation reservation : response.getReservations()) {
                for (Instance instance : reservation.getInstances()) {
                    instanceToRow(instance, spiller);
                }
            }

            request.setNextToken(response.getNextToken());

            if (response.getNextToken() == null || !queryStatusChecker.isQueryRunning()) {
                done = true;
            }
        }
    }

    /**
     * Maps an EC2 Instance into a row in our Apache Arrow response block(s).
     *
     * @param instance The EBS Volume to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(Instance instance,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("instance_id", row, instance.getInstanceId());
            matched &= block.offerValue("image_id", row, instance.getImageId());
            matched &= block.offerValue("instance_type", row, instance.getInstanceType());
            matched &= block.offerValue("platform", row, instance.getPlatform());
            matched &= block.offerValue("private_dns_name", row, instance.getPrivateDnsName());
            matched &= block.offerValue("private_ip_address", row, instance.getPrivateIpAddress());
            matched &= block.offerValue("public_dns_name", row, instance.getPublicDnsName());
            matched &= block.offerValue("public_ip_address", row, instance.getPublicIpAddress());
            matched &= block.offerValue("subnet_id", row, instance.getSubnetId());
            matched &= block.offerValue("vpc_id", row, instance.getVpcId());
            matched &= block.offerValue("architecture", row, instance.getArchitecture());
            matched &= block.offerValue("instance_lifecycle", row, instance.getInstanceLifecycle());
            matched &= block.offerValue("root_device_name", row, instance.getRootDeviceName());
            matched &= block.offerValue("root_device_type", row, instance.getRootDeviceType());
            matched &= block.offerValue("spot_instance_request_id", row, instance.getSpotInstanceRequestId());
            matched &= block.offerValue("virtualization_type", row, instance.getVirtualizationType());
            matched &= block.offerValue("key_name", row, instance.getKeyName());
            matched &= block.offerValue("kernel_id", row, instance.getKernelId());
            matched &= block.offerValue("capacity_reservation_id", row, instance.getCapacityReservationId());
            matched &= block.offerValue("launch_time", row, instance.getLaunchTime());

            matched &= block.offerComplexValue("state",
                    row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("name")) {
                            return ((InstanceState) val).getName();
                        }
                        else if (field.getName().equals("code")) {
                            return ((InstanceState) val).getCode();
                        }
                        throw new RuntimeException("Unknown field " + field.getName());
                    }, instance.getState());

            matched &= block.offerComplexValue("network_interfaces",
                    row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("status")) {
                            return ((InstanceNetworkInterface) val).getStatus();
                        }
                        else if (field.getName().equals("subnet")) {
                            return ((InstanceNetworkInterface) val).getSubnetId();
                        }
                        else if (field.getName().equals("vpc")) {
                            return ((InstanceNetworkInterface) val).getVpcId();
                        }
                        else if (field.getName().equals("mac")) {
                            return ((InstanceNetworkInterface) val).getMacAddress();
                        }
                        else if (field.getName().equals("private_dns")) {
                            return ((InstanceNetworkInterface) val).getPrivateDnsName();
                        }
                        else if (field.getName().equals("private_ip")) {
                            return ((InstanceNetworkInterface) val).getPrivateIpAddress();
                        }
                        else if (field.getName().equals("security_groups")) {
                            return ((InstanceNetworkInterface) val).getGroups().stream().map(next -> next.getGroupName() + ":" + next.getGroupId()).collect(Collectors.toList());
                        }
                        else if (field.getName().equals("interface_id")) {
                            return ((InstanceNetworkInterface) val).getNetworkInterfaceId();
                        }

                        throw new RuntimeException("Unknown field " + field.getName());
                    }, instance.getNetworkInterfaces());

            matched &= block.offerComplexValue("state_reason", row, (Field field, Object val) -> {
                if (field.getName().equals("message")) {
                    return ((StateReason) val).getMessage();
                }
                else if (field.getName().equals("code")) {
                    return ((StateReason) val).getCode();
                }
                throw new RuntimeException("Unknown field " + field.getName());
            }, instance.getStateReason());

            matched &= block.offerValue("ebs_optimized", row, instance.getEbsOptimized());

            List<String> securityGroups = instance.getSecurityGroups().stream()
                    .map(next -> next.getGroupId()).collect(Collectors.toList());
            matched &= block.offerComplexValue("security_groups", row, FieldResolver.DEFAULT, securityGroups);

            List<String> securityGroupNames = instance.getSecurityGroups().stream()
                    .map(next -> next.getGroupName()).collect(Collectors.toList());
            matched &= block.offerComplexValue("security_group_names", row, FieldResolver.DEFAULT, securityGroupNames);

            List<String> ebsVolumes = instance.getBlockDeviceMappings().stream()
                    .map(next -> next.getEbs().getVolumeId()).collect(Collectors.toList());
            matched &= block.offerComplexValue("ebs_volumes", row, FieldResolver.DEFAULT, ebsVolumes);

            matched &= block.offerComplexValue("tags", row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("key")) {
                            return ((Tag) val).getKey();
                        }
                        else if (field.getName().equals("value")) {
                            return ((Tag) val).getValue();
                        }

                        throw new RuntimeException("Unexpected field " + field.getName());
                    },
                    instance.getTags());

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("instance_id")
                .addStringField("image_id")
                .addStringField("instance_type")
                .addStringField("platform")
                .addStringField("private_dns_name")
                .addStringField("private_ip_address")
                .addStringField("public_dns_name")
                .addStringField("public_ip_address")
                .addStringField("subnet_id")
                .addStringField("vpc_id")
                .addStringField("architecture")
                .addStringField("instance_lifecycle")
                .addStringField("root_device_name")
                .addStringField("root_device_type")
                .addStringField("spot_instance_request_id")
                .addStringField("virtualization_type")
                .addStringField("key_name")
                .addStringField("kernel_id")
                .addStringField("capacity_reservation_id")
                .addField("launch_time", Types.MinorType.DATEMILLI.getType())
                .addStructField("state")
                .addChildField("state", "name", Types.MinorType.VARCHAR.getType())
                .addChildField("state", "code", Types.MinorType.INT.getType())
                .addStructField("state_reason")
                .addChildField("state_reason", "message", Types.MinorType.VARCHAR.getType())
                .addChildField("state_reason", "code", Types.MinorType.VARCHAR.getType())

                //Example of a List of Structs
                .addField(
                        FieldBuilder.newBuilder("network_interfaces", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("interface", Types.MinorType.STRUCT.getType())
                                                .addStringField("status")
                                                .addStringField("subnet")
                                                .addStringField("vpc")
                                                .addStringField("mac")
                                                .addStringField("private_dns")
                                                .addStringField("private_ip")
                                                .addListField("security_groups", Types.MinorType.VARCHAR.getType())
                                                .addStringField("interface_id")
                                                .build())
                                .build())
                .addBitField("ebs_optimized")
                .addListField("security_groups", Types.MinorType.VARCHAR.getType())
                .addListField("security_group_names", Types.MinorType.VARCHAR.getType())
                .addListField("ebs_volumes", Types.MinorType.VARCHAR.getType())
                .addField(FieldBuilder.newBuilder("tags", new ArrowType.List())
                        .addField(FieldBuilder.newBuilder("tag", Types.MinorType.STRUCT.getType())
                                        .addStringField("key")
                                        .addStringField("value")
                                        .build())
                        .build())
                .addMetadata("instance_id", "EC2 Instance id.")
                .addMetadata("image_id", "The id of the AMI used to boot the instance.")
                .addMetadata("instance_type", "The EC2 instance type,")
                .addMetadata("platform", "The platform of the instance (e.g. Linux)")
                .addMetadata("private_dns_name", "The private dns name of the instance.")
                .addMetadata("private_ip_address", "The private ip address of the instance.")
                .addMetadata("public_dns_name", "The public dns name of the instance.")
                .addMetadata("public_ip_address", "The public ip address of the instance.")
                .addMetadata("subnet_id", "The subnet id that the instance was launched in.")
                .addMetadata("vpc_id", "The id of the VPC that the instance was launched in.")
                .addMetadata("architecture", "The architecture of the instance (e.g. x86).")
                .addMetadata("instance_lifecycle", "The lifecycle state of the instance.")
                .addMetadata("root_device_name", "The name of the root device that the instance booted from.")
                .addMetadata("root_device_type", "The type of the root device that the instance booted from.")
                .addMetadata("spot_instance_requestId", "Spot Request ID if the instance was launched via spot. ")
                .addMetadata("virtualization_type", "The type of virtualization used by the instance (e.g. HVM)")
                .addMetadata("key_name", "The name of the ec2 instance from the name tag.")
                .addMetadata("kernel_id", "The id of the kernel used in the AMI that booted the instance.")
                .addMetadata("capacity_reservation_id", "Capacity reservation id that this instance was launched against.")
                .addMetadata("launch_time", "The time that the instance was launched at.")
                .addMetadata("state", "The state of the ec2 instance.")
                .addMetadata("state_reason", "The reason for the 'state' associated with the instance.")
                .addMetadata("ebs_optimized", "True if the instance is EBS optimized.")
                .addMetadata("network_interfaces", "The list of the network interfaces on the instance.")
                .addMetadata("security_groups", "The list of security group (ids) attached to this instance.")
                .addMetadata("security_group_names", "The list of security group (names) attached to this instance.")
                .addMetadata("ebs_volumes", "The list of ebs volume (ids) attached to this instance.")
                .addMetadata("tags", "Tags associated with the instance.")
                .build();
    }
}
