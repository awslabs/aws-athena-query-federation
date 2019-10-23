package com.amazonaws.athena.connectors.aws.cmdb.tables.ec2;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Ec2TableProvider
        implements TableProvider
{
    private static Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("instanceId")
                .addStringField("imageId")
                .addStringField("instanceType")
                .addStringField("platform")
                .addStringField("privateDnsName")
                .addStringField("privateIpAddress")
                .addStringField("publicDnsName")
                .addStringField("publicIpAddress")
                .addStringField("subnetId")
                .addStringField("vpcId")
                .addStringField("architecture")
                .addStringField("instanceLifecycle")
                .addStringField("rootDeviceName")
                .addStringField("rootDeviceType")
                .addStringField("spotInstanceRequestId")
                .addStringField("virtualizationType")
                .addStringField("keyName")
                .addStringField("kernelId")
                .addStringField("capacityReservationId")
                .addField("launchTime", Types.MinorType.DATEMILLI.getType())
                .addStructField("state")
                .addChildField("state", "name", Types.MinorType.VARCHAR.getType())
                .addChildField("state", "code", Types.MinorType.INT.getType())
                .addStructField("stateReason")
                .addChildField("stateReason", "message", Types.MinorType.VARCHAR.getType())
                .addChildField("stateReason", "code", Types.MinorType.VARCHAR.getType())

                //Example of a List of Structs
                .addField(
                        FieldBuilder.newBuilder("networkInterfaces", new ArrowType.List())
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
                .addBitField("ebsOptimized")
                .addListField("securityGroups", Types.MinorType.VARCHAR.getType())
                .addListField("securityGroupNames", Types.MinorType.VARCHAR.getType())
                .addListField("ebsVolumes", Types.MinorType.VARCHAR.getType())
                .addMetadata("instanceId", "EC2 Instance id.")
                .addMetadata("imageId", "The id of the AMI used to boot the instance.")
                .addMetadata("instanceType", "The EC2 instance type,")
                .addMetadata("platform", "The platform of the instance (e.g. Linux)")
                .addMetadata("privateDnsName", "The private dns name of the instance.")
                .addMetadata("privateIpAddress", "The private ip address of the instance.")
                .addMetadata("publicDnsName", "The public dns name of the instance.")
                .addMetadata("publicIpAddress", "The public ip address of the instance.")
                .addMetadata("subnetId", "The subnet id that the instance was launched in.")
                .addMetadata("vpcId", "The id of the VPC that the instance was launched in.")
                .addMetadata("architecture", "The architecture of the instance (e.g. x86).")
                .addMetadata("instanceLifecycle", "The lifecycle state of the instance.")
                .addMetadata("rootDeviceName", "The name of the root device that the instance booted from.")
                .addMetadata("rootDeviceType", "The type of the root device that the instance booted from.")
                .addMetadata("spotInstanceRequestId", "Spot Request ID if the instance was launched via spot. ")
                .addMetadata("virtualizationType", "The type of virtualization used by the instance (e.g. HVM)")
                .addMetadata("keyName", "The name of the ec2 instance from the name tag.")
                .addMetadata("kernelId", "The id of the kernel used in the AMI that booted the instance.")
                .addMetadata("capacityReservationId", "Capacity reservation id that this instance was launched against.")
                .addMetadata("launchTime", "The time that the instance was launched at.")
                .addMetadata("state", "The state of the ec2 instance.")
                .addMetadata("stateReason", "The reason for the 'state' associated with the instance.")
                .addMetadata("ebsOptimized", "True if the instance is EBS optimized.")
                .addMetadata("networkInterfaces", "The list of the network interfaces on the instance.")
                .addMetadata("securityGroups", "The list of security group (ids) attached to this instance.")
                .addMetadata("securityGroupNames", "The list of security group (names) attached to this instance.")
                .addMetadata("ebsVolumes", "The list of ebs volume (ids) attached to this instance.")
                .build();
    }

    private AmazonEC2 ec2;

    public Ec2TableProvider(AmazonEC2 ec2)
    {
        this.ec2 = ec2;
    }

    @Override
    public String getSchema()
    {
        return "ec2";
    }

    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "ec2_instances");
    }

    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    @Override
    public void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {

        boolean done = false;
        DescribeInstancesRequest request = new DescribeInstancesRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("instanceId");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setInstanceIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        while (!done) {
            DescribeInstancesResult response = ec2.describeInstances(request);

            for (Reservation reservation : response.getReservations()) {
                for (Instance instance : reservation.getInstances()) {
                    instanceToRow(instance, constraintEvaluator, spiller, recordsRequest);
                }
            }

            request.setNextToken(response.getNextToken());

            if (response.getNextToken() == null) {
                done = true;
            }
        }
    }

    private void instanceToRow(Instance instance,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            ReadRecordsRequest request)
    {
        final Map<String, Field> fields = new HashMap<>();
        request.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("instanceId")) {
                String value = instance.getInstanceId();
                matched &= constraintEvaluator.apply("instanceId", value);
                BlockUtils.setValue(block.getFieldVector("instanceId"), row, value);
            }

            if (matched && fields.containsKey("imageId")) {
                String value = instance.getImageId();
                matched &= constraintEvaluator.apply("imageId", value);
                BlockUtils.setValue(block.getFieldVector("imageId"), row, value);
            }

            if (matched && fields.containsKey("instanceType")) {
                String value = instance.getInstanceType();
                matched &= constraintEvaluator.apply("instanceType", value);
                BlockUtils.setValue(block.getFieldVector("instanceType"), row, value);
            }

            if (matched && fields.containsKey("state")) {
                //TODO: apply constraint for complex type
                StructVector vector = (StructVector) block.getFieldVector("state");
                BlockUtils.setComplexValue(vector, row, (Field field, Object val) -> {
                    if (field.getName().equals("name")) {
                        return ((InstanceState) val).getName();
                    }
                    else if (field.getName().equals("code")) {
                        return ((InstanceState) val).getCode();
                    }
                    throw new RuntimeException("Unknown field " + field.getName());
                }, instance.getState());
            }

            if (matched && fields.containsKey("networkInterfaces")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("networkInterfaces");

                BlockUtils.setComplexValue(vector, row, (Field field, Object val) -> {
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
                    }else if (field.getName().equals("private_ip")) {
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
            }

            if (matched && fields.containsKey("platform")) {
                String value = instance.getPlatform();
                matched &= constraintEvaluator.apply("platform", value);
                BlockUtils.setValue(block.getFieldVector("platform"), row, value);
            }

            if (matched && fields.containsKey("privateDnsName")) {
                String value = instance.getPrivateDnsName();
                matched &= constraintEvaluator.apply("privateDnsName", value);
                BlockUtils.setValue(block.getFieldVector("privateDnsName"), row, value);
            }

            if (matched && fields.containsKey("privateDnsName")) {
                String value = instance.getPrivateDnsName();
                matched &= constraintEvaluator.apply("privateDnsName", value);
                BlockUtils.setValue(block.getFieldVector("privateDnsName"), row, value);
            }

            if (matched && fields.containsKey("privateIpAddress")) {
                String value = instance.getPrivateIpAddress();
                matched &= constraintEvaluator.apply("privateIpAddress", value);
                BlockUtils.setValue(block.getFieldVector("privateIpAddress"), row, value);
            }

            if (matched && fields.containsKey("publicDnsName")) {
                String value = instance.getPublicDnsName();
                matched &= constraintEvaluator.apply("publicDnsName", value);
                BlockUtils.setValue(block.getFieldVector("publicDnsName"), row, value);
            }

            if (matched && fields.containsKey("publicIpAddress")) {
                String value = instance.getPublicIpAddress();
                matched &= constraintEvaluator.apply("publicIpAddress", value);
                BlockUtils.setValue(block.getFieldVector("publicIpAddress"), row, value);
            }

            if (matched && fields.containsKey("subnetId")) {
                String value = instance.getSubnetId();
                matched &= constraintEvaluator.apply("subnetId", value);
                BlockUtils.setValue(block.getFieldVector("subnetId"), row, value);
            }

            if (matched && fields.containsKey("vpcId")) {
                String value = instance.getVpcId();
                matched &= constraintEvaluator.apply("vpcId", value);
                BlockUtils.setValue(block.getFieldVector("vpcId"), row, value);
            }

            if (matched && fields.containsKey("architecture")) {
                String value = instance.getArchitecture();
                matched &= constraintEvaluator.apply("architecture", value);
                BlockUtils.setValue(block.getFieldVector("architecture"), row, value);
            }

            if (matched && fields.containsKey("instanceLifecycle")) {
                String value = instance.getInstanceLifecycle();
                matched &= constraintEvaluator.apply("instanceLifecycle", value);
                BlockUtils.setValue(block.getFieldVector("instanceLifecycle"), row, value);
            }

            if (matched && fields.containsKey("rootDeviceName")) {
                String value = instance.getRootDeviceName();
                matched &= constraintEvaluator.apply("rootDeviceName", value);
                BlockUtils.setValue(block.getFieldVector("rootDeviceName"), row, value);
            }

            if (matched && fields.containsKey("rootDeviceType")) {
                String value = instance.getRootDeviceType();
                matched &= constraintEvaluator.apply("rootDeviceType", value);
                BlockUtils.setValue(block.getFieldVector("rootDeviceType"), row, value);
            }

            if (matched && fields.containsKey("spotInstanceRequestId")) {
                String value = instance.getSpotInstanceRequestId();
                matched &= constraintEvaluator.apply("spotInstanceRequestId", value);
                BlockUtils.setValue(block.getFieldVector("spotInstanceRequestId"), row, value);
            }

            if (matched && fields.containsKey("virtualizationType")) {
                String value = instance.getVirtualizationType();
                matched &= constraintEvaluator.apply("virtualizationType", value);
                BlockUtils.setValue(block.getFieldVector("virtualizationType"), row, value);
            }

            if (matched && fields.containsKey("keyName")) {
                String value = instance.getKeyName();
                matched &= constraintEvaluator.apply("keyName", value);
                BlockUtils.setValue(block.getFieldVector("keyName"), row, value);
            }

            if (matched && fields.containsKey("kernelId")) {
                String value = instance.getKernelId();
                matched &= constraintEvaluator.apply("kernelId", value);
                BlockUtils.setValue(block.getFieldVector("kernelId"), row, value);
            }

            if (matched && fields.containsKey("capacityReservationId")) {
                String value = instance.getCapacityReservationId();
                matched &= constraintEvaluator.apply("capacityReservationId", value);
                BlockUtils.setValue(block.getFieldVector("capacityReservationId"), row, value);
            }

            if (matched && fields.containsKey("launchTime")) {
                Date value = instance.getLaunchTime();
                matched &= constraintEvaluator.apply("launchTime", value);
                BlockUtils.setValue(block.getFieldVector("launchTime"), row, value);
            }

            if (matched && fields.containsKey("stateReason")) {
                //TODO: apply constraint for complex type
                StructVector vector = (StructVector) block.getFieldVector("stateReason");
                BlockUtils.setComplexValue(vector, row, (Field field, Object val) -> {
                    if (field.getName().equals("message")) {
                        return ((StateReason) val).getMessage();
                    }
                    else if (field.getName().equals("code")) {
                        return ((StateReason) val).getCode();
                    }
                    throw new RuntimeException("Unknown field " + field.getName());
                }, instance.getStateReason());
            }

            if (matched && fields.containsKey("ebsOptimized")) {
                boolean value = instance.getEbsOptimized();
                matched &= constraintEvaluator.apply("ebsOptimized", value);
                BlockUtils.setValue(block.getFieldVector("ebsOptimized"), row, value);
            }

            if (matched && fields.containsKey("securityGroups")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("securityGroups");
                List<String> values = instance.getSecurityGroups().stream()
                        .map(next -> next.getGroupId()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("securityGroupNames")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("securityGroupNames");
                List<String> values = instance.getSecurityGroups().stream()
                        .map(next -> next.getGroupName()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("ebsVolumes")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("ebsVolumes");
                List<String> values = instance.getBlockDeviceMappings().stream()
                        .map(next -> next.getEbs().getVolumeId()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            return matched ? 1 : 0;
        });
    }
}
