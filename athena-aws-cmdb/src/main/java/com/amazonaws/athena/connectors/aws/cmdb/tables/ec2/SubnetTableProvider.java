package com.amazonaws.athena.connectors.aws.cmdb.tables.ec2;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
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
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Subnet;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SubnetTableProvider
        implements TableProvider
{
    private static Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("id")
                .addStringField("availability_zone")
                .addIntField("available_ip_count")
                .addStringField("cidr_block")
                .addBitField("default_for_az")
                .addBitField("map_public_ip")
                .addStringField("owner")
                .addStringField("state")
                .addListField("tags", Types.MinorType.VARCHAR.getType())
                .addStringField("vpc")
                .addMetadata("id", "Subnet Id")
                .addMetadata("availability_zone", "Availability zone the subnet is in.")
                .addMetadata("available_ip_count", "Number of available IPs in the subnet.")
                .addMetadata("cidr_block", "The CIDR block that the subnet uses to allocate addresses.")
                .addMetadata("default_for_az", "True if this is the default subnet for the AZ.")
                .addMetadata("map_public_ip", "True if public addresses are signed by default in this subnet.")
                .addMetadata("owner", "Owner of the subnet.")
                .addMetadata("state", "The state of the subnet.")
                .addMetadata("vpc", "The VPC the subnet is part of.")
                .addMetadata("tags", "Tags associated with the volume.")
                .build();
    }

    private AmazonEC2 ec2;

    public SubnetTableProvider(AmazonEC2 ec2)
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
        return new TableName(getSchema(), "subnets");
    }

    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    @Override
    public void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        DescribeSubnetsRequest request = new DescribeSubnetsRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setSubnetIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        DescribeSubnetsResult response = ec2.describeSubnets(request);
        for (Subnet subnet : response.getSubnets()) {
            instanceToRow(subnet, constraintEvaluator, spiller, recordsRequest);
        }
    }

    private void instanceToRow(Subnet subnet,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            ReadRecordsRequest request)
    {
        final Map<String, Field> fields = new HashMap<>();
        request.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("id")) {
                String value = subnet.getSubnetId();
                matched &= constraintEvaluator.apply("id", value);
                BlockUtils.setValue(block.getFieldVector("id"), row, value);
            }

            if (matched && fields.containsKey("availability_zone")) {
                String value = subnet.getAvailabilityZone();
                matched &= constraintEvaluator.apply("availability_zone", value);
                BlockUtils.setValue(block.getFieldVector("availability_zone"), row, value);
            }

            if (matched && fields.containsKey("available_ip_count")) {
                Integer value = subnet.getAvailableIpAddressCount();
                matched &= constraintEvaluator.apply("available_ip_count", value);
                BlockUtils.setValue(block.getFieldVector("available_ip_count"), row, value);
            }

            if (matched && fields.containsKey("cidr_block")) {
                String value = subnet.getCidrBlock();
                matched &= constraintEvaluator.apply("cidr_block", value);
                BlockUtils.setValue(block.getFieldVector("cidr_block"), row, value);
            }

            if (matched && fields.containsKey("default_for_az")) {
                Boolean value = subnet.getDefaultForAz();
                matched &= constraintEvaluator.apply("default_for_az", value);
                BlockUtils.setValue(block.getFieldVector("default_for_az"), row, value);
            }

            if (matched && fields.containsKey("map_public_ip")) {
                Boolean value = subnet.getMapPublicIpOnLaunch();
                matched &= constraintEvaluator.apply("map_public_ip", value);
                BlockUtils.setValue(block.getFieldVector("map_public_ip"), row, value);
            }

            if (matched && fields.containsKey("owner")) {
                String value = subnet.getOwnerId();
                matched &= constraintEvaluator.apply("owner", value);
                BlockUtils.setValue(block.getFieldVector("owner"), row, value);
            }

            if (matched && fields.containsKey("state")) {
                String value = subnet.getState();
                matched &= constraintEvaluator.apply("state", value);
                BlockUtils.setValue(block.getFieldVector("state"), row, value);
            }

            if (matched && fields.containsKey("tags")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("tags");
                List<String> interfaces = subnet.getTags().stream()
                        .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, interfaces);
            }

            if (matched && fields.containsKey("vpc")) {
                String value = subnet.getVpcId();
                matched &= constraintEvaluator.apply("vpc", value);
                BlockUtils.setValue(block.getFieldVector("vpc"), row, value);
            }

            return matched ? 1 : 0;
        });
    }
}
