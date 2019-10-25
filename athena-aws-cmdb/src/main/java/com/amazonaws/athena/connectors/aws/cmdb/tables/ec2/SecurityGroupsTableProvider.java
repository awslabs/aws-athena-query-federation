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
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.SecurityGroup;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SecurityGroupsTableProvider
        implements TableProvider
{
    private static final String INGRESS = "ingress";
    private static final String EGRESS = "egress";

    private static final Schema SCHEMA;
    private AmazonEC2 ec2;

    public SecurityGroupsTableProvider(AmazonEC2 ec2)
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
        return new TableName(getSchema(), "security_groups");
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
        DescribeSecurityGroupsRequest request = new DescribeSecurityGroupsRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setGroupIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        ValueSet nameConstraint = recordsRequest.getConstraints().getSummary().get("name");
        if (nameConstraint != null && nameConstraint.isSingleValue()) {
            request.setGroupNames(Collections.singletonList(nameConstraint.getSingleValue().toString()));
        }

        while (!done) {
            DescribeSecurityGroupsResult response = ec2.describeSecurityGroups(request);

            for (SecurityGroup next : response.getSecurityGroups()) {
                for (IpPermission nextPerm : next.getIpPermissions()) {
                    instanceToRow(next, nextPerm, INGRESS, constraintEvaluator, spiller, recordsRequest);
                }

                for (IpPermission nextPerm : next.getIpPermissionsEgress()) {
                    instanceToRow(next, nextPerm, EGRESS, constraintEvaluator, spiller, recordsRequest);
                }
            }

            request.setNextToken(response.getNextToken());

            if (response.getNextToken() == null) {
                done = true;
            }
        }
    }

    private void instanceToRow(SecurityGroup securityGroup, IpPermission permission, String direction,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            ReadRecordsRequest request)
    {
        final Map<String, Field> fields = new HashMap<>();
        request.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("id")) {
                String value = securityGroup.getGroupId();
                matched &= constraintEvaluator.apply("id", value);
                BlockUtils.setValue(block.getFieldVector("id"), row, value);
            }

            if (matched && fields.containsKey("name")) {
                String value = securityGroup.getGroupName();
                matched &= constraintEvaluator.apply("name", value);
                BlockUtils.setValue(block.getFieldVector("name"), row, value);
            }

            if (matched && fields.containsKey("description")) {
                String value = securityGroup.getDescription();
                matched &= constraintEvaluator.apply("description", value);
                BlockUtils.setValue(block.getFieldVector("description"), row, value);
            }

            if (matched && fields.containsKey("from_port")) {
                Integer value = permission.getFromPort();
                matched &= constraintEvaluator.apply("from_port", value);
                BlockUtils.setValue(block.getFieldVector("from_port"), row, value);
            }

            if (matched && fields.containsKey("to_port")) {
                Integer value = permission.getFromPort();
                matched &= constraintEvaluator.apply("to_port", value);
                BlockUtils.setValue(block.getFieldVector("to_port"), row, value);
            }

            if (matched && fields.containsKey("protocol")) {
                String value = permission.getIpProtocol();
                matched &= constraintEvaluator.apply("protocol", value);
                BlockUtils.setValue(block.getFieldVector("protocol"), row, value);
            }

            if (matched && fields.containsKey("direction")) {
                matched &= constraintEvaluator.apply("direction", direction);
                BlockUtils.setValue(block.getFieldVector("direction"), row, direction);
            }

            if (matched && fields.containsKey("ipv4_ranges")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("ipv4_ranges");
                List<String> values = permission.getIpv4Ranges().stream()
                        .map(next -> next.getCidrIp() + ":" + next.getDescription()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("ipv6_ranges")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("ipv6_ranges");
                List<String> values = permission.getIpv6Ranges().stream()
                        .map(next -> next.getCidrIpv6() + ":" + next.getDescription()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("prefixLists")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("prefixLists");
                List<String> values = permission.getPrefixListIds().stream()
                        .map(next -> next.getPrefixListId() + ":" + next.getDescription()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("userIdGroups")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("userIdGroups");
                List<String> values = permission.getUserIdGroupPairs().stream()
                        .map(next -> next.getUserId() + ":" + next.getVpcPeeringConnectionId() + ":" + next.getDescription())
                        .collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            return matched ? 1 : 0;
        });
    }

    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("id")
                .addStringField("name")
                .addStringField("description")
                .addIntField("from_port")
                .addIntField("to_port")
                .addStringField("protocol")
                .addStringField("direction")
                .addListField("ipv4_ranges", Types.MinorType.VARCHAR.getType())
                .addListField("ipv6_ranges", Types.MinorType.VARCHAR.getType())
                .addListField("prefixLists", Types.MinorType.VARCHAR.getType())
                .addListField("userIdGroups", Types.MinorType.VARCHAR.getType())
                .addMetadata("id", "Security Group ID.")
                .addMetadata("name", "Name of the security group.")
                .addMetadata("description", "Description of the security group.")
                .addMetadata("from_port", "Beginging of the port range covered by this security group.")
                .addMetadata("to_port", "Ending of the port range covered by this security group.")
                .addMetadata("protocol", "The network protocol covered by this security group.")
                .addMetadata("direction", "Notes if the rule applies inbound (ingress) or outbound (egress).")
                .addMetadata("ipv4_ranges", "The ip v4 ranges covered by this security group.")
                .addMetadata("ipv6_ranges", "The ip v6 ranges covered by this security group.")
                .addMetadata("prefixLists", "The prefix lists covered by this security group.")
                .addMetadata("userIdGroups", "The user id groups covered by this security group.")
                .build();
    }
}
