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
import com.amazonaws.services.ec2.model.DescribeVpcsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcsResult;
import com.amazonaws.services.ec2.model.Vpc;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VpcTableProvider
        implements TableProvider
{
    private static Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("id")
                .addStringField("cidr_block")
                .addStringField("dhcp_opts")
                .addStringField("tenancy")
                .addStringField("owner")
                .addStringField("state")
                .addBitField("is_default")
                .addListField("tags", Types.MinorType.VARCHAR.getType())
                .addMetadata("id", "VPC Id")
                .addMetadata("cidr_block", "CIDR block used to vend IPs for the VPC.")
                .addMetadata("dhcp_opts", "DHCP options used for DNS resolution in the VPC.")
                .addMetadata("tenancy", "EC2 Instance tenancy of this VPC (e.g. dedicated)")
                .addMetadata("owner", "The owner of the VPC.")
                .addMetadata("state", "The state of the VPC.")
                .addMetadata("is_default", "True if the VPC is the default VPC.")
                .addMetadata("tags", "Tags associated with the volume.")
                .build();
    }

    private AmazonEC2 ec2;

    public VpcTableProvider(AmazonEC2 ec2)
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
        return new TableName(getSchema(), "vpcs");
    }

    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    @Override
    public void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        DescribeVpcsRequest request = new DescribeVpcsRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setVpcIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        DescribeVpcsResult response = ec2.describeVpcs(request);
        for (Vpc vpc : response.getVpcs()) {
            instanceToRow(vpc, constraintEvaluator, spiller, recordsRequest);
        }
    }

    private void instanceToRow(Vpc vpc,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            ReadRecordsRequest request)
    {
        final Map<String, Field> fields = new HashMap<>();
        request.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("id")) {
                String value = vpc.getVpcId();
                matched &= constraintEvaluator.apply("id", value);
                BlockUtils.setValue(block.getFieldVector("id"), row, value);
            }

            if (matched && fields.containsKey("cidr_block")) {
                String value = vpc.getCidrBlock();
                matched &= constraintEvaluator.apply("cidr_block", value);
                BlockUtils.setValue(block.getFieldVector("cidr_block"), row, value);
            }

            if (matched && fields.containsKey("dhcp_opts")) {
                String value = vpc.getDhcpOptionsId();
                matched &= constraintEvaluator.apply("dhcp_opts", value);
                BlockUtils.setValue(block.getFieldVector("dhcp_opts"), row, value);
            }

            if (matched && fields.containsKey("tenancy")) {
                String value = vpc.getInstanceTenancy();
                matched &= constraintEvaluator.apply("tenancy", value);
                BlockUtils.setValue(block.getFieldVector("tenancy"), row, value);
            }

            if (matched && fields.containsKey("owner")) {
                String value = vpc.getOwnerId();
                matched &= constraintEvaluator.apply("owner", value);
                BlockUtils.setValue(block.getFieldVector("owner"), row, value);
            }

            if (matched && fields.containsKey("state")) {
                String value = vpc.getState();
                matched &= constraintEvaluator.apply("state", value);
                BlockUtils.setValue(block.getFieldVector("state"), row, value);
            }

            if (matched && fields.containsKey("is_default")) {
                boolean value = vpc.getIsDefault();
                matched &= constraintEvaluator.apply("is_default", value);
                BlockUtils.setValue(block.getFieldVector("is_default"), row, value);
            }

            if (matched && fields.containsKey("tags")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("tags");
                List<String> interfaces = vpc.getTags().stream()
                        .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, interfaces);
            }

            return matched ? 1 : 0;
        });
    }
}
