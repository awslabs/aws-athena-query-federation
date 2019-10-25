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
import com.amazonaws.services.ec2.model.DescribeRouteTablesRequest;
import com.amazonaws.services.ec2.model.DescribeRouteTablesResult;
import com.amazonaws.services.ec2.model.Route;
import com.amazonaws.services.ec2.model.RouteTable;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps your EC2 RouteTable entries (routes) to a table.
 */
public class RouteTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private AmazonEC2 ec2;

    public RouteTableProvider(AmazonEC2 ec2)
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
        return new TableName(getSchema(), "routing_tables");
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
     * Calls DescribeRouteTables on the AWS EC2 Client returning all Routes that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific RoutingTables) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        final Map<String, Field> fields = new HashMap<>();
        recordsRequest.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        boolean done = false;
        DescribeRouteTablesRequest request = new DescribeRouteTablesRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("routeTableId");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setRouteTableIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        while (!done) {
            DescribeRouteTablesResult response = ec2.describeRouteTables(request);

            for (RouteTable nextRouteTable : response.getRouteTables()) {
                for (Route route : nextRouteTable.getRoutes()) {
                    instanceToRow(nextRouteTable, route, constraintEvaluator, spiller, fields);
                }
            }

            request.setNextToken(response.getNextToken());

            if (response.getNextToken() == null) {
                done = true;
            }
        }
    }

    /**
     * Maps an EC2 Route into a row in our Apache Arrow response block(s).
     *
     * @param routeTable The RouteTable that owns the given Route.
     * @param route The Route to map.
     * @param constraintEvaluator The ConstraintEvaluator we can use to filter results.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @param fields The set of fields that need to be projected.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(RouteTable routeTable,
            Route route,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            Map<String, Field> fields)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("routeTableId")) {
                String value = routeTable.getRouteTableId();
                matched &= constraintEvaluator.apply("routeTableId", value);
                BlockUtils.setValue(block.getFieldVector("routeTableId"), row, value);
            }

            if (matched && fields.containsKey("owner")) {
                String value = routeTable.getOwnerId();
                matched &= constraintEvaluator.apply("owner", value);
                BlockUtils.setValue(block.getFieldVector("owner"), row, value);
            }

            if (matched && fields.containsKey("vpc")) {
                String value = routeTable.getVpcId();
                matched &= constraintEvaluator.apply("vpc", value);
                BlockUtils.setValue(block.getFieldVector("vpc"), row, value);
            }

            if (matched && fields.containsKey("associations")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("associations");
                List<String> values = routeTable.getAssociations().stream()
                        .map(next -> next.getSubnetId() + ":" + next.getRouteTableId()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("tags")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("tags");
                List<String> values = routeTable.getTags().stream()
                        .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("propagatingVgws")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("propagatingVgws");
                List<String> values = routeTable.getPropagatingVgws().stream()
                        .map(next -> next.getGatewayId()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (matched && fields.containsKey("dst_cidr")) {
                String value = route.getDestinationCidrBlock();
                matched &= constraintEvaluator.apply("dst_cidr", value);
                BlockUtils.setValue(block.getFieldVector("dst_cidr"), row, value);
            }

            if (matched && fields.containsKey("dst_cidr_v6")) {
                String value = route.getDestinationIpv6CidrBlock();
                matched &= constraintEvaluator.apply("dst_cidr_v6", value);
                BlockUtils.setValue(block.getFieldVector("dst_cidr_v6"), row, value);
            }

            if (matched && fields.containsKey("dst_prefix_list")) {
                String value = route.getDestinationPrefixListId();
                matched &= constraintEvaluator.apply("dst_prefix_list", value);
                BlockUtils.setValue(block.getFieldVector("dst_prefix_list"), row, value);
            }

            if (matched && fields.containsKey("egress_igw")) {
                String value = route.getEgressOnlyInternetGatewayId();
                matched &= constraintEvaluator.apply("egress_igw", value);
                BlockUtils.setValue(block.getFieldVector("egress_igw"), row, value);
            }

            if (matched && fields.containsKey("gateway")) {
                String value = route.getGatewayId();
                matched &= constraintEvaluator.apply("gateway", value);
                BlockUtils.setValue(block.getFieldVector("gateway"), row, value);
            }

            if (matched && fields.containsKey("instanceId")) {
                String value = route.getInstanceId();
                matched &= constraintEvaluator.apply("instanceId", value);
                BlockUtils.setValue(block.getFieldVector("instanceId"), row, value);
            }

            if (matched && fields.containsKey("instance_owner")) {
                String value = route.getInstanceOwnerId();
                matched &= constraintEvaluator.apply("instance_owner", value);
                BlockUtils.setValue(block.getFieldVector("instance_owner"), row, value);
            }

            if (matched && fields.containsKey("nat_gateway")) {
                String value = route.getNatGatewayId();
                matched &= constraintEvaluator.apply("nat_gateway", value);
                BlockUtils.setValue(block.getFieldVector("nat_gateway"), row, value);
            }

            if (matched && fields.containsKey("interface")) {
                String value = route.getNetworkInterfaceId();
                matched &= constraintEvaluator.apply("interface", value);
                BlockUtils.setValue(block.getFieldVector("interface"), row, value);
            }

            if (matched && fields.containsKey("origin")) {
                String value = route.getOrigin();
                matched &= constraintEvaluator.apply("origin", value);
                BlockUtils.setValue(block.getFieldVector("origin"), row, value);
            }

            if (matched && fields.containsKey("state")) {
                String value = route.getState();
                matched &= constraintEvaluator.apply("state", value);
                BlockUtils.setValue(block.getFieldVector("state"), row, value);
            }

            if (matched && fields.containsKey("transit_gateway")) {
                String value = route.getTransitGatewayId();
                matched &= constraintEvaluator.apply("transit_gateway", value);
                BlockUtils.setValue(block.getFieldVector("transit_gateway"), row, value);
            }

            if (matched && fields.containsKey("vpc_peering_con")) {
                String value = route.getVpcPeeringConnectionId();
                matched &= constraintEvaluator.apply("vpc_peering_con", value);
                BlockUtils.setValue(block.getFieldVector("vpc_peering_con"), row, value);
            }

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("routeTableId")
                .addStringField("owner")
                .addStringField("vpc")
                .addListField("associations", Types.MinorType.VARCHAR.getType())
                .addListField("tags", Types.MinorType.VARCHAR.getType())
                .addListField("propagatingVgws", Types.MinorType.VARCHAR.getType())
                .addStringField("dst_cidr")
                .addStringField("dst_cidr_v6")
                .addStringField("dst_prefix_list")
                .addStringField("egress_igw")
                .addStringField("gateway")
                .addStringField("instanceId")
                .addStringField("instance_owner")
                .addStringField("nat_gateway")
                .addStringField("interface")
                .addStringField("origin")
                .addStringField("state")
                .addStringField("transit_gateway")
                .addStringField("vpc_peering_con")
                .addMetadata("routeTableId", "Id of the route table the route belongs to.")
                .addMetadata("owner", "Owner of the route table.")
                .addMetadata("vpc", "VPC the route table is associated with.")
                .addMetadata("associations", "List of associations for this route table.")
                .addMetadata("tags", "Tags on the route table.")
                .addMetadata("propagatingVgws", "Vgws the route table propogates through.")
                .addMetadata("dst_cidr", "Destination IPv4 CIDR block for the route.")
                .addMetadata("dst_cidr_v6", "Destination IPv6 CIDR block for the route.")
                .addMetadata("dst_prefix_list", "Destination prefix list for the route.")
                .addMetadata("egress_igw", "Egress gateway for the route.")
                .addMetadata("gateway", "Gateway for the route.")
                .addMetadata("instanceId", "Instance id of the route.")
                .addMetadata("instance_owner", "Owner of the route.")
                .addMetadata("nat_gateway", "NAT gateway used by the route.")
                .addMetadata("interface", "Interface associated with the route.")
                .addMetadata("origin", "Origin of the route.")
                .addMetadata("state", "State of the route.")
                .addMetadata("transit_gateway", "Transit Gateway associated with the route.")
                .addMetadata("vpc_peering_con", "VPC Peering connection associated with the route.")
                .build();
    }
}
