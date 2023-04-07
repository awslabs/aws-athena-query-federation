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
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeRouteTablesRequest;
import com.amazonaws.services.ec2.model.DescribeRouteTablesResult;
import com.amazonaws.services.ec2.model.Route;
import com.amazonaws.services.ec2.model.RouteTable;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;
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
        return TableName.newBuilder().setSchemaName(getSchema()).setTableName("routing_tables").build();
    }

    /**
     * @See TableProvider
     */
    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return GetTableResponse.newBuilder().setCatalogName(getTableRequest.getCatalogName()).setTableName(getTableName()).setSchema(ProtobufMessageConverter.toProtoSchemaBytes(SCHEMA)).build();
    }

    /**
     * Calls DescribeRouteTables on the AWS EC2 Client returning all Routes that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific RoutingTables) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockAllocator allocator, BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        boolean done = false;
        DescribeRouteTablesRequest request = new DescribeRouteTablesRequest();

        ValueSet idConstraint = ProtobufMessageConverter.fromProtoConstraints(allocator, recordsRequest.getConstraints()).getSummary().get("route_table_id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setRouteTableIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        while (!done) {
            DescribeRouteTablesResult response = ec2.describeRouteTables(request);

            for (RouteTable nextRouteTable : response.getRouteTables()) {
                for (Route route : nextRouteTable.getRoutes()) {
                    instanceToRow(nextRouteTable, route, spiller);
                }
            }

            request.setNextToken(response.getNextToken());

            if (response.getNextToken() == null || !queryStatusChecker.isQueryRunning()) {
                done = true;
            }
        }
    }

    /**
     * Maps an EC2 Route into a row in our Apache Arrow response block(s).
     *
     * @param routeTable The RouteTable that owns the given Route.
     * @param route The Route to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(RouteTable routeTable,
            Route route,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("route_table_id", row, routeTable.getRouteTableId());
            matched &= block.offerValue("owner", row, routeTable.getOwnerId());
            matched &= block.offerValue("vpc", row, routeTable.getVpcId());
            matched &= block.offerValue("dst_cidr", row, route.getDestinationCidrBlock());
            matched &= block.offerValue("dst_cidr_v6", row, route.getDestinationIpv6CidrBlock());
            matched &= block.offerValue("dst_prefix_list", row, route.getDestinationPrefixListId());
            matched &= block.offerValue("egress_igw", row, route.getEgressOnlyInternetGatewayId());
            matched &= block.offerValue("gateway", row, route.getGatewayId());
            matched &= block.offerValue("instance_id", row, route.getInstanceId());
            matched &= block.offerValue("instance_owner", row, route.getInstanceOwnerId());
            matched &= block.offerValue("nat_gateway", row, route.getNatGatewayId());
            matched &= block.offerValue("interface", row, route.getNetworkInterfaceId());
            matched &= block.offerValue("origin", row, route.getOrigin());
            matched &= block.offerValue("state", row, route.getState());
            matched &= block.offerValue("transit_gateway", row, route.getTransitGatewayId());
            matched &= block.offerValue("vpc_peering_con", row, route.getVpcPeeringConnectionId());

            List<String> associations = routeTable.getAssociations().stream()
                    .map(next -> next.getSubnetId() + ":" + next.getRouteTableId()).collect(Collectors.toList());
            matched &= block.offerComplexValue("associations", row, FieldResolver.DEFAULT, associations);

            List<String> tags = routeTable.getTags().stream()
                    .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
            matched &= block.offerComplexValue("tags", row, FieldResolver.DEFAULT, tags);

            List<String> propagatingVgws = routeTable.getPropagatingVgws().stream()
                    .map(next -> next.getGatewayId()).collect(Collectors.toList());
            matched &= block.offerComplexValue("propagating_vgws", row, FieldResolver.DEFAULT, propagatingVgws);

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("route_table_id")
                .addStringField("owner")
                .addStringField("vpc")
                .addListField("associations", Types.MinorType.VARCHAR.getType())
                .addListField("tags", Types.MinorType.VARCHAR.getType())
                .addListField("propagating_vgws", Types.MinorType.VARCHAR.getType())
                .addStringField("dst_cidr")
                .addStringField("dst_cidr_v6")
                .addStringField("dst_prefix_list")
                .addStringField("egress_igw")
                .addStringField("gateway")
                .addStringField("instance_id")
                .addStringField("instance_owner")
                .addStringField("nat_gateway")
                .addStringField("interface")
                .addStringField("origin")
                .addStringField("state")
                .addStringField("transit_gateway")
                .addStringField("vpc_peering_con")
                .addMetadata("route_table_id", "Id of the route table the route belongs to.")
                .addMetadata("owner", "Owner of the route table.")
                .addMetadata("vpc", "VPC the route table is associated with.")
                .addMetadata("associations", "List of associations for this route table.")
                .addMetadata("tags", "Tags on the route table.")
                .addMetadata("propagating_vgws", "Vgws the route table propogates through.")
                .addMetadata("dst_cidr", "Destination IPv4 CIDR block for the route.")
                .addMetadata("dst_cidr_v6", "Destination IPv6 CIDR block for the route.")
                .addMetadata("dst_prefix_list", "Destination prefix list for the route.")
                .addMetadata("egress_igw", "Egress gateway for the route.")
                .addMetadata("gateway", "Gateway for the route.")
                .addMetadata("instance_id", "Instance id of the route.")
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
