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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeRouteTablesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeRouteTablesResponse;
import software.amazon.awssdk.services.ec2.model.Route;
import software.amazon.awssdk.services.ec2.model.RouteTable;

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
    private Ec2Client ec2;

    public RouteTableProvider(Ec2Client ec2)
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
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        boolean done = false;
        DescribeRouteTablesRequest.Builder request = DescribeRouteTablesRequest.builder();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("route_table_id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.routeTableIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        while (!done) {
            DescribeRouteTablesResponse response = ec2.describeRouteTables(request.build());

            for (RouteTable nextRouteTable : response.routeTables()) {
                for (Route route : nextRouteTable.routes()) {
                    instanceToRow(nextRouteTable, route, spiller);
                }
            }

            request.nextToken(response.nextToken());

            if (response.nextToken() == null || !queryStatusChecker.isQueryRunning()) {
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

            matched &= block.offerValue("route_table_id", row, routeTable.routeTableId());
            matched &= block.offerValue("owner", row, routeTable.ownerId());
            matched &= block.offerValue("vpc", row, routeTable.vpcId());
            matched &= block.offerValue("dst_cidr", row, route.destinationCidrBlock());
            matched &= block.offerValue("dst_cidr_v6", row, route.destinationIpv6CidrBlock());
            matched &= block.offerValue("dst_prefix_list", row, route.destinationPrefixListId());
            matched &= block.offerValue("egress_igw", row, route.egressOnlyInternetGatewayId());
            matched &= block.offerValue("gateway", row, route.gatewayId());
            matched &= block.offerValue("instance_id", row, route.instanceId());
            matched &= block.offerValue("instance_owner", row, route.instanceOwnerId());
            matched &= block.offerValue("nat_gateway", row, route.natGatewayId());
            matched &= block.offerValue("interface", row, route.networkInterfaceId());
            matched &= block.offerValue("origin", row, route.originAsString());
            matched &= block.offerValue("state", row, route.stateAsString());
            matched &= block.offerValue("transit_gateway", row, route.transitGatewayId());
            matched &= block.offerValue("vpc_peering_con", row, route.vpcPeeringConnectionId());

            List<String> associations = routeTable.associations().stream()
                    .map(next -> next.subnetId() + ":" + next.routeTableId()).collect(Collectors.toList());
            matched &= block.offerComplexValue("associations", row, FieldResolver.DEFAULT, associations);

            List<String> tags = routeTable.tags().stream()
                    .map(next -> next.key() + ":" + next.value()).collect(Collectors.toList());
            matched &= block.offerComplexValue("tags", row, FieldResolver.DEFAULT, tags);

            List<String> propagatingVgws = routeTable.propagatingVgws().stream()
                    .map(next -> next.gatewayId()).collect(Collectors.toList());
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
