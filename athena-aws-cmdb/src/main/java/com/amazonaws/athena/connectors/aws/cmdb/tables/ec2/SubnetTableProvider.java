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
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsResponse;
import software.amazon.awssdk.services.ec2.model.Subnet;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps your EC2 Subnets to a table.
 */
public class SubnetTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private Ec2Client ec2;

    public SubnetTableProvider(Ec2Client ec2)
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
        return new TableName(getSchema(), "subnets");
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
     * Calls DescribeSubnets on the AWS EC2 Client returning all subnets that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific subnet) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        DescribeSubnetsRequest.Builder request = DescribeSubnetsRequest.builder();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.subnetIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        DescribeSubnetsResponse response = ec2.describeSubnets(request.build());
        for (Subnet subnet : response.subnets()) {
            instanceToRow(subnet, spiller);
        }
    }

    /**
     * Maps an EC2 Subnet into a row in our Apache Arrow response block(s).
     *
     * @param subnet The EC2 Subnet to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(Subnet subnet,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("id", row, subnet.subnetId());
            matched &= block.offerValue("availability_zone", row, subnet.availabilityZone());
            matched &= block.offerValue("available_ip_count", row, subnet.availableIpAddressCount());
            matched &= block.offerValue("cidr_block", row, subnet.cidrBlock());
            matched &= block.offerValue("default_for_az", row, subnet.defaultForAz());
            matched &= block.offerValue("map_public_ip", row, subnet.mapPublicIpOnLaunch());
            matched &= block.offerValue("owner", row, subnet.ownerId());
            matched &= block.offerValue("state", row, subnet.stateAsString());
            matched &= block.offerValue("vpc", row, subnet.vpcId());

            List<String> tags = subnet.tags().stream()
                    .map(next -> next.key() + ":" + next.value()).collect(Collectors.toList());
            matched &= block.offerComplexValue("tags", row, FieldResolver.DEFAULT, tags);

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
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
}
