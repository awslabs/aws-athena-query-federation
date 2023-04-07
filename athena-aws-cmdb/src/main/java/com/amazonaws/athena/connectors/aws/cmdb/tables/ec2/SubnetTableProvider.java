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
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeSubnetsResult;
import com.amazonaws.services.ec2.model.Subnet;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

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
    private AmazonEC2 ec2;

    public SubnetTableProvider(AmazonEC2 ec2)
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
        return TableName.newBuilder().setSchemaName(getSchema()).setTableName("subnets").build();
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
     * Calls DescribeSubnets on the AWS EC2 Client returning all subnets that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific subnet) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockAllocator allocator, BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        DescribeSubnetsRequest request = new DescribeSubnetsRequest();

        ValueSet idConstraint = ProtobufMessageConverter.fromProtoConstraints(allocator, recordsRequest.getConstraints()).getSummary().get("id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setSubnetIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        DescribeSubnetsResult response = ec2.describeSubnets(request);
        for (Subnet subnet : response.getSubnets()) {
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

            matched &= block.offerValue("id", row, subnet.getSubnetId());
            matched &= block.offerValue("availability_zone", row, subnet.getAvailabilityZone());
            matched &= block.offerValue("available_ip_count", row, subnet.getAvailableIpAddressCount());
            matched &= block.offerValue("cidr_block", row, subnet.getCidrBlock());
            matched &= block.offerValue("default_for_az", row, subnet.getDefaultForAz());
            matched &= block.offerValue("map_public_ip", row, subnet.getMapPublicIpOnLaunch());
            matched &= block.offerValue("owner", row, subnet.getOwnerId());
            matched &= block.offerValue("state", row, subnet.getState());
            matched &= block.offerValue("vpc", row, subnet.getVpcId());
            matched &= block.offerValue("vpc", row, subnet.getVpcId());

            List<String> tags = subnet.getTags().stream()
                    .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
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
