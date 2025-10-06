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
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcsResponse;
import software.amazon.awssdk.services.ec2.model.Vpc;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps your VPCs to a table.
 */
public class VpcTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private Ec2Client ec2;

    public VpcTableProvider(Ec2Client ec2)
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
        return new TableName(getSchema(), "vpcs");
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
     * Calls DescribeVPCs on the AWS EC2 Client returning all VPCs that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific VPCs) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
    {
        DescribeVpcsRequest.Builder request = DescribeVpcsRequest.builder();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.vpcIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        DescribeVpcsResponse response = ec2.describeVpcs(request.build());
        for (Vpc vpc : response.vpcs()) {
            instanceToRow(vpc, spiller);
        }
    }

    /**
     * Maps a VPC into a row in our Apache Arrow response block(s).
     *
     * @param vpc The VPCs to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(Vpc vpc,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("id", row, vpc.vpcId());
            matched &= block.offerValue("cidr_block", row, vpc.cidrBlock());
            matched &= block.offerValue("dhcp_opts", row, vpc.dhcpOptionsId());
            matched &= block.offerValue("tenancy", row, vpc.instanceTenancyAsString());
            matched &= block.offerValue("owner", row, vpc.ownerId());
            matched &= block.offerValue("state", row, vpc.stateAsString());
            matched &= block.offerValue("is_default", row, vpc.isDefault());

            List<String> tags = vpc.tags().stream()
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
}
