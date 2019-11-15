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
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.SecurityGroup;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps your EC2 SecurityGroups to a table.
 */
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
        return new TableName(getSchema(), "security_groups");
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
     * Calls DescribeSecurityGroups on the AWS EC2 Client returning all SecurityGroup rules that match the supplied
     * predicate and attempting to push down certain predicates (namely queries for specific SecurityGroups) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
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

            //Each rule is mapped to a row in the response. SGs have INGRESS and EGRESS rules.
            for (SecurityGroup next : response.getSecurityGroups()) {
                for (IpPermission nextPerm : next.getIpPermissions()) {
                    instanceToRow(next, nextPerm, INGRESS, spiller);
                }

                for (IpPermission nextPerm : next.getIpPermissionsEgress()) {
                    instanceToRow(next, nextPerm, EGRESS, spiller);
                }
            }

            request.setNextToken(response.getNextToken());
            if (response.getNextToken() == null || !queryStatusChecker.isQueryRunning()) {
                done = true;
            }
        }
    }

    /**
     * Maps an each SecurityGroup rule (aka IpPermission) to a row in the response.
     *
     * @param securityGroup The SecurityGroup that owns the permission entry.
     * @param permission The permission entry (aka rule) to map.
     * @param direction The direction (EGRESS or INGRESS) of the rule.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(SecurityGroup securityGroup,
            IpPermission permission,
            String direction,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("id", row, securityGroup.getGroupId());
            matched &= block.offerValue("name", row, securityGroup.getGroupName());
            matched &= block.offerValue("description", row, securityGroup.getDescription());
            matched &= block.offerValue("from_port", row, permission.getFromPort());
            matched &= block.offerValue("to_port", row, permission.getFromPort());
            matched &= block.offerValue("protocol", row, permission.getIpProtocol());
            matched &= block.offerValue("direction", row, permission.getIpProtocol());

            List<String> ipv4Ranges = permission.getIpv4Ranges().stream()
                    .map(next -> next.getCidrIp() + ":" + next.getDescription()).collect(Collectors.toList());
            matched &= block.offerComplexValue("ipv4_ranges", row, FieldResolver.DEFAULT, ipv4Ranges);

            List<String> ipv6Ranges = permission.getIpv6Ranges().stream()
                    .map(next -> next.getCidrIpv6() + ":" + next.getDescription()).collect(Collectors.toList());
            matched &= block.offerComplexValue("ipv6_ranges", row, FieldResolver.DEFAULT, ipv6Ranges);

            List<String> prefixLists = permission.getPrefixListIds().stream()
                    .map(next -> next.getPrefixListId() + ":" + next.getDescription()).collect(Collectors.toList());
            matched &= block.offerComplexValue("prefix_lists", row, FieldResolver.DEFAULT, prefixLists);

            List<String> userIdGroups = permission.getUserIdGroupPairs().stream()
                    .map(next -> next.getUserId() + ":" + next.getGroupId())
                    .collect(Collectors.toList());
            matched &= block.offerComplexValue("user_id_groups", row, FieldResolver.DEFAULT, userIdGroups);

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
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
                .addListField("prefix_lists", Types.MinorType.VARCHAR.getType())
                .addListField("user_id_groups", Types.MinorType.VARCHAR.getType())
                .addMetadata("id", "Security Group ID.")
                .addMetadata("name", "Name of the security group.")
                .addMetadata("description", "Description of the security group.")
                .addMetadata("from_port", "Beginging of the port range covered by this security group.")
                .addMetadata("to_port", "Ending of the port range covered by this security group.")
                .addMetadata("protocol", "The network protocol covered by this security group.")
                .addMetadata("direction", "Notes if the rule applies inbound (ingress) or outbound (egress).")
                .addMetadata("ipv4_ranges", "The ip v4 ranges covered by this security group.")
                .addMetadata("ipv6_ranges", "The ip v6 ranges covered by this security group.")
                .addMetadata("prefix_lists", "The prefix lists covered by this security group.")
                .addMetadata("user_id_groups", "The user id groups covered by this security group.")
                .build();
    }
}
