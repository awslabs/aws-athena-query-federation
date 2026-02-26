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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.apache.arrow.vector.types.Types;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.IpPermission;
import software.amazon.awssdk.services.ec2.model.IpRange;
import software.amazon.awssdk.services.ec2.model.Ipv6Range;
import software.amazon.awssdk.services.ec2.model.PrefixListId;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.ec2.model.UserIdGroupPair;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;

public class SecurityGroupsTableProviderTest
        extends AbstractTableProviderTest
{

    @Mock
    private Ec2Client mockEc2;

    protected String getIdField()
    {
        return "id";
    }

    protected String getIdValue()
    {
        return "123";
    }

    protected String getExpectedSchema()
    {
        return "ec2";
    }

    protected String getExpectedTable()
    {
        return "security_groups";
    }

    protected int getExpectedRows()
    {
        return 2;
    }

    @Override
    protected boolean directionColumnNotNullOnly()
    {
        return true;
    }

    protected TableProvider setUpSource()
    {
        return new SecurityGroupsTableProvider(mockEc2);
    }

    @Override
    protected void setUpRead()
    {
        when(mockEc2.describeSecurityGroups(nullable(DescribeSecurityGroupsRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    DescribeSecurityGroupsRequest request = (DescribeSecurityGroupsRequest) invocation.getArguments()[0];

                    assertEquals(getIdValue(), request.groupIds().get(0));
                    List<SecurityGroup> values = new ArrayList<>();
                    values.add(makeSecurityGroup(getIdValue()));
                    values.add(makeSecurityGroup(getIdValue()));
                    values.add(makeSecurityGroup("fake-id"));
                    return DescribeSecurityGroupsResponse.builder().securityGroups(values).build();
                });
    }

    private SecurityGroup makeSecurityGroup(String id)
    {
        return SecurityGroup.builder()
                .groupId(id)
                .groupName("name")
                .description("description")
                .ipPermissions(IpPermission.builder()
                        .ipProtocol("protocol")
                        .fromPort(100)
                        .toPort(100)
                        .ipRanges(IpRange.builder().cidrIp("cidr").description("description").build())
                        .ipv6Ranges(Ipv6Range.builder().cidrIpv6("cidr").description("description").build())
                        .prefixListIds(PrefixListId.builder().prefixListId("prefix").description("description").build())
                        .userIdGroupPairs(UserIdGroupPair.builder().groupId("group_id").userId("user_id").build()).build()
                ).build();
    }

    @Test
    public void readWithConstraint_whenNameConstraintSingleValue_usesGroupNames() {
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            SecurityGroupsTableProvider provider = new SecurityGroupsTableProvider(mockEc2);
            String nameValue = "my-sg-name";
            when(mockEc2.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
                    .thenReturn(DescribeSecurityGroupsResponse.builder()
                            .securityGroups(Collections.singletonList(makeSecurityGroup("sg-123")))
                            .build());

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("name",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false).add(nameValue).build());
            ReadRecordsRequest request = buildReadRequest(allocator, provider, constraintsMap);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            provider.readWithConstraint(mockSpiller, request, mockChecker);

            ArgumentCaptor<DescribeSecurityGroupsRequest> captor = ArgumentCaptor.forClass(DescribeSecurityGroupsRequest.class);
            verify(mockEc2).describeSecurityGroups(captor.capture());
            assertEquals("Request should use groupNames for name constraint", Collections.singletonList(nameValue), captor.getValue().groupNames());
        }
    }

    @Test
    public void readWithConstraint_whenSecurityGroupHasEgressPermissions_writesEgressRows() {
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            SecurityGroupsTableProvider provider = new SecurityGroupsTableProvider(mockEc2);
            IpPermission egressPerm = IpPermission.builder()
                    .ipProtocol("tcp")
                    .fromPort(443)
                    .toPort(443)
                    .ipRanges(IpRange.builder().cidrIp("0.0.0.0/0").build())
                    .build();
            SecurityGroup sgWithEgress = makeSecurityGroup("sg-egress").toBuilder()
                    .ipPermissionsEgress(Collections.singletonList(egressPerm))
                    .build();
            when(mockEc2.describeSecurityGroups(any(DescribeSecurityGroupsRequest.class)))
                    .thenReturn(DescribeSecurityGroupsResponse.builder()
                            .securityGroups(Collections.singletonList(sgWithEgress))
                            .build());

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("id",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false).add("sg-egress").build());
            ReadRecordsRequest request = buildReadRequest(allocator, provider, constraintsMap);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            provider.readWithConstraint(mockSpiller, request, mockChecker);

            verify(mockEc2).describeSecurityGroups(any(DescribeSecurityGroupsRequest.class));
            verify(mockSpiller, atLeastOnce()).writeRows(any());
        }
    }

    private ReadRecordsRequest buildReadRequest(BlockAllocatorImpl allocator, SecurityGroupsTableProvider provider,
                                                Map<String, ValueSet> constraintsMap)
    {
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        TableName tableName = new TableName(getExpectedSchema(), getExpectedTable());
        FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        return new ReadRecordsRequest(identity, "catalog", "queryId", tableName,
                provider.getTable(allocator, new GetTableRequest(identity,
                        "queryId", "catalog", tableName, Collections.emptyMap())).getSchema(),
                Split.newBuilder(
                        S3SpillLocation.newBuilder().withBucket("b").withPrefix("p").withSplitId("s").withQueryId("q").withIsDirectory(true).build(),
                        new LocalKeyFactory().create()).build(),
                constraints, 100_000, 100_000);
    }
}
