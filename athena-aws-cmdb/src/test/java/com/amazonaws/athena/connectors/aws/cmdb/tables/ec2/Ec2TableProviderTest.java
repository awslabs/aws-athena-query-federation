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
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.EbsInstanceBlockDevice;
import software.amazon.awssdk.services.ec2.model.GroupIdentifier;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceBlockDeviceMapping;
import software.amazon.awssdk.services.ec2.model.InstanceNetworkInterface;
import software.amazon.awssdk.services.ec2.model.InstanceState;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.StateReason;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class Ec2TableProviderTest
        extends AbstractTableProviderTest
{
    private static final Map<String, List<String>> EC2_SUCCESS_FIELDS;
    static {
        EC2_SUCCESS_FIELDS = Map.of("state", Arrays.asList("name", "code"), "network_interfaces", Arrays.asList("status", "subnet", "vpc", "mac", "private_dns", "private_ip", "security_groups", "interface_id"), "state_reason", Arrays.asList("message", "code"), "tags", Arrays.asList("key", "value"));
    }

    @Mock
    private Ec2Client mockEc2;

    protected String getIdField()
    {
        return "instance_id";
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
        return "ec2_instances";
    }

    protected int getExpectedRows()
    {
        return 4;
    }

    protected TableProvider setUpSource()
    {
        return new Ec2TableProvider(mockEc2);
    }

    @Override
    protected void setUpRead()
    {
        when(mockEc2.describeInstances(nullable(DescribeInstancesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            DescribeInstancesRequest request = (DescribeInstancesRequest) invocation.getArguments()[0];

            assertEquals(getIdValue(), request.instanceIds().get(0));
            List<Reservation> reservations = new ArrayList<>();
            reservations.add(makeReservation());
            reservations.add(makeReservation());
            return DescribeInstancesResponse.builder().reservations(reservations).build();
        });
    }

    private Reservation makeReservation()
    {
        List<Instance> instances = new ArrayList<>();
        instances.add(makeInstance(getIdValue()));
        instances.add(makeInstance(getIdValue()));
        instances.add(makeInstance("non-matching-id"));
        return Reservation.builder().instances(instances).build();
    }

    private Instance makeInstance(String id)
    {
        Instance.Builder instance = Instance.builder()
                .instanceId(id)
                .imageId("image_id")
                .instanceType("instance_type")
                .platform("platform")
                .privateDnsName("private_dns_name")
                .privateIpAddress("private_ip_address")
                .publicDnsName("public_dns_name")
                .publicIpAddress("public_ip_address")
                .subnetId("subnet_id")
                .vpcId("vpc_id")
                .architecture("architecture")
                .instanceLifecycle("instance_lifecycle")
                .rootDeviceName("root_device_name")
                .rootDeviceType("root_device_type")
                .spotInstanceRequestId("spot_instance_request_id")
                .virtualizationType("virtualization_type")
                .keyName("key_name")
                .kernelId("kernel_id")
                .capacityReservationId("capacity_reservation_id")
                .launchTime(new Date(100_000).toInstant())
                .state(InstanceState.builder().code(100).name("name").build())
                .stateReason(StateReason.builder().code("code").message("message").build())
                .ebsOptimized(true)
                .tags(Tag.builder().key("key").value("value").build());

        List<InstanceNetworkInterface> interfaces = new ArrayList<>();
        interfaces.add(InstanceNetworkInterface.builder()
                .status("status")
                .subnetId("subnet")
                .vpcId("vpc")
                .macAddress("mac_address")
                .privateDnsName("private_dns")
                .privateIpAddress("private_ip")
                .networkInterfaceId("interface_id")
                .groups(GroupIdentifier.builder().groupId("group_id").groupName("group_name").build()).build());

        interfaces.add(InstanceNetworkInterface.builder()
                .status("status")
                .subnetId("subnet")
                .vpcId("vpc")
                .macAddress("mac")
                .privateDnsName("private_dns")
                .privateIpAddress("private_ip")
                .networkInterfaceId("interface_id")
                .groups(GroupIdentifier.builder().groupId("group_id").groupName("group_name").build()).build());

        instance.networkInterfaces(interfaces)
                .securityGroups(GroupIdentifier.builder().groupId("group_id").groupName("group_name").build())
                .blockDeviceMappings(InstanceBlockDeviceMapping.builder().deviceName("device_name").ebs(EbsInstanceBlockDevice.builder().volumeId("volume_id").build()).build());

        return instance.build();
    }

    @Test
    public void readWithConstraint_whenBlockInvokesStateResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runEc2ThrowTestForTrigger("state");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesNetworkInterfacesResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runEc2ThrowTestForTrigger("network_interfaces");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesStateReasonResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runEc2ThrowTestForTrigger("state_reason");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesTagsResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runEc2ThrowTestForTrigger("tags");
    }

    private void invokeEc2ResolverSuccess(FieldResolver resolver, Object value, String fieldName)
    {
        List<String> names = EC2_SUCCESS_FIELDS.get(fieldName);
        if (names == null) {
            return;
        }
        Object elem = value;
        if (value instanceof List && !((List<?>) value).isEmpty()) {
            elem = ((List<?>) value).get(0);
        }
        for (String n : names) {
            Field f = mock(Field.class);
            when(f.getName()).thenReturn(n);
            resolver.getFieldValue(f, elem);
        }
    }

    private void runEc2ThrowTestForTrigger(String triggerFieldName) throws Exception
    {
        reset(mockEc2);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            Ec2TableProvider provider = (Ec2TableProvider) setUpSource();
            when(mockEc2.describeInstances(any(DescribeInstancesRequest.class)))
                    .thenReturn(DescribeInstancesResponse.builder()
                            .reservations(Collections.singletonList(Reservation.builder().instances(Collections.singletonList(makeInstance("id"))).build()))
                            .build());

            try (Block mockBlock = mock(Block.class);
                    QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
                when(mockBlock.offerValue(anyString(), anyInt(), nullable(Object.class))).thenReturn(true);
                when(mockBlock.offerComplexValue(anyString(), anyInt(), any(FieldResolver.class), nullable(Object.class)))
                        .thenAnswer(invocation -> {
                            String fieldName = invocation.getArgument(0);
                            FieldResolver resolver = invocation.getArgument(2);
                            Object value = invocation.getArgument(3);
                            if (fieldName.equals(triggerFieldName)) {
                                Object resolverValue = value;
                                if (value instanceof List && !((List<?>) value).isEmpty()) {
                                    resolverValue = ((List<?>) value).get(0);
                                }
                                Field unexpectedField = mock(Field.class);
                                when(unexpectedField.getName()).thenReturn("unexpected_field");
                                resolver.getFieldValue(unexpectedField, resolverValue);
                                return true;
                            }
                            invokeEc2ResolverSuccess(resolver, value, fieldName);
                            return true;
                        });

                BlockSpiller mockSpiller = mock(BlockSpiller.class);
                doAnswer(invocation -> {
                    BlockWriter.RowWriter rowWriter = invocation.getArgument(0);
                    rowWriter.writeRows(mockBlock, 0);
                    return null;
                }).when(mockSpiller).writeRows(any(BlockWriter.RowWriter.class));

                Map<String, ValueSet> constraintsMap = new HashMap<>();
                constraintsMap.put("instance_id",
                        EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false).add("id").build());
                ReadRecordsRequest request = new ReadRecordsRequest(
                        new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()),
                        "catalog", "queryId", new TableName(getExpectedSchema(), getExpectedTable()),
                        provider.getTable(allocator, new GetTableRequest(
                                new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()),
                                "queryId", "catalog", new TableName(getExpectedSchema(), getExpectedTable()), Collections.emptyMap())).getSchema(),
                        Split.newBuilder(
                                S3SpillLocation.newBuilder().withBucket("b").withPrefix("p").withSplitId("s").withQueryId("q").withIsDirectory(true).build(),
                                new LocalKeyFactory().create()).build(),
                        new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                        100_000, 100_000);

                lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

                RuntimeException ex = assertThrows(RuntimeException.class, () ->
                        provider.readWithConstraint(mockSpiller, request, mockChecker));
                assertTrue("Exception message should contain Unknown field or Unexpected field",
                        ex.getMessage() != null && (ex.getMessage().contains("Unknown field") || ex.getMessage().contains("Unexpected field")));
            }
        }
    }
}
