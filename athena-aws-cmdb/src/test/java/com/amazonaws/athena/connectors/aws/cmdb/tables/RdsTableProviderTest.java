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
package com.amazonaws.athena.connectors.aws.cmdb.tables;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DBInstanceStatusInfo;
import software.amazon.awssdk.services.rds.model.DBParameterGroupStatus;
import software.amazon.awssdk.services.rds.model.DBSecurityGroupMembership;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.Endpoint;
import software.amazon.awssdk.services.rds.model.Subnet;
import software.amazon.awssdk.services.rds.model.Tag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RdsTableProviderTest
        extends AbstractTableProviderTest
{
    @Mock
    private RdsClient mockRds;

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
        return "rds";
    }

    protected String getExpectedTable()
    {
        return "rds_instances";
    }

    protected int getExpectedRows()
    {
        return 6;
    }

    protected TableProvider setUpSource()
    {
        return new RdsTableProvider(mockRds);
    }

    @Override
    protected void setUpRead()
    {
        final AtomicLong requestCount = new AtomicLong(0);
        when(mockRds.describeDBInstances(nullable(DescribeDbInstancesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    List<DBInstance> values = new ArrayList<>();
                    values.add(makeValue(getIdValue()));
                    values.add(makeValue(getIdValue()));
                    values.add(makeValue("fake-id"));
                    DescribeDbInstancesResponse.Builder resultBuilder = DescribeDbInstancesResponse.builder();
                    resultBuilder.dbInstances(values);

                    if (requestCount.incrementAndGet() < 3) {
                        resultBuilder.marker(String.valueOf(requestCount.get()));
                    }
                    return resultBuilder.build();
                });
    }

    private DBInstance makeValue(String id)
    {
        return DBInstance.builder()
                .dbInstanceIdentifier(id)
                .availabilityZone("primary_az")
                .allocatedStorage(100)
                .storageEncrypted(true)
                .backupRetentionPeriod(100)
                .autoMinorVersionUpgrade(true)
                .dbInstanceClass("instance_class")
                .dbInstancePort(100)
                .dbInstanceStatus("status")
                .storageType("storage_type")
                .dbiResourceId("dbi_resource_id")
                .dbName("name")
                .domainMemberships(DomainMembership.builder()
                        .domain("domain")
                        .fqdn("fqdn")
                        .iamRoleName("iam_role")
                        .status("status")
                        .build())
                .engine("engine")
                .engineVersion("engine_version")
                .licenseModel("license_model")
                .secondaryAvailabilityZone("secondary_az")
                .preferredBackupWindow("backup_window")
                .preferredMaintenanceWindow("maint_window")
                .readReplicaSourceDBInstanceIdentifier("read_replica_source_id")
                .dbParameterGroups(DBParameterGroupStatus.builder()
                        .dbParameterGroupName("name")
                        .parameterApplyStatus("status")
                        .build())
                .dbSecurityGroups(DBSecurityGroupMembership.builder()
                        .dbSecurityGroupName("name")
                        .status("status").build())
                .dbSubnetGroup(DBSubnetGroup.builder()
                        .dbSubnetGroupName("name")
                        .subnetGroupStatus("status")
                        .vpcId("vpc")
                        .subnets(Subnet.builder().subnetIdentifier("subnet").build())
                        .build())
                .statusInfos(DBInstanceStatusInfo.builder()
                        .status("status")
                        .message("message")
                        .normal(true)
                        .statusType("type")
                        .build())
                .endpoint(Endpoint.builder()
                        .address("address")
                        .port(100)
                        .hostedZoneId("zone")
                        .build())
                .instanceCreateTime(new Date(100000).toInstant())
                .iops(100)
                .multiAZ(true)
                .publiclyAccessible(true)
                .tagList(Tag.builder().key("key").value("value").build())
                .build();
    }

    @Test
    public void readWithConstraint_whenNoInstanceIdConstraint_doesNotSetDbInstanceIdentifier()
    {
        reset(mockRds);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            RdsTableProvider provider = (RdsTableProvider) setUpSource();
            when(mockRds.describeDBInstances(any(DescribeDbInstancesRequest.class)))
                    .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(Collections.emptyList()).build());

            ReadRecordsRequest request = createReadRecordsRequest(allocator, provider, Collections.emptyMap());

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            provider.readWithConstraint(mockSpiller, request, mockChecker);

            ArgumentCaptor<DescribeDbInstancesRequest> captor = ArgumentCaptor.forClass(DescribeDbInstancesRequest.class);
            verify(mockRds).describeDBInstances(captor.capture());
            assertNull("dbInstanceIdentifier should not be set when no instance_id constraint",
                    captor.getValue().dbInstanceIdentifier());
        }
    }

    @Test
    public void readWithConstraint_whenInstanceIdMultiValue_doesNotSetDbInstanceIdentifier()
    {
        reset(mockRds);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            RdsTableProvider provider = (RdsTableProvider) setUpSource();
            when(mockRds.describeDBInstances(any(DescribeDbInstancesRequest.class)))
                    .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(Collections.emptyList()).build());

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("instance_id",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                            .add("id1").add("id2").build());

            ReadRecordsRequest request = createReadRecordsRequest(allocator, provider, constraintsMap);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            provider.readWithConstraint(mockSpiller, request, mockChecker);

            ArgumentCaptor<DescribeDbInstancesRequest> captor = ArgumentCaptor.forClass(DescribeDbInstancesRequest.class);
            verify(mockRds).describeDBInstances(captor.capture());
            assertNull("dbInstanceIdentifier should not be set when instance_id has multiple values",
                    captor.getValue().dbInstanceIdentifier());
        }
    }

    @Test
    public void readWithConstraint_whenPaginationAndQueryNotRunning_exitsLoopWithoutSecondPage()
    {
        reset(mockRds);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            RdsTableProvider provider = (RdsTableProvider) setUpSource();
            AtomicInteger callCount = new AtomicInteger(0);
            when(mockRds.describeDBInstances(any(DescribeDbInstancesRequest.class)))
                    .thenAnswer((Answer<DescribeDbInstancesResponse>) invocation -> {
                        int count = callCount.incrementAndGet();
                        List<DBInstance> instances = Collections.singletonList(makeValue("id1"));
                        DescribeDbInstancesResponse.Builder builder = DescribeDbInstancesResponse.builder().dbInstances(instances);
                        if (count == 1) {
                            builder.marker("next-page");
                        }
                        return builder.build();
                    });

            QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);
            when(mockChecker.isQueryRunning()).thenReturn(false);

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("instance_id",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false).add("id1").build());

            ReadRecordsRequest request = createReadRecordsRequest(allocator, provider, constraintsMap);

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            provider.readWithConstraint(mockSpiller, request, mockChecker);

            assertEquals("describeDBInstances should be called once; loop exits when isQueryRunning returns false",
                    1, callCount.get());
        }
    }

    @Test
    public void readWithConstraint_whenBlockInvokesResolverWithUnexpectedField_throwsRuntimeException() throws Exception {
        reset(mockRds);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            RdsTableProvider provider = (RdsTableProvider) setUpSource();
            when(mockRds.describeDBInstances(any(DescribeDbInstancesRequest.class)))
                    .thenReturn(DescribeDbInstancesResponse.builder()
                            .dbInstances(Collections.singletonList(makeValue("id")))
                            .build());

            try (Block mockBlock = mock(Block.class);
                    QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
                when(mockBlock.offerValue(anyString(), anyInt(), nullable(Object.class))).thenReturn(true);
                when(mockBlock.offerComplexValue(anyString(), anyInt(), any(FieldResolver.class), nullable(Object.class)))
                        .thenAnswer(invocation -> {
                            FieldResolver resolver = invocation.getArgument(2);
                            Object value = invocation.getArgument(3);
                            Field unexpectedField = mock(Field.class);
                            when(unexpectedField.getName()).thenReturn("unexpected_field");
                            resolver.getFieldValue(unexpectedField, value);
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

                ReadRecordsRequest request = createReadRecordsRequest(allocator, provider, constraintsMap);

                lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

                RuntimeException ex = assertThrows(RuntimeException.class, () ->
                        provider.readWithConstraint(mockSpiller, request, mockChecker));
                assertTrue("Exception message should contain Unexpected field",
                        ex.getMessage() != null && ex.getMessage().contains("Unexpected field"));
            }
        }
    }

    @Test
    public void readWithConstraint_whenBlockInvokesSubnetGroupResolverWithDescription_returnsDescription() throws Exception {
        reset(mockRds);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            DBSubnetGroup subnetGroup = DBSubnetGroup.builder()
                    .dbSubnetGroupDescription("test-description")
                    .dbSubnetGroupName("name")
                    .subnetGroupStatus("status")
                    .vpcId("vpc")
                    .subnets(Subnet.builder().subnetIdentifier("subnet").build())
                    .build();
            DBInstance instanceWithDescription = makeValue("id").toBuilder()
                    .dbSubnetGroup(subnetGroup)
                    .build();

            RdsTableProvider provider = (RdsTableProvider) setUpSource();
            when(mockRds.describeDBInstances(any(DescribeDbInstancesRequest.class)))
                    .thenReturn(DescribeDbInstancesResponse.builder()
                            .dbInstances(Collections.singletonList(instanceWithDescription))
                            .build());

            final AtomicInteger callCount = new AtomicInteger(0);
            try (Block mockBlock = mock(Block.class);
                    QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
                when(mockBlock.offerValue(anyString(), anyInt(), nullable(Object.class))).thenReturn(true);
                when(mockBlock.offerComplexValue(anyString(), anyInt(), any(FieldResolver.class), nullable(Object.class)))
                        .thenAnswer(invocation -> {
                            String fieldName = invocation.getArgument(0);
                            FieldResolver resolver = invocation.getArgument(2);
                            Object value = invocation.getArgument(3);
                            if ("subnet_group".equals(fieldName) && value instanceof DBSubnetGroup) {
                                Field descField = mock(Field.class);
                                when(descField.getName()).thenReturn("description");
                                Object result = resolver.getFieldValue(descField, value);
                                assertNotNull("Description should be resolved", result);
                                assertEquals("test-description", result);
                            }
                            callCount.incrementAndGet();
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

                ReadRecordsRequest request = createReadRecordsRequest(allocator, provider, constraintsMap);

                lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

                provider.readWithConstraint(mockSpiller, request, mockChecker);

                assertTrue("subnet_group resolver should have been invoked", callCount.get() > 0);
            }
        }
    }

    @Test
    public void readWithConstraint_whenBlockInvokesSubnetGroupResolverWithSubnetValue_returnsSubnetIdentifier() throws Exception {
        reset(mockRds);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            Subnet subnet = Subnet.builder().subnetIdentifier("subnet-123").build();
            DBSubnetGroup subnetGroup = DBSubnetGroup.builder()
                    .dbSubnetGroupName("name")
                    .subnetGroupStatus("status")
                    .vpcId("vpc")
                    .subnets(subnet)
                    .build();
            DBInstance instanceWithSubnet = makeValue("id").toBuilder()
                    .dbSubnetGroup(subnetGroup)
                    .build();

            RdsTableProvider provider = (RdsTableProvider) setUpSource();
            when(mockRds.describeDBInstances(any(DescribeDbInstancesRequest.class)))
                    .thenReturn(DescribeDbInstancesResponse.builder()
                            .dbInstances(Collections.singletonList(instanceWithSubnet))
                            .build());

            final AtomicInteger callCount = new AtomicInteger(0);
            try (Block mockBlock = mock(Block.class);
                    QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
                when(mockBlock.offerValue(anyString(), anyInt(), nullable(Object.class))).thenReturn(true);
                when(mockBlock.offerComplexValue(anyString(), anyInt(), any(FieldResolver.class), nullable(Object.class)))
                        .thenAnswer(invocation -> {
                            String fieldName = invocation.getArgument(0);
                            FieldResolver resolver = invocation.getArgument(2);
                            Object value = invocation.getArgument(3);
                        if ("subnet_group".equals(fieldName) && value instanceof DBSubnetGroup) {
                            DBSubnetGroup group = (DBSubnetGroup) value;
                            if (!group.subnets().isEmpty()) {
                                Field subnetField = mock(Field.class);
                                when(subnetField.getName()).thenReturn("subnet_item");
                                Object result = resolver.getFieldValue(subnetField, group.subnets().get(0));
                                    assertNotNull("Subnet identifier should be resolved", result);
                                    assertEquals("subnet-123", result.toString());
                                }
                            }
                            callCount.incrementAndGet();
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

                ReadRecordsRequest request = createReadRecordsRequest(allocator, provider, constraintsMap);

                lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

                provider.readWithConstraint(mockSpiller, request, mockChecker);

                assertTrue("subnet_group resolver should have been invoked", callCount.get() > 0);
            }
        }
    }

    @Test
    public void readWithConstraint_whenBlockInvokesParamGroupsResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runRdsThrowTestForTrigger("param_groups");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesDbSecurityGroupsResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runRdsThrowTestForTrigger("db_security_groups");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesSubnetGroupResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runRdsThrowTestForTrigger("subnet_group");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesEndpointResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runRdsThrowTestForTrigger("endpoint");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesStatusInfosResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runRdsThrowTestForTrigger("status_infos");
    }

    @Test
    public void readWithConstraint_whenBlockInvokesTagsResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        runRdsThrowTestForTrigger("tags");
    }

    private ReadRecordsRequest createReadRecordsRequest(BlockAllocatorImpl allocator, RdsTableProvider provider,
                                                        java.util.Map<String, com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet> constraintsMap)
    {
        return new ReadRecordsRequest(
                new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()),
                "catalog", "queryId", new TableName("rds", "rds_instances"),
                provider.getTable(allocator, new GetTableRequest(
                        new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap()),
                        "queryId", "catalog", new TableName("rds", "rds_instances"), Collections.emptyMap())).getSchema(),
                Split.newBuilder(
                        S3SpillLocation.newBuilder().withBucket("b").withPrefix("p").withSplitId("s").withQueryId("q").withIsDirectory(true).build(),
                        new LocalKeyFactory().create()).build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000, 100_000);
    }

    private static final Map<String, List<String>> RDS_SUCCESS_FIELDS = new HashMap<>();
    static {
        RDS_SUCCESS_FIELDS.put("domains", Arrays.asList("domain", "fqdn", "iam_role", "status"));
        RDS_SUCCESS_FIELDS.put("param_groups", Arrays.asList("name", "status"));
        RDS_SUCCESS_FIELDS.put("db_security_groups", Arrays.asList("name", "status"));
        RDS_SUCCESS_FIELDS.put("subnet_group", Arrays.asList("description", "name", "status", "vpc", "subnets"));
        RDS_SUCCESS_FIELDS.put("endpoint", Arrays.asList("address", "port", "zone"));
        RDS_SUCCESS_FIELDS.put("status_infos", Arrays.asList("message", "is_normal", "status", "type"));
        RDS_SUCCESS_FIELDS.put("tags", Arrays.asList("key", "value"));
    }

    private void invokeResolverSuccess(FieldResolver resolver, Object value, String fieldName)
    {
        List<String> names = RDS_SUCCESS_FIELDS.get(fieldName);
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

    private void runRdsThrowTestForTrigger(String triggerFieldName) throws Exception
    {
        reset(mockRds);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            RdsTableProvider provider = (RdsTableProvider) setUpSource();
            when(mockRds.describeDBInstances(any(DescribeDbInstancesRequest.class)))
                    .thenReturn(DescribeDbInstancesResponse.builder()
                            .dbInstances(Collections.singletonList(makeValue("id")))
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
                                Field unexpectedField = mock(Field.class);
                                when(unexpectedField.getName()).thenReturn("unexpected_field");
                                resolver.getFieldValue(unexpectedField, value);
                                return true;
                            }
                            invokeResolverSuccess(resolver, value, fieldName);
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
                ReadRecordsRequest request = createReadRecordsRequest(allocator, provider, constraintsMap);

                lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

                RuntimeException ex = assertThrows(RuntimeException.class, () ->
                        provider.readWithConstraint(mockSpiller, request, mockChecker));
                assertTrue("Exception message should contain Unexpected field",
                        ex.getMessage() != null && ex.getMessage().contains("Unexpected field"));
            }
        }
    }
}
