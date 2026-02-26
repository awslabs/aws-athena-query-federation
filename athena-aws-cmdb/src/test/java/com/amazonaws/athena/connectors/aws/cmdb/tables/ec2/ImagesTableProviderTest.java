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
import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.BlockDeviceMapping;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeImagesResponse;
import software.amazon.awssdk.services.ec2.model.EbsBlockDevice;
import software.amazon.awssdk.services.ec2.model.Image;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

public class ImagesTableProviderTest
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
        return "ec2_images";
    }

    protected int getExpectedRows()
    {
        return 2;
    }

    protected TableProvider setUpSource()
    {
        return new ImagesTableProvider(mockEc2, com.google.common.collect.ImmutableMap.of());
    }

    @Override
    protected void setUpRead()
    {
        when(mockEc2.describeImages(nullable(DescribeImagesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            DescribeImagesRequest request = (DescribeImagesRequest) invocation.getArguments()[0];

            assertEquals(getIdValue(), request.imageIds().get(0));
            List<Image> values = new ArrayList<>();
            values.add(makeImage(getIdValue()));
            values.add(makeImage(getIdValue()));
            values.add(makeImage("fake-id"));
            return DescribeImagesResponse.builder().images(values).build();
        });
    }

    private Image makeImage(String id)
    {
        Image image = Image.builder()
                .imageId(id)
                .architecture("architecture")
                .creationDate("created")
                .description("description")
                .hypervisor("hypervisor")
                .imageLocation("location")
                .imageType("type")
                .kernelId("kernel")
                .name("name")
                .ownerId("owner")
                .platform("platform")
                .ramdiskId("ramdisk")
                .rootDeviceName("root_device")
                .rootDeviceType("root_type")
                .sriovNetSupport("srvio_net")
                .state("state")
                .virtualizationType("virt_type")
                .publicLaunchPermissions(true)
                .tags(Tag.builder().key("key").value("value").build())
                .blockDeviceMappings(BlockDeviceMapping.builder()
                        .deviceName("dev_name")
                        .noDevice("no_device")
                        .virtualName("virt_name")
                        .ebs(EbsBlockDevice.builder()
                                .iops(100)
                                .kmsKeyId("ebs_kms_key")
                                .volumeType("ebs_type")
                                .volumeSize(100).build()).build()).build();

        return image;
    }

    @Test
    public void readWithConstraint_whenOwnerConstraintSingleValue_usesOwnerFilter()
    {
        reset(mockEc2);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            ImagesTableProvider provider = (ImagesTableProvider) setUpSource();
            when(mockEc2.describeImages(any(DescribeImagesRequest.class)))
                    .thenReturn(DescribeImagesResponse.builder()
                            .images(Collections.singletonList(makeImage("ami-1")))
                            .build());

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("owner",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                            .add("123456789012").build());

            ReadRecordsRequest request = buildReadRequest(provider, allocator, constraintsMap);
            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            com.amazonaws.athena.connector.lambda.QueryStatusChecker mockChecker = mock(com.amazonaws.athena.connector.lambda.QueryStatusChecker.class);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            provider.readWithConstraint(mockSpiller, request, mockChecker);

            ArgumentCaptor<DescribeImagesRequest> captor = ArgumentCaptor.forClass(DescribeImagesRequest.class);
            verify(mockEc2).describeImages(captor.capture());
            assertNotNull("Request should have owners", captor.getValue().owners());
            assertEquals("Should use owner filter", 1, captor.getValue().owners().size());
            assertEquals("Owner should match", "123456789012", captor.getValue().owners().get(0));
        }
    }

    @Test
    public void readWithConstraint_whenDefaultOwnerSet_usesDefaultOwner()
    {
        reset(mockEc2);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl()) {
            Map<String, String> config = new HashMap<>();
            config.put("default_ec2_image_owner", "111111111111");
            ImagesTableProvider provider = new ImagesTableProvider(mockEc2, config);
            when(mockEc2.describeImages(any(DescribeImagesRequest.class)))
                    .thenReturn(DescribeImagesResponse.builder()
                            .images(Collections.singletonList(makeImage("ami-1")))
                            .build());

            ReadRecordsRequest request = buildReadRequest(provider, allocator, Collections.emptyMap());
            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            provider.readWithConstraint(mockSpiller, request, mockChecker);

            ArgumentCaptor<DescribeImagesRequest> captor = ArgumentCaptor.forClass(DescribeImagesRequest.class);
            verify(mockEc2).describeImages(captor.capture());
            assertNotNull("Request should have owners", captor.getValue().owners());
            assertEquals("Should use exactly one owner filter", 1, captor.getValue().owners().size());
            assertEquals("Should use default owner", "111111111111", captor.getValue().owners().get(0));
        }
    }

    @Test
    public void readWithConstraint_whenNoIdNoOwnerNoDefault_throwsRuntimeException()
    {
        reset(mockEc2);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl();
                QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
            ImagesTableProvider provider = (ImagesTableProvider) setUpSource();
            ReadRecordsRequest request = buildReadRequest(provider, allocator, Collections.emptyMap());
            BlockSpiller mockSpiller = mock(BlockSpiller.class);

            RuntimeException ex = assertThrows(RuntimeException.class, () ->
                    provider.readWithConstraint(mockSpiller, request, mockChecker));
            assertTrue("Exception message should mention default owner or owner filter",
                    ex.getMessage().contains("default owner") || ex.getMessage().contains("owner"));
        }
    }

    @Test
    public void readWithConstraint_whenMoreThanMaxImages_throwsRuntimeException()
    {
        reset(mockEc2);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl();
                QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
            ImagesTableProvider provider = (ImagesTableProvider) setUpSource();
            List<Image> tooMany = new ArrayList<>();
            for (int i = 0; i < 1002; i++) {
                tooMany.add(makeImage("ami-" + i));
            }
            when(mockEc2.describeImages(any(DescribeImagesRequest.class)))
                    .thenReturn(DescribeImagesResponse.builder().images(tooMany).build());

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("id",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                            .add("ami-0").build());

            ReadRecordsRequest request = buildReadRequest(provider, allocator, constraintsMap);
            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            RuntimeException ex = assertThrows(RuntimeException.class, () ->
                    provider.readWithConstraint(mockSpiller, request, mockChecker));
            assertTrue("Exception message should mention too many images",
                    ex.getMessage() != null && ex.getMessage().contains("Too many images"));
        }
    }

    @Test
    public void readWithConstraint_whenBlockInvokesTagsResolverWithUnexpectedField_throwsRuntimeException() throws Exception {
        reset(mockEc2);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl();
                Block mockBlock = mock(Block.class);
                QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
            ImagesTableProvider provider = (ImagesTableProvider) setUpSource();
            when(mockEc2.describeImages(any(DescribeImagesRequest.class)))
                    .thenReturn(DescribeImagesResponse.builder()
                            .images(Collections.singletonList(makeImage("ami-1")))
                            .build());

            when(mockBlock.offerValue(anyString(), anyInt(), nullable(Object.class))).thenReturn(true);
            when(mockBlock.offerComplexValue(anyString(), anyInt(), any(FieldResolver.class), nullable(Object.class)))
                    .thenAnswer(invocation -> {
                        String fieldName = invocation.getArgument(0);
                        FieldResolver resolver = invocation.getArgument(2);
                        Object value = invocation.getArgument(3);
                        if ("tags".equals(fieldName)) {
                            Field unexpectedField = mock(Field.class);
                            when(unexpectedField.getName()).thenReturn("unexpected_field");
                            resolver.getFieldValue(unexpectedField, value);
                        }
                        return true;
                    });

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            doAnswer(invocation -> {
                BlockWriter.RowWriter rowWriter = invocation.getArgument(0);
                rowWriter.writeRows(mockBlock, 0);
                return null;
            }).when(mockSpiller).writeRows(any(BlockWriter.RowWriter.class));

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("id",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false).add("ami-1").build());
            ReadRecordsRequest request = buildReadRequest(provider, allocator, constraintsMap);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            RuntimeException ex = assertThrows(RuntimeException.class, () ->
                    provider.readWithConstraint(mockSpiller, request, mockChecker));
            assertTrue("Exception message should contain Unexpected field",
                    ex.getMessage() != null && ex.getMessage().contains("Unexpected field"));
        }
    }

    @Test
    public void readWithConstraint_whenBlockInvokesBlockDevicesResolverWithUnexpectedField_throwsRuntimeException() throws Exception
    {
        reset(mockEc2);
        try (BlockAllocatorImpl allocator = new BlockAllocatorImpl();
                Block mockBlock = mock(Block.class);
                QueryStatusChecker mockChecker = mock(QueryStatusChecker.class)) {
            ImagesTableProvider provider = (ImagesTableProvider) setUpSource();
            when(mockEc2.describeImages(any(DescribeImagesRequest.class)))
                    .thenReturn(DescribeImagesResponse.builder()
                            .images(Collections.singletonList(makeImage("ami-1")))
                            .build());

            when(mockBlock.offerValue(anyString(), anyInt(), nullable(Object.class))).thenReturn(true);
            when(mockBlock.offerComplexValue(anyString(), anyInt(), any(FieldResolver.class), nullable(Object.class)))
                    .thenAnswer(invocation -> {
                        String fieldName = invocation.getArgument(0);
                        FieldResolver resolver = invocation.getArgument(2);
                        Object value = invocation.getArgument(3);
                        if ("block_devices".equals(fieldName)) {
                            Field unexpectedField = mock(Field.class);
                            when(unexpectedField.getName()).thenReturn("unexpected_field");
                            resolver.getFieldValue(unexpectedField, value);
                        }
                        return true;
                    });

            BlockSpiller mockSpiller = mock(BlockSpiller.class);
            doAnswer(invocation -> {
                BlockWriter.RowWriter rowWriter = invocation.getArgument(0);
                rowWriter.writeRows(mockBlock, 0);
                return null;
            }).when(mockSpiller).writeRows(any(BlockWriter.RowWriter.class));

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("id",
                    EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false).add("ami-1").build());
            ReadRecordsRequest request = buildReadRequest(provider, allocator, constraintsMap);
            lenient().when(mockChecker.isQueryRunning()).thenReturn(true);

            RuntimeException ex = assertThrows(RuntimeException.class, () ->
                    provider.readWithConstraint(mockSpiller, request, mockChecker));
            assertTrue("Exception message should contain Unexpected field",
                    ex.getMessage() != null && ex.getMessage().contains("Unexpected field"));
        }
    }

    private ReadRecordsRequest buildReadRequest(ImagesTableProvider provider, BlockAllocatorImpl allocator, Map<String, ValueSet> constraintsMap)
    {
        FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
        TableName tableName = new TableName(getExpectedSchema(), getExpectedTable());
        GetTableRequest getTableRequest = new GetTableRequest(identity, "queryId", "catalog", tableName, Collections.emptyMap());
        return new ReadRecordsRequest(identity, "catalog", "queryId", tableName,
                provider.getTable(allocator, getTableRequest).getSchema(),
                Split.newBuilder(
                        S3SpillLocation.newBuilder().withBucket("b").withPrefix("p").withSplitId("s").withQueryId("q").withIsDirectory(true).build(),
                        new LocalKeyFactory().create()).build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000, 100_000);
    }
}
