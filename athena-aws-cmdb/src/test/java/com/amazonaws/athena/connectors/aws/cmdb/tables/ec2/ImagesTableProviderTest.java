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
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Tag;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ImagesTableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(ImagesTableProviderTest.class);

    @Mock
    private AmazonEC2 mockEc2;

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

            assertEquals(getIdValue(), request.getImageIds().get(0));
            DescribeImagesResult mockResult = mock(DescribeImagesResult.class);
            List<Image> values = new ArrayList<>();
            values.add(makeImage(getIdValue()));
            values.add(makeImage(getIdValue()));
            values.add(makeImage("fake-id"));
            when(mockResult.getImages()).thenReturn(values);
            return mockResult;
        });
    }

    protected void validateRow(Block block, int pos)
    {
        for (FieldReader fieldReader : block.getFieldReaders()) {
            fieldReader.setPosition(pos);
            Field field = fieldReader.getField();

            if (field.getName().equals(getIdField())) {
                assertEquals(getIdValue(), fieldReader.readText().toString());
            }
            else {
                validate(fieldReader);
            }
        }
    }

    private void validate(FieldReader fieldReader)
    {
        Field field = fieldReader.getField();
        Types.MinorType type = Types.getMinorTypeForArrowType(field.getType());
        switch (type) {
            case VARCHAR:
                if (field.getName().equals("$data$")) {
                    assertNotNull(fieldReader.readText().toString());
                }
                else {
                    assertEquals(field.getName(), fieldReader.readText().toString());
                }
                break;
            case DATEMILLI:
                assertEquals(100_000, fieldReader.readLocalDateTime().atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli());
                break;
            case BIT:
                assertTrue(fieldReader.readBoolean());
                break;
            case INT:
                assertTrue(fieldReader.readInteger() > 0);
                break;
            case STRUCT:
                for (Field child : field.getChildren()) {
                    validate(fieldReader.reader(child.getName()));
                }
                break;
            case LIST:
                validate(fieldReader.reader());
                break;
            default:
                throw new RuntimeException("No validation configured for field " + field.getName() + ":" + type + " " + field.getChildren());
        }
    }

    private Image makeImage(String id)
    {
        Image image = new Image();
        image.withImageId(id)
                .withArchitecture("architecture")
                .withCreationDate("created")
                .withDescription("description")
                .withHypervisor("hypervisor")
                .withImageLocation("location")
                .withImageType("type")
                .withKernelId("kernel")
                .withName("name")
                .withOwnerId("owner")
                .withPlatform("platform")
                .withRamdiskId("ramdisk")
                .withRootDeviceName("root_device")
                .withRootDeviceType("root_type")
                .withSriovNetSupport("srvio_net")
                .withState("state")
                .withVirtualizationType("virt_type")
                .withPublic(true)
                .withTags(new Tag("key", "value"))
                .withBlockDeviceMappings(new BlockDeviceMapping()
                        .withDeviceName("dev_name")
                        .withNoDevice("no_device")
                        .withVirtualName("virt_name")
                        .withEbs(new EbsBlockDevice()
                                .withIops(100)
                                .withKmsKeyId("ebs_kms_key")
                                .withVolumeType("ebs_type")
                                .withVolumeSize(100)));

        return image;
    }
}
