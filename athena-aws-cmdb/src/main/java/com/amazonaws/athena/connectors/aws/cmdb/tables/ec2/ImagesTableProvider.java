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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Tag;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps your EC2 images (aka AMIs) to a table.
 */
public class ImagesTableProvider
        implements TableProvider
{
    private static final String DEFAULT_OWNER_ENV = "default_ec2_image_owner";
    private static final int MAX_IMAGES = 1000;
    //Sets a default owner filter (when not null) to reduce the number of irrelevant AMIs returned when you do not
    //query for a specific owner.
    private static final String DEFAULT_OWNER = System.getenv(DEFAULT_OWNER_ENV);
    private static final Schema SCHEMA;
    private AmazonEC2 ec2;

    public ImagesTableProvider(AmazonEC2 ec2)
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
        return new TableName(getSchema(), "ec2_images");
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
     * Calls DescribeImagess on the AWS EC2 Client returning all images that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific volumes) to EC2.
     *
     * @note Because of the large number of public AMIs we also support using a default 'owner' filter if your query doesn't
     * filter on owner itself. You can set this using an env variable on your Lambda function defined by DEFAULT_OWNER_ENV.
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        final Map<String, Field> fields = new HashMap<>();
        recordsRequest.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        DescribeImagesRequest request = new DescribeImagesRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        ValueSet ownerConstraint = recordsRequest.getConstraints().getSummary().get("owner");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setImageIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }
        else if (ownerConstraint != null && ownerConstraint.isSingleValue()) {
            request.setOwners(Collections.singletonList(ownerConstraint.getSingleValue().toString()));
        }
        else if (DEFAULT_OWNER != null) {
            request.setOwners(Collections.singletonList(DEFAULT_OWNER));
        }
        else {
            throw new RuntimeException("A default owner account must be set or the query must have owner" +
                    "in the where clause with exactly 1 value otherwise results may be too big.");
        }

        DescribeImagesResult response = ec2.describeImages(request);

        int count = 0;
        for (Image next : response.getImages()) {
            if (count++ > MAX_IMAGES) {
                throw new RuntimeException("Too many images returned, add an owner or id filter.");
            }
            instanceToRow(next, constraintEvaluator, spiller, fields);
        }
    }

    /**
     * Maps an EC2 Image (AMI) into a row in our Apache Arrow response block(s).
     *
     * @param image The EC2 Image (AMI) to map.
     * @param constraintEvaluator The ConstraintEvaluator we can use to filter results.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @param fields The set of fields that need to be projected.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(Image image,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            Map<String, Field> fields)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("id")) {
                String value = image.getImageId();
                matched &= constraintEvaluator.apply("id", value);
                BlockUtils.setValue(block.getFieldVector("id"), row, value);
            }

            if (matched && fields.containsKey("architecture")) {
                String value = image.getArchitecture();
                matched &= constraintEvaluator.apply("architecture", value);
                BlockUtils.setValue(block.getFieldVector("architecture"), row, value);
            }

            if (matched && fields.containsKey("created")) {
                String value = image.getCreationDate();
                matched &= constraintEvaluator.apply("created", value);
                BlockUtils.setValue(block.getFieldVector("created"), row, value);
            }

            if (matched && fields.containsKey("description")) {
                String value = image.getDescription();
                matched &= constraintEvaluator.apply("description", value);
                BlockUtils.setValue(block.getFieldVector("description"), row, value);
            }

            if (matched && fields.containsKey("hypervisor")) {
                String value = image.getHypervisor();
                matched &= constraintEvaluator.apply("hypervisor", value);
                BlockUtils.setValue(block.getFieldVector("hypervisor"), row, value);
            }

            if (matched && fields.containsKey("location")) {
                String value = image.getImageLocation();
                matched &= constraintEvaluator.apply("location", value);
                BlockUtils.setValue(block.getFieldVector("location"), row, value);
            }

            if (matched && fields.containsKey("type")) {
                String value = image.getImageType();
                matched &= constraintEvaluator.apply("type", value);
                BlockUtils.setValue(block.getFieldVector("type"), row, value);
            }

            if (matched && fields.containsKey("kernel")) {
                String value = image.getKernelId();
                matched &= constraintEvaluator.apply("kernel", value);
                BlockUtils.setValue(block.getFieldVector("kernel"), row, value);
            }

            if (matched && fields.containsKey("name")) {
                String value = image.getName();
                matched &= constraintEvaluator.apply("name", value);
                BlockUtils.setValue(block.getFieldVector("name"), row, value);
            }

            if (matched && fields.containsKey("owner")) {
                String value = image.getOwnerId();
                matched &= constraintEvaluator.apply("owner", value);
                BlockUtils.setValue(block.getFieldVector("owner"), row, value);
            }

            if (matched && fields.containsKey("platform")) {
                String value = image.getPlatform();
                matched &= constraintEvaluator.apply("platform", value);
                BlockUtils.setValue(block.getFieldVector("platform"), row, value);
            }

            if (matched && fields.containsKey("ramdisk")) {
                String value = image.getRamdiskId();
                matched &= constraintEvaluator.apply("ramdisk", value);
                BlockUtils.setValue(block.getFieldVector("ramdisk"), row, value);
            }

            if (matched && fields.containsKey("root_device")) {
                String value = image.getRootDeviceName();
                matched &= constraintEvaluator.apply("root_device", value);
                BlockUtils.setValue(block.getFieldVector("root_device"), row, value);
            }

            if (matched && fields.containsKey("root_type")) {
                String value = image.getRootDeviceType();
                matched &= constraintEvaluator.apply("root_type", value);
                BlockUtils.setValue(block.getFieldVector("root_type"), row, value);
            }

            if (matched && fields.containsKey("srvio_net")) {
                String value = image.getSriovNetSupport();
                matched &= constraintEvaluator.apply("srvio_net", value);
                BlockUtils.setValue(block.getFieldVector("srvio_net"), row, value);
            }

            if (matched && fields.containsKey("state")) {
                String value = image.getState();
                matched &= constraintEvaluator.apply("state", value);
                BlockUtils.setValue(block.getFieldVector("state"), row, value);
            }

            if (matched && fields.containsKey("virt_type")) {
                String value = image.getVirtualizationType();
                matched &= constraintEvaluator.apply("virt_type", value);
                BlockUtils.setValue(block.getFieldVector("virt_type"), row, value);
            }

            if (matched && fields.containsKey("is_public")) {
                boolean value = image.getPublic();
                matched &= constraintEvaluator.apply("is_public", value);
                BlockUtils.setValue(block.getFieldVector("is_public"), row, value);
            }

            if (matched && fields.containsKey("tags")) {
                //TODO: apply constraint for complex type
                List<Tag> tags = image.getTags();
                BlockUtils.setComplexValue(block.getFieldVector("tags"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("key")) {
                                return ((Tag) val).getKey();
                            }
                            else if (field.getName().equals("value")) {
                                return ((Tag) val).getValue();
                            }

                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        tags);
            }

            if (matched && fields.containsKey("block_devices")) {
                //TODO: constraints for complex types
                List<BlockDeviceMapping> value = image.getBlockDeviceMappings();
                matched &= constraintEvaluator.apply("block_devices", value);
                BlockUtils.setComplexValue(block.getFieldVector("block_devices"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("dev_name")) {
                                return ((BlockDeviceMapping) val).getDeviceName();
                            }
                            else if (field.getName().equals("no_device")) {
                                return ((BlockDeviceMapping) val).getNoDevice();
                            }
                            else if (field.getName().equals("virt_name")) {
                                return ((BlockDeviceMapping) val).getVirtualName();
                            }
                            else if (field.getName().equals("ebs")) {
                                return ((BlockDeviceMapping) val).getEbs();
                            }
                            else if (field.getName().equals("ebs_size")) {
                                return ((EbsBlockDevice) val).getVolumeSize();
                            }
                            else if (field.getName().equals("ebs_iops")) {
                                return ((EbsBlockDevice) val).getIops();
                            }
                            else if (field.getName().equals("ebs_type")) {
                                return ((EbsBlockDevice) val).getVolumeType();
                            }
                            else if (field.getName().equals("ebs_kms_key")) {
                                return ((EbsBlockDevice) val).getKmsKeyId();
                            }

                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        value);
            }

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("id")
                .addStringField("architecture")
                .addStringField("created")
                .addStringField("description")
                .addStringField("hypervisor")
                .addStringField("location")
                .addStringField("type")
                .addStringField("kernel")
                .addStringField("name")
                .addStringField("owner")
                .addStringField("platform")
                .addStringField("ramdisk")
                .addStringField("root_device")
                .addStringField("root_type")
                .addStringField("srvio_net")
                .addStringField("state")
                .addStringField("virt_type")
                .addBitField("is_public")
                .addField(
                        FieldBuilder.newBuilder("tags", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("tag", Types.MinorType.STRUCT.getType())
                                                .addStringField("key")
                                                .addStringField("value")
                                                .build())
                                .build())
                .addField(
                        FieldBuilder.newBuilder("block_devices", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("device", Types.MinorType.STRUCT.getType())
                                                .addStringField("dev_name")
                                                .addStringField("no_device")
                                                .addStringField("virt_name")
                                                .addField(
                                                        FieldBuilder.newBuilder("ebs", Types.MinorType.STRUCT.getType())
                                                                .addIntField("ebs_size")
                                                                .addIntField("ebs_iops")
                                                                .addStringField("ebs_type")
                                                                .addStringField("ebs_kms_key")
                                                                .build())
                                                .build())
                                .build())
                .addMetadata("id", "The id of the image.")
                .addMetadata("architecture", "The architecture required to run the image.")
                .addMetadata("created", "The date and time the image was created.")
                .addMetadata("description", "The description associated with the image.")
                .addMetadata("hypervisor", "The type of hypervisor required by the image.")
                .addMetadata("location", "The location of the image.")
                .addMetadata("type", "The type of image.")
                .addMetadata("kernel", "The kernel used by the image.")
                .addMetadata("name", "The name of the image.")
                .addMetadata("owner", "The owner of the image.")
                .addMetadata("platform", "The platform required by the image.")
                .addMetadata("ramdisk", "Detailed of the ram disk used by the image.")
                .addMetadata("root_device", "The root device used by the image.")
                .addMetadata("root_type", "The type of root device required by the image.")
                .addMetadata("srvio_net", "Details of srvio network support in the image.")
                .addMetadata("state", "The state of the image.")
                .addMetadata("virt_type", "The type of virtualization supported by the image.")
                .addMetadata("is_public", "True if the image is publically available.")
                .addMetadata("tags", "Tags associated with the image.")
                .addMetadata("block_devices", "Block devices required by the image.")
                .build();
    }
}
