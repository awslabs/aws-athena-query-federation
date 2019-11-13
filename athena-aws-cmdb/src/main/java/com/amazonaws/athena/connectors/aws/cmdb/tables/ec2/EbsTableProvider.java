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
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.Volume;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps your EBS volumes to a table.
 */
public class EbsTableProvider
        implements TableProvider
{
    private static final Logger logger = LoggerFactory.getLogger(EbsTableProvider.class);
    private static final Schema SCHEMA;
    private AmazonEC2 ec2;

    public EbsTableProvider(AmazonEC2 ec2)
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
        return new TableName(getSchema(), "ebs_volumes");
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
     * Calls DescribeVolumes on the AWS EC2 Client returning all volumes that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific volumes) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        boolean done = false;
        DescribeVolumesRequest request = new DescribeVolumesRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setVolumeIds(Collections.singletonList(idConstraint.getSingleValue().toString()));
        }

        while (!done) {
            DescribeVolumesResult response = ec2.describeVolumes(request);

            for (Volume volume : response.getVolumes()) {
                logger.info("readWithConstraint: {}", response);
                instanceToRow(volume, spiller);
            }

            request.setNextToken(response.getNextToken());

            if (response.getNextToken() == null) {
                done = true;
            }
        }
    }

    /**
     * Maps an EBS Volume into a row in our Apache Arrow response block(s).
     *
     * @param volume The EBS Volume to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(Volume volume,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("id", row, volume.getVolumeId());
            matched &= block.offerValue("type", row, volume.getVolumeType());
            matched &= block.offerValue("availability_zone", row, volume.getAvailabilityZone());
            matched &= block.offerValue("created_time", row, volume.getCreateTime());
            matched &= block.offerValue("is_encrypted", row, volume.getEncrypted());
            matched &= block.offerValue("kms_key_id", row, volume.getKmsKeyId());
            matched &= block.offerValue("size", row, volume.getSize());
            matched &= block.offerValue("iops", row, volume.getIops());
            matched &= block.offerValue("snapshot_id", row, volume.getSnapshotId());
            matched &= block.offerValue("state", row, volume.getState());

            if (volume.getAttachments().size() == 1) {
                matched &= block.offerValue("target", row, volume.getAttachments().get(0).getInstanceId());
                matched &= block.offerValue("attached_device", row, volume.getAttachments().get(0).getDevice());
                matched &= block.offerValue("attachment_state", row, volume.getAttachments().get(0).getState());
                matched &= block.offerValue("attachment_time", row, volume.getAttachments().get(0).getAttachTime());
            }

            List<String> tags = volume.getTags().stream()
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
                .addStringField("type")
                .addStringField("target")
                .addStringField("attached_device")
                .addStringField("attachment_state")
                .addField("attachment_time", Types.MinorType.DATEMILLI.getType())
                .addStringField("availability_zone")
                .addField("created_time", Types.MinorType.DATEMILLI.getType())
                .addBitField("is_encrypted")
                .addStringField("kms_key_id")
                .addIntField("size")
                .addIntField("iops")
                .addStringField("snapshot_id")
                .addStringField("state")
                .addListField("tags", Types.MinorType.VARCHAR.getType())
                .addMetadata("id", "EBS Volume Id")
                .addMetadata("type", "EBS Volume Type")
                .addMetadata("target", "EC2 Instance Id that this volume is attached to.")
                .addMetadata("attached_device", "Device name where this EBS volume is attached.")
                .addMetadata("attachment_state", "The state of the volume attachement.")
                .addMetadata("attachment_time", "The time this volume was attached to its target.")
                .addMetadata("availability_zone", "The AZ that this EBS Volume is in.")
                .addMetadata("created_time", "The date time that the volume was created.")
                .addMetadata("is_encrypted", "True if the volume is encrypted with KMS managed key.")
                .addMetadata("kms_key_id", "The KMS key id used to encrypt this volume.")
                .addMetadata("size", "The size in GBs of this volume.")
                .addMetadata("iops", "Provisioned IOPs supported by this volume.")
                .addMetadata("snapshot_id", "ID of the last snapshot for this volume.")
                .addMetadata("state", "State of the EBS Volume.")
                .addMetadata("tags", "Tags associated with the volume.")
                .build();
    }
}
