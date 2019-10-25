package com.amazonaws.athena.connectors.aws.cmdb.tables.ec2;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.Volume;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EbsTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private AmazonEC2 ec2;

    public EbsTableProvider(AmazonEC2 ec2)
    {
        this.ec2 = ec2;
    }

    @Override
    public String getSchema()
    {
        return "ec2";
    }

    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "ebs_volumes");
    }

    @Override
    public GetTableResponse getTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableName(), SCHEMA);
    }

    @Override
    public void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
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
                instanceToRow(volume, constraintEvaluator, spiller, recordsRequest);
            }

            request.setNextToken(response.getNextToken());

            if (response.getNextToken() == null) {
                done = true;
            }
        }
    }

    private void instanceToRow(Volume volume,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            ReadRecordsRequest request)
    {
        final Map<String, Field> fields = new HashMap<>();
        request.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("id")) {
                String value = volume.getVolumeId();
                matched &= constraintEvaluator.apply("id", value);
                BlockUtils.setValue(block.getFieldVector("id"), row, value);
            }

            if (matched && fields.containsKey("type")) {
                String value = volume.getVolumeType();
                matched &= constraintEvaluator.apply("type", value);
                BlockUtils.setValue(block.getFieldVector("type"), row, value);
            }

            if (matched && fields.containsKey("target") && volume.getAttachments().size() == 1) {
                String value = volume.getAttachments().get(0).getInstanceId();
                matched &= constraintEvaluator.apply("target", value);
                BlockUtils.setValue(block.getFieldVector("target"), row, value);
            }

            if (matched && fields.containsKey("attached_device") && volume.getAttachments().size() == 1) {
                String value = volume.getAttachments().get(0).getDevice();
                matched &= constraintEvaluator.apply("attached_device", value);
                BlockUtils.setValue(block.getFieldVector("attached_device"), row, value);
            }

            if (matched && fields.containsKey("attachment_state") && volume.getAttachments().size() == 1) {
                String value = volume.getAttachments().get(0).getState();
                matched &= constraintEvaluator.apply("attachment_state", value);
                BlockUtils.setValue(block.getFieldVector("attachment_state"), row, value);
            }

            if (matched && fields.containsKey("attachment_time") && volume.getAttachments().size() == 1) {
                Date value = volume.getAttachments().get(0).getAttachTime();
                matched &= constraintEvaluator.apply("attachment_time", value);
                BlockUtils.setValue(block.getFieldVector("attachment_time"), row, value);
            }

            if (matched && fields.containsKey("availability_zone")) {
                String value = volume.getAvailabilityZone();
                matched &= constraintEvaluator.apply("availability_zone", value);
                BlockUtils.setValue(block.getFieldVector("availability_zone"), row, value);
            }

            if (matched && fields.containsKey("created_time")) {
                Date value = volume.getCreateTime();
                matched &= constraintEvaluator.apply("created_time", value);
                BlockUtils.setValue(block.getFieldVector("created_time"), row, value);
            }

            if (matched && fields.containsKey("is_encrypted")) {
                boolean value = volume.getEncrypted();
                matched &= constraintEvaluator.apply("is_encrypted", value);
                BlockUtils.setValue(block.getFieldVector("is_encrypted"), row, value);
            }

            if (matched && fields.containsKey("kms_key_id")) {
                String value = volume.getKmsKeyId();
                matched &= constraintEvaluator.apply("kms_key_id", value);
                BlockUtils.setValue(block.getFieldVector("kms_key_id"), row, value);
            }

            if (matched && fields.containsKey("size")) {
                Integer value = volume.getSize();
                matched &= constraintEvaluator.apply("size", value);
                BlockUtils.setValue(block.getFieldVector("size"), row, value);
            }

            if (matched && fields.containsKey("iops")) {
                Integer value = volume.getIops();
                matched &= constraintEvaluator.apply("iops", value);
                BlockUtils.setValue(block.getFieldVector("iops"), row, value);
            }

            if (matched && fields.containsKey("snapshot_id")) {
                String value = volume.getSnapshotId();
                matched &= constraintEvaluator.apply("snapshot_id", value);
                BlockUtils.setValue(block.getFieldVector("snapshot_id"), row, value);
            }

            if (matched && fields.containsKey("state")) {
                String value = volume.getState();
                matched &= constraintEvaluator.apply("state", value);
                BlockUtils.setValue(block.getFieldVector("state"), row, value);
            }

            if (matched && fields.containsKey("tags")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("tags");
                List<String> interfaces = volume.getTags().stream()
                        .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, interfaces);
            }

            return matched ? 1 : 0;
        });
    }

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
