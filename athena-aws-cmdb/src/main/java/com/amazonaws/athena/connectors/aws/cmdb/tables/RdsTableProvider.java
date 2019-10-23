package com.amazonaws.athena.connectors.aws.cmdb.tables;

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
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DBInstanceStatusInfo;
import com.amazonaws.services.rds.model.DBParameterGroupStatus;
import com.amazonaws.services.rds.model.DBSecurityGroupMembership;
import com.amazonaws.services.rds.model.DBSubnetGroup;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.amazonaws.services.rds.model.DomainMembership;
import com.amazonaws.services.rds.model.Endpoint;
import com.amazonaws.services.rds.model.Subnet;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RdsTableProvider
        implements TableProvider
{
    private static Schema SCHEMA;

    static {
        SCHEMA = SchemaBuilder.newBuilder()
                .addStringField("instance_id")
                .addStringField("primary_az")
                .addIntField("storage_gb")
                .addBitField("is_encrypted")
                .addStringField("storage_type")
                .addIntField("backup_retention_days")
                .addBitField("auto_upgrade")
                .addStringField("instance_class")
                .addIntField("port")
                .addStringField("status")
                .addStringField("dbi_resource_id")
                .addStringField("name")
                .addField(
                        FieldBuilder.newBuilder("domains", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("domain", Types.MinorType.STRUCT.getType())
                                                .addStringField("domain")
                                                .addStringField("fqdn")
                                                .addStringField("iam_role")
                                                .addStringField("status")
                                                .build())
                                .build())
                .addStringField("engine")
                .addStringField("engine_version")
                .addStringField("license_model")
                .addStringField("secondary_az")
                .addStringField("backup_window")
                .addStringField("maint_window")
                .addStringField("read_replica_source_id")
                .addField(
                        FieldBuilder.newBuilder("param_groups", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("param_group", Types.MinorType.STRUCT.getType())
                                                .addStringField("name")
                                                .addStringField("status")
                                                .build())
                                .build())
                .addField(
                        FieldBuilder.newBuilder("db_security_groups", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("db_security_group", Types.MinorType.STRUCT.getType())
                                                .addStringField("name")
                                                .addStringField("status")
                                                .build())
                                .build())
                .addStructField("subnet_group")
                .addChildField("subnet_group", "name", Types.MinorType.VARCHAR.getType())
                .addChildField("subnet_group", "status", Types.MinorType.VARCHAR.getType())
                .addChildField("subnet_group", "vpc", Types.MinorType.VARCHAR.getType())
                .addChildField("subnet_group", FieldBuilder.newBuilder("subnets", Types.MinorType.LIST.getType())
                        .addStringField("subnets").build())
                .addField(FieldBuilder.newBuilder("endpoint", Types.MinorType.STRUCT.getType())
                        .addStringField("address")
                        .addIntField("port")
                        .addStringField("zone")
                        .build())
                .addField("create_time", Types.MinorType.DATEMILLI.getType())
                .addBitField("public_access")

                .addField(
                        FieldBuilder.newBuilder("status_infos", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("status_info", Types.MinorType.STRUCT.getType())
                                                .addStringField("message")
                                                .addBitField("is_normal")
                                                .addStringField("status")
                                                .addStringField("type")
                                                .build())
                                .build())

                .addIntField("iops")
                .addBitField("is_multi_az")
                .addMetadata("instance_id", "Database Instance Id")
                .addMetadata("primary_az", "The primary az for the database instance")
                .addMetadata("storage_gb", "Total allocated storage for the Database Instances in GB.")
                .addMetadata("is_encrypted", "True if the database is encrypted.")
                .addMetadata("storage_type", "The type of storage used by this Database Instance.")
                .addMetadata("backup_retention_days", "The number of days of backups to keep.")
                .addMetadata("auto_upgrade", "True if the cluster auto-upgrades minor versions.")
                .addMetadata("instance_class", "The instance type used by this database.")
                .addMetadata("port", "Listen port for the database.")
                .addMetadata("status", "Status of the DB Instance.")
                .addMetadata("dbi_resource_id", "Unique id for the instance of the database.")
                .addMetadata("name", "Name of the DB Instance.")
                .addMetadata("domains", "Active Directory domains to which the DB Instance is associated.")
                .addMetadata("applications", "The EMR applications installed on the cluster.")
                .addMetadata("engine", "The engine type of the DB Instance.")
                .addMetadata("engine_version", "The engine version of the DB Instance")
                .addMetadata("license_model", "The license model of the DB Instance")
                .addMetadata("secondary_az", "The secondary AZ of the DB Instance")
                .addMetadata("backup_window", "The backup window of the DB Instance")
                .addMetadata("maint_window", "The maintenance window of the DB Instance")
                .addMetadata("read_replica_source_id", "The read replica source id, if present, of the DB Instance")
                .addMetadata("param_groups", "The param groups applied to the DB Instance")
                .addMetadata("db_security_groups", "The security groups applies the DB Instance")
                .addMetadata("subnet_groups", "The subnets available to the DB Instance")
                .addMetadata("endpoint", "The endpoint of the DB Instance")
                .addMetadata("create_time", "The create time of the DB Instance")
                .addMetadata("public_access", "True if publically accessible.")
                .addMetadata("status_infos", "The status info details associated with the DB Instance")
                .addMetadata("iops", "The total provisioned IOPs for the DB Instance.")
                .addMetadata("is_multi_az", "True if the DB Instance is avialable in multiple AZs.")
                .build();
    }

    private AmazonRDS rds;

    public RdsTableProvider(AmazonRDS rds)
    {
        this.rds = rds;
    }

    @Override
    public String getSchema()
    {
        return "rds";
    }

    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "rds_instances");
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
        DescribeDBInstancesRequest request = new DescribeDBInstancesRequest();

        ValueSet idConstraint = recordsRequest.getConstraints().getSummary().get("instance_id");
        if (idConstraint != null && idConstraint.isSingleValue()) {
            request.setDBInstanceIdentifier(idConstraint.getSingleValue().toString());
        }

        while (!done) {
            DescribeDBInstancesResult response = rds.describeDBInstances(request);

            for (DBInstance instance : response.getDBInstances()) {
                instanceToRow(instance, constraintEvaluator, spiller, recordsRequest);
            }

            request.setMarker(response.getMarker());

            if (response.getMarker() == null) {
                done = true;
            }
        }
    }

    private void instanceToRow(DBInstance instance,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            ReadRecordsRequest request)
    {
        final Map<String, Field> fields = new HashMap<>();
        request.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("instance_id")) {
                String value = instance.getDBInstanceIdentifier();
                matched &= constraintEvaluator.apply("instance_id", value);
                BlockUtils.setValue(block.getFieldVector("instance_id"), row, value);
            }

            if (matched && fields.containsKey("primary_az")) {
                String value = instance.getAvailabilityZone();
                matched &= constraintEvaluator.apply("primary_az", value);
                BlockUtils.setValue(block.getFieldVector("primary_az"), row, value);
            }

            if (matched && fields.containsKey("storage_gb")) {
                Integer value = instance.getAllocatedStorage();
                matched &= constraintEvaluator.apply("storage_gb", value);
                BlockUtils.setValue(block.getFieldVector("storage_gb"), row, value);
            }

            if (matched && fields.containsKey("is_encrypted")) {
                Boolean value = instance.getStorageEncrypted();
                matched &= constraintEvaluator.apply("is_encrypted", value);
                BlockUtils.setValue(block.getFieldVector("is_encrypted"), row, value);
            }

            if (matched && fields.containsKey("storage_type")) {
                String value = instance.getStorageType();
                matched &= constraintEvaluator.apply("storage_type", value);
                BlockUtils.setValue(block.getFieldVector("storage_type"), row, value);
            }

            if (matched && fields.containsKey("backup_retention_days")) {
                Integer value = instance.getBackupRetentionPeriod();
                matched &= constraintEvaluator.apply("backup_retention_days", value);
                BlockUtils.setValue(block.getFieldVector("backup_retention_days"), row, value);
            }

            if (matched && fields.containsKey("auto_upgrade")) {
                Boolean value = instance.getAutoMinorVersionUpgrade();
                matched &= constraintEvaluator.apply("auto_upgrade", value);
                BlockUtils.setValue(block.getFieldVector("auto_upgrade"), row, value);
            }

            if (matched && fields.containsKey("instance_class")) {
                String value = instance.getDBInstanceClass();
                matched &= constraintEvaluator.apply("instance_class", value);
                BlockUtils.setValue(block.getFieldVector("instance_class"), row, value);
            }

            if (matched && fields.containsKey("port")) {
                Integer value = instance.getDbInstancePort();
                matched &= constraintEvaluator.apply("port", value);
                BlockUtils.setValue(block.getFieldVector("port"), row, value);
            }

            if (matched && fields.containsKey("status")) {
                String value = instance.getDBInstanceStatus();
                matched &= constraintEvaluator.apply("status", value);
                BlockUtils.setValue(block.getFieldVector("status"), row, value);
            }

            if (matched && fields.containsKey("dbi_resource_id")) {
                String value = instance.getDbiResourceId();
                matched &= constraintEvaluator.apply("dbi_resource_id", value);
                BlockUtils.setValue(block.getFieldVector("dbi_resource_id"), row, value);
            }

            if (matched && fields.containsKey("name")) {
                String value = instance.getDBName();
                matched &= constraintEvaluator.apply("name", value);
                BlockUtils.setValue(block.getFieldVector("name"), row, value);
            }

            if (matched && fields.containsKey("domains")) {
                //TODO: constraints for complex types
                List<DomainMembership> value = instance.getDomainMemberships();
                matched &= constraintEvaluator.apply("domains", value);
                BlockUtils.setComplexValue(block.getFieldVector("domains"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("domain")) {
                                return ((DomainMembership) val).getDomain();
                            }
                            else if (field.getName().equals("fqdn")) {
                                return ((DomainMembership) val).getFQDN();
                            }
                            else if (field.getName().equals("iam_role")) {
                                return ((DomainMembership) val).getIAMRoleName();
                            }
                            else if (field.getName().equals("status")) {
                                return ((DomainMembership) val).getStatus();
                            }

                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        value);
            }

            if (matched && fields.containsKey("engine")) {
                String value = instance.getEngine();
                matched &= constraintEvaluator.apply("engine", value);
                BlockUtils.setValue(block.getFieldVector("engine"), row, value);
            }

            if (matched && fields.containsKey("engine_version")) {
                String value = instance.getEngineVersion();
                matched &= constraintEvaluator.apply("engine_version", value);
                BlockUtils.setValue(block.getFieldVector("engine_version"), row, value);
            }

            if (matched && fields.containsKey("license_model")) {
                String value = instance.getLicenseModel();
                matched &= constraintEvaluator.apply("license_model", value);
                BlockUtils.setValue(block.getFieldVector("license_model"), row, value);
            }

            if (matched && fields.containsKey("secondary_az")) {
                String value = instance.getSecondaryAvailabilityZone();
                matched &= constraintEvaluator.apply("secondary_az", value);
                BlockUtils.setValue(block.getFieldVector("secondary_az"), row, value);
            }

            if (matched && fields.containsKey("backup_window")) {
                String value = instance.getPreferredBackupWindow();
                matched &= constraintEvaluator.apply("backup_window", value);
                BlockUtils.setValue(block.getFieldVector("backup_window"), row, value);
            }

            if (matched && fields.containsKey("maint_window")) {
                String value = instance.getPreferredMaintenanceWindow();
                matched &= constraintEvaluator.apply("maint_window", value);
                BlockUtils.setValue(block.getFieldVector("maint_window"), row, value);
            }

            if (matched && fields.containsKey("read_replica_source_id")) {
                String value = instance.getReadReplicaSourceDBInstanceIdentifier();
                matched &= constraintEvaluator.apply("read_replica_source_id", value);
                BlockUtils.setValue(block.getFieldVector("read_replica_source_id"), row, value);
            }

            if (matched && fields.containsKey("param_groups")) {
                //TODO: constraints for complex types
                List<DBParameterGroupStatus> value = instance.getDBParameterGroups();
                matched &= constraintEvaluator.apply("param_groups", value);
                BlockUtils.setComplexValue(block.getFieldVector("param_groups"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("name")) {
                                return ((DBParameterGroupStatus) val).getDBParameterGroupName();
                            }
                            else if (field.getName().equals("status")) {
                                return ((DBParameterGroupStatus) val).getParameterApplyStatus();
                            }
                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        value);
            }

            if (matched && fields.containsKey("db_security_groups")) {
                //TODO: constraints for complex types
                List<DBSecurityGroupMembership> value = instance.getDBSecurityGroups();
                matched &= constraintEvaluator.apply("db_security_groups", value);
                BlockUtils.setComplexValue(block.getFieldVector("db_security_groups"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("name")) {
                                return ((DBSecurityGroupMembership) val).getDBSecurityGroupName();
                            }
                            else if (field.getName().equals("status")) {
                                return ((DBSecurityGroupMembership) val).getStatus();
                            }
                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        value);
            }

            if (matched && fields.containsKey("subnet_group")) {
                //TODO: constraints for complex types
                DBSubnetGroup value = instance.getDBSubnetGroup();
                matched &= constraintEvaluator.apply("subnet_group", value);
                BlockUtils.setComplexValue(block.getFieldVector("subnet_group"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("description")) {
                                return ((DBSubnetGroup) val).getDBSubnetGroupDescription();
                            }
                            else if (field.getName().equals("name")) {
                                return ((DBSubnetGroup) val).getDBSubnetGroupName();
                            }
                            else if (field.getName().equals("status")) {
                                return ((DBSubnetGroup) val).getSubnetGroupStatus();
                            }
                            else if (field.getName().equals("vpc")) {
                                return ((DBSubnetGroup) val).getVpcId();
                            }
                            else if (field.getName().equals("subnets")) {
                                return ((DBSubnetGroup) val).getSubnets().stream()
                                        .map(next -> next.getSubnetIdentifier()).collect(Collectors.toList());
                            }
                            else if (val instanceof Subnet) {
                                return ((Subnet) val).getSubnetIdentifier();
                            }
                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        value);
            }

            if (matched && fields.containsKey("endpoint")) {
                //TODO: constraints for complex types
                Endpoint value = instance.getEndpoint();
                matched &= constraintEvaluator.apply("endpoint", value);
                BlockUtils.setComplexValue(block.getFieldVector("endpoint"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("address")) {
                                return ((Endpoint) val).getAddress();
                            }
                            else if (field.getName().equals("port")) {
                                return ((Endpoint) val).getPort();
                            }
                            else if (field.getName().equals("zone")) {
                                return ((Endpoint) val).getHostedZoneId();
                            }
                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        value);
            }

            if (matched && fields.containsKey("create_time")) {
                Date value = instance.getInstanceCreateTime();
                matched &= constraintEvaluator.apply("create_time", value);
                BlockUtils.setValue(block.getFieldVector("create_time"), row, value);
            }

            if (matched && fields.containsKey("public_access")) {
                Boolean value = instance.getPubliclyAccessible();
                matched &= constraintEvaluator.apply("public_access", value);
                BlockUtils.setValue(block.getFieldVector("public_access"), row, value);
            }

            if (matched && fields.containsKey("status_infos")) {
                //TODO: constraints for complex types
                List<DBInstanceStatusInfo> value = instance.getStatusInfos();
                matched &= constraintEvaluator.apply("status_infos", value);
                BlockUtils.setComplexValue(block.getFieldVector("status_infos"),
                        row,
                        (Field field, Object val) -> {
                            if (field.getName().equals("message")) {
                                return ((DBInstanceStatusInfo) val).getMessage();
                            }
                            else if (field.getName().equals("is_normal")) {
                                return ((DBInstanceStatusInfo) val).getNormal();
                            }
                            else if (field.getName().equals("status")) {
                                return ((DBInstanceStatusInfo) val).getStatus();
                            }
                            else if (field.getName().equals("type")) {
                                return ((DBInstanceStatusInfo) val).getStatusType();
                            }
                            throw new RuntimeException("Unexpected field " + field.getName());
                        },
                        value);
            }

            if (matched && fields.containsKey("iops")) {
                Integer value = instance.getIops();
                matched &= constraintEvaluator.apply("iops", value);
                BlockUtils.setValue(block.getFieldVector("iops"), row, value);
            }

            if (matched && fields.containsKey("is_multi_az")) {
                Boolean value = instance.getMultiAZ();
                matched &= constraintEvaluator.apply("is_multi_az", value);
                BlockUtils.setValue(block.getFieldVector("is_multi_az"), row, value);
            }

            return matched ? 1 : 0;
        });
    }
}
