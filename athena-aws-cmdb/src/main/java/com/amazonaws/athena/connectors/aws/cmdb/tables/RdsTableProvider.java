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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.amazonaws.services.rds.model.Tag;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.stream.Collectors;

/**
 * Maps your RDS instances to a table.
 */
public class RdsTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private AmazonRDS rds;

    public RdsTableProvider(AmazonRDS rds)
    {
        this.rds = rds;
    }

    /**
     * @See TableProvider
     */
    @Override
    public String getSchema()
    {
        return "rds";
    }

    /**
     * @See TableProvider
     */
    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "rds_instances");
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
     * Calls DescribeDBInstances on the AWS RDS Client returning all DB Instances that match the supplied predicate and attempting
     * to push down certain predicates (namely queries for specific DB Instance) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
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
                instanceToRow(instance, spiller);
            }

            request.setMarker(response.getMarker());

            if (response.getMarker() == null || !queryStatusChecker.isQueryRunning()) {
                done = true;
            }
        }
    }

    /**
     * Maps a DBInstance into a row in our Apache Arrow response block(s).
     *
     * @param instance The DBInstance to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void instanceToRow(DBInstance instance,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("instance_id", row, instance.getDBInstanceIdentifier());
            matched &= block.offerValue("primary_az", row, instance.getAvailabilityZone());
            matched &= block.offerValue("storage_gb", row, instance.getAllocatedStorage());
            matched &= block.offerValue("is_encrypted", row, instance.getStorageEncrypted());
            matched &= block.offerValue("storage_type", row, instance.getStorageType());
            matched &= block.offerValue("backup_retention_days", row, instance.getBackupRetentionPeriod());
            matched &= block.offerValue("auto_upgrade", row, instance.getAutoMinorVersionUpgrade());
            matched &= block.offerValue("instance_class", row, instance.getDBInstanceClass());
            matched &= block.offerValue("port", row, instance.getDbInstancePort());
            matched &= block.offerValue("status", row, instance.getDBInstanceStatus());
            matched &= block.offerValue("dbi_resource_id", row, instance.getDbiResourceId());
            matched &= block.offerValue("name", row, instance.getDBName());
            matched &= block.offerValue("engine", row, instance.getEngine());
            matched &= block.offerValue("engine_version", row, instance.getEngineVersion());
            matched &= block.offerValue("license_model", row, instance.getLicenseModel());
            matched &= block.offerValue("secondary_az", row, instance.getSecondaryAvailabilityZone());
            matched &= block.offerValue("backup_window", row, instance.getPreferredBackupWindow());
            matched &= block.offerValue("maint_window", row, instance.getPreferredMaintenanceWindow());
            matched &= block.offerValue("read_replica_source_id", row, instance.getReadReplicaSourceDBInstanceIdentifier());
            matched &= block.offerValue("create_time", row, instance.getInstanceCreateTime());
            matched &= block.offerValue("public_access", row, instance.getPubliclyAccessible());
            matched &= block.offerValue("iops", row, instance.getIops());
            matched &= block.offerValue("is_multi_az", row, instance.getMultiAZ());

            matched &= block.offerComplexValue("domains", row, (Field field, Object val) -> {
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
                    instance.getDomainMemberships());

            matched &= block.offerComplexValue("param_groups", row, (Field field, Object val) -> {
                        if (field.getName().equals("name")) {
                            return ((DBParameterGroupStatus) val).getDBParameterGroupName();
                        }
                        else if (field.getName().equals("status")) {
                            return ((DBParameterGroupStatus) val).getParameterApplyStatus();
                        }
                        throw new RuntimeException("Unexpected field " + field.getName());
                    },
                    instance.getDBParameterGroups());

            matched &= block.offerComplexValue("db_security_groups",
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
                    instance.getDBSecurityGroups());

            matched &= block.offerComplexValue("subnet_group",
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
                    instance.getDBSubnetGroup());

            matched &= block.offerComplexValue("endpoint",
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
                    instance.getEndpoint());

            matched &= block.offerComplexValue("status_infos",
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
                    instance.getStatusInfos());

            matched &= block.offerComplexValue("tags", row,
                    (Field field, Object val) -> {
                        if (field.getName().equals("key")) {
                            return ((Tag) val).getKey();
                        }
                        else if (field.getName().equals("value")) {
                            return ((Tag) val).getValue();
                        }

                        throw new RuntimeException("Unexpected field " + field.getName());
                    },
                    instance.getTagList());

            return matched ? 1 : 0;
        });
    }

    /**
     * Defines the schema of this table.
     */
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
                .addField(FieldBuilder.newBuilder("tags", new ArrowType.List())
                        .addField(FieldBuilder.newBuilder("tag", Types.MinorType.STRUCT.getType())
                                .addStringField("key")
                                .addStringField("value")
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
                .addMetadata("tags", "Tags associated with the DB instance.")
                .build();
    }
}
