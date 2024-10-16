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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RdsTableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(RdsTableProviderTest.class);

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
        try {
            logger.info("validate: {} {}", fieldReader.getField().getName(), fieldReader.getMinorType());
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
        catch (RuntimeException ex) {
            throw new RuntimeException("Error validating field " + fieldReader.getField().getName(), ex);
        }
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
}
