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
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterStateChangeReason;
import com.amazonaws.services.elasticmapreduce.model.ClusterStatus;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DBInstanceStatusInfo;
import com.amazonaws.services.rds.model.DBParameterGroup;
import com.amazonaws.services.rds.model.DBParameterGroupStatus;
import com.amazonaws.services.rds.model.DBSecurityGroupMembership;
import com.amazonaws.services.rds.model.DBSubnetGroup;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.amazonaws.services.rds.model.DomainMembership;
import com.amazonaws.services.rds.model.Endpoint;
import com.amazonaws.services.rds.model.Subnet;
import com.amazonaws.services.rds.model.Tag;

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
    private AmazonRDS mockRds;

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
        when(mockRds.describeDBInstances(nullable(DescribeDBInstancesRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    DescribeDBInstancesResult mockResult = mock(DescribeDBInstancesResult.class);
                    List<DBInstance> values = new ArrayList<>();
                    values.add(makeValue(getIdValue()));
                    values.add(makeValue(getIdValue()));
                    values.add(makeValue("fake-id"));
                    when(mockResult.getDBInstances()).thenReturn(values);

                    if (requestCount.incrementAndGet() < 3) {
                        when(mockResult.getMarker()).thenReturn(String.valueOf(requestCount.get()));
                    }
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
        return new DBInstance()
                .withDBInstanceIdentifier(id)
                .withAvailabilityZone("primary_az")
                .withAllocatedStorage(100)
                .withStorageEncrypted(true)
                .withBackupRetentionPeriod(100)
                .withAutoMinorVersionUpgrade(true)
                .withDBInstanceClass("instance_class")
                .withDbInstancePort(100)
                .withDBInstanceStatus("status")
                .withStorageType("storage_type")
                .withDbiResourceId("dbi_resource_id")
                .withDBName("name")
                .withDomainMemberships(new DomainMembership()
                        .withDomain("domain")
                        .withFQDN("fqdn")
                        .withIAMRoleName("iam_role")
                        .withStatus("status"))
                .withEngine("engine")
                .withEngineVersion("engine_version")
                .withLicenseModel("license_model")
                .withSecondaryAvailabilityZone("secondary_az")
                .withPreferredBackupWindow("backup_window")
                .withPreferredMaintenanceWindow("maint_window")
                .withReadReplicaSourceDBInstanceIdentifier("read_replica_source_id")
                .withDBParameterGroups(new DBParameterGroupStatus()
                        .withDBParameterGroupName("name")
                        .withParameterApplyStatus("status"))
                .withDBSecurityGroups(new DBSecurityGroupMembership()
                        .withDBSecurityGroupName("name")
                        .withStatus("status"))
                .withDBSubnetGroup(new DBSubnetGroup()
                        .withDBSubnetGroupName("name")
                        .withSubnetGroupStatus("status")
                        .withVpcId("vpc")
                        .withSubnets(new Subnet()
                                .withSubnetIdentifier("subnet")))
                .withStatusInfos(new DBInstanceStatusInfo()
                        .withStatus("status")
                        .withMessage("message")
                        .withNormal(true)
                        .withStatusType("type"))
                .withEndpoint(new Endpoint()
                        .withAddress("address")
                        .withPort(100)
                        .withHostedZoneId("zone"))
                .withInstanceCreateTime(new Date(100000))
                .withIops(100)
                .withMultiAZ(true)
                .withPubliclyAccessible(true)
                .withTagList(new Tag().withKey("key").withValue("value"));
    }
}
