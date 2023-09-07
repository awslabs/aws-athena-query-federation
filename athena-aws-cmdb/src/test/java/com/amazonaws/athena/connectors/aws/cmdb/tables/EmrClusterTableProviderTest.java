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
import com.amazonaws.services.elasticmapreduce.model.Tag;
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
public class EmrClusterTableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(EmrClusterTableProviderTest.class);

    @Mock
    private AmazonElasticMapReduce mockEmr;

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
        return "emr";
    }

    protected String getExpectedTable()
    {
        return "emr_clusters";
    }

    protected int getExpectedRows()
    {
        return 2;
    }

    protected TableProvider setUpSource()
    {
        return new EmrClusterTableProvider(mockEmr);
    }

    @Override
    protected void setUpRead()
    {
        when(mockEmr.listClusters(nullable(ListClustersRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    ListClustersResult mockResult = mock(ListClustersResult.class);
                    List<ClusterSummary> values = new ArrayList<>();
                    values.add(makeClusterSummary(getIdValue()));
                    values.add(makeClusterSummary(getIdValue()));
                    values.add(makeClusterSummary("fake-id"));
                    when(mockResult.getClusters()).thenReturn(values);
                    return mockResult;
                });

        when(mockEmr.describeCluster(nullable(DescribeClusterRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    DescribeClusterRequest request = (DescribeClusterRequest) invocation.getArguments()[0];
                    DescribeClusterResult mockResult = mock(DescribeClusterResult.class);
                    List<ClusterSummary> values = new ArrayList<>();
                    values.add(makeClusterSummary(getIdValue()));
                    values.add(makeClusterSummary(getIdValue()));
                    values.add(makeClusterSummary("fake-id"));
                    when(mockResult.getCluster()).thenReturn(makeCluster(request.getClusterId()));
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
                if (field.getName().equals("$data$") || field.getName().equals("direction")) {
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

    private ClusterSummary makeClusterSummary(String id)
    {
        return new ClusterSummary()
                .withName("name")
                .withId(id)
                .withStatus(new ClusterStatus()
                        .withState("state")
                        .withStateChangeReason(new ClusterStateChangeReason()
                                .withCode("state_code")
                                .withMessage("state_msg")))
                .withNormalizedInstanceHours(100);
    }

    private Cluster makeCluster(String id)
    {
        return new Cluster()
                .withId(id)
                .withName("name")
                .withAutoScalingRole("autoscaling_role")
                .withCustomAmiId("custom_ami")
                .withInstanceCollectionType("instance_collection_type")
                .withLogUri("log_uri")
                .withMasterPublicDnsName("master_public_dns")
                .withReleaseLabel("release_label")
                .withRunningAmiVersion("running_ami")
                .withScaleDownBehavior("scale_down_behavior")
                .withServiceRole("service_role")
                .withApplications(new Application().withName("name").withVersion("version"))
                .withTags(new Tag("key", "value"));
    }
}
