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
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.Application;
import software.amazon.awssdk.services.emr.model.Cluster;
import software.amazon.awssdk.services.emr.model.ClusterStateChangeReason;
import software.amazon.awssdk.services.emr.model.ClusterStatus;
import software.amazon.awssdk.services.emr.model.ClusterSummary;
import software.amazon.awssdk.services.emr.model.DescribeClusterRequest;
import software.amazon.awssdk.services.emr.model.DescribeClusterResponse;
import software.amazon.awssdk.services.emr.model.ListClustersRequest;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;
import software.amazon.awssdk.services.emr.model.Tag;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EmrClusterTableProviderTest
        extends AbstractTableProviderTest
{
    private static final Logger logger = LoggerFactory.getLogger(EmrClusterTableProviderTest.class);

    @Mock
    private EmrClient mockEmr;

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
                    List<ClusterSummary> values = new ArrayList<>();
                    values.add(makeClusterSummary(getIdValue()));
                    values.add(makeClusterSummary(getIdValue()));
                    values.add(makeClusterSummary("fake-id"));
                    ListClustersResponse mockResult = ListClustersResponse.builder().clusters(values).build();
                    return mockResult;
                });

        when(mockEmr.describeCluster(nullable(DescribeClusterRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    DescribeClusterRequest request = (DescribeClusterRequest) invocation.getArguments()[0];
                    DescribeClusterResponse mockResult = DescribeClusterResponse.builder().cluster(makeCluster(request.clusterId())).build();
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
        return ClusterSummary.builder()
                .name("name")
                .id(id)
                .status(ClusterStatus.builder().state("state")
                        .stateChangeReason(ClusterStateChangeReason.builder()
                                .code("state_code")
                                .message("state_msg").build()).build())
                .normalizedInstanceHours(100).build();
    }

    private Cluster makeCluster(String id)
    {
        return Cluster.builder()
                .id(id)
                .name("name")
                .autoScalingRole("autoscaling_role")
                .customAmiId("custom_ami")
                .instanceCollectionType("instance_collection_type")
                .logUri("log_uri")
                .masterPublicDnsName("master_public_dns")
                .releaseLabel("release_label")
                .runningAmiVersion("running_ami")
                .scaleDownBehavior("scale_down_behavior")
                .serviceRole("service_role")
                .applications(Application.builder().name("name").version("version").build())
                .tags(Tag.builder().key("key").value("value").build())
                .build();
    }
}
