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

import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
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
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EmrClusterTableProviderTest
        extends AbstractTableProviderTest
{
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

    @Override
    protected boolean directionColumnNotNullOnly()
    {
        return true;
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
