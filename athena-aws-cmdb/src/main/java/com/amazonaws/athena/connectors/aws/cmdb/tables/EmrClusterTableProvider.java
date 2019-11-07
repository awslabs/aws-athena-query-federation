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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps your EMR Clusters to a table.
 */
public class EmrClusterTableProvider
        implements TableProvider
{
    private static final Schema SCHEMA;
    private AmazonElasticMapReduce emr;

    public EmrClusterTableProvider(AmazonElasticMapReduce emr)
    {
        this.emr = emr;
    }

    /**
     * @See TableProvider
     */
    @Override
    public String getSchema()
    {
        return "emr";
    }

    /**
     * @See TableProvider
     */
    @Override
    public TableName getTableName()
    {
        return new TableName(getSchema(), "emr_clusters");
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
     * Calls ListClusters and DescribeCluster on the AWS EMR Client returning all clusters that match the supplied
     * predicate and attempting to push down certain predicates (namely queries for specific cluster) to EC2.
     *
     * @See TableProvider
     */
    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        boolean done = false;
        ListClustersRequest request = new ListClustersRequest();

        while (!done) {
            ListClustersResult response = emr.listClusters(request);

            for (ClusterSummary next : response.getClusters()) {
                Cluster cluster = null;
                if (!next.getStatus().getState().toLowerCase().contains("terminated")) {
                    DescribeClusterResult clusterResponse = emr.describeCluster(new DescribeClusterRequest().withClusterId(next.getId()));
                    cluster = clusterResponse.getCluster();
                }
                clusterToRow(next, cluster, spiller);
            }

            request.setMarker(response.getMarker());

            if (response.getMarker() == null) {
                done = true;
            }
        }
    }

    /**
     * Maps an EBS Volume into a row in our Apache Arrow response block(s).
     *
     * @param clusterSummary The CluserSummary for the provided Cluster.
     * @param cluster The EMR Cluster to map.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void clusterToRow(ClusterSummary clusterSummary,
            Cluster cluster,
            BlockSpiller spiller)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            matched &= block.offerValue("id", row, clusterSummary.getId());
            matched &= block.offerValue("name", row, clusterSummary.getName());
            matched &= block.offerValue("instance_hours", row, clusterSummary.getNormalizedInstanceHours());
            matched &= block.offerValue("state", row, clusterSummary.getStatus().getState());
            matched &= block.offerValue("state_code", row, clusterSummary.getStatus().getStateChangeReason().getCode());
            matched &= block.offerValue("state_msg", row, clusterSummary.getStatus().getStateChangeReason().getMessage());

            if (cluster != null) {
                matched &= block.offerValue("autoscaling_role", row, cluster.getAutoScalingRole());
                matched &= block.offerValue("custom_ami", row, cluster.getCustomAmiId());
                matched &= block.offerValue("instance_collection_type", row, cluster.getInstanceCollectionType());
                matched &= block.offerValue("log_uri", row, cluster.getLogUri());
                matched &= block.offerValue("master_public_dns", row, cluster.getMasterPublicDnsName());
                matched &= block.offerValue("release_label", row, cluster.getReleaseLabel());
                matched &= block.offerValue("running_ami", row, cluster.getRunningAmiVersion());
                matched &= block.offerValue("scale_down_behavior", row, cluster.getScaleDownBehavior());
                matched &= block.offerValue("service_role", row, cluster.getServiceRole());
                matched &= block.offerValue("service_role", row, cluster.getServiceRole());

                List<String> applications = cluster.getApplications().stream()
                        .map(next -> next.getName() + ":" + next.getVersion()).collect(Collectors.toList());
                matched &= block.offerComplexValue("applications", row, FieldResolver.DEFAULT, applications);

                List<String> tags = cluster.getTags().stream()
                        .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
                matched &= block.offerComplexValue("tags", row, FieldResolver.DEFAULT, tags);
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
                .addStringField("name")
                .addIntField("instance_hours")
                .addStringField("state")
                .addStringField("state_code")
                .addStringField("state_msg")
                .addStringField("autoscaling_role")
                .addStringField("custom_ami")
                .addStringField("instance_collection_type")
                .addStringField("log_uri")
                .addStringField("master_public_dns")
                .addStringField("release_label")
                .addStringField("running_ami")
                .addStringField("scale_down_behavior")
                .addStringField("service_role")
                .addListField("applications", Types.MinorType.VARCHAR.getType())
                .addListField("tags", Types.MinorType.VARCHAR.getType())
                .addMetadata("id", "Cluster Id")
                .addMetadata("name", "Cluster Name")
                .addMetadata("state", "State of the cluster.")
                .addMetadata("state_code", "Code associated with the state of the cluster.")
                .addMetadata("state_msg", "Message associated with the state of the cluster.")
                .addMetadata("autoscaling_role", "AutoScaling role used by the cluster.")
                .addMetadata("custom_ami", "Custom AMI used by the cluster (if any)")
                .addMetadata("instance_collection_type", "Instance collection type used by the cluster.")
                .addMetadata("log_uri", "URI where debug logs can be found for the cluster.")
                .addMetadata("master_public_dns", "Public DNS name of the master node.")
                .addMetadata("release_label", "EMR release label the cluster is running.")
                .addMetadata("running_ami", "AMI the cluster are running.")
                .addMetadata("scale_down_behavior", "Scale down behavoir of the cluster.")
                .addMetadata("applications", "The EMR applications installed on the cluster.")
                .addMetadata("tags", "Tags associated with the volume.")
                .build();
    }
}
