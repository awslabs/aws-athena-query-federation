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
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        final Map<String, Field> fields = new HashMap<>();
        recordsRequest.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

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
                clusterToRow(next, cluster, constraintEvaluator, spiller, fields);
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
     * @param constraintEvaluator The ConstraintEvaluator we can use to filter results.
     * @param spiller The BlockSpiller to use when we want to write a matching row to the response.
     * @param fields The set of fields that need to be projected.
     * @note The current implementation is rather naive in how it maps fields. It leverages a static
     * list of fields that we'd like to provide and then explicitly filters and converts each field.
     */
    private void clusterToRow(ClusterSummary clusterSummary,
            Cluster cluster,
            ConstraintEvaluator constraintEvaluator,
            BlockSpiller spiller,
            Map<String, Field> fields)
    {
        spiller.writeRows((Block block, int row) -> {
            boolean matched = true;

            if (matched && fields.containsKey("id")) {
                String value = clusterSummary.getId();
                matched &= constraintEvaluator.apply("id", value);
                BlockUtils.setValue(block.getFieldVector("id"), row, value);
            }

            if (matched && fields.containsKey("name")) {
                String value = clusterSummary.getName();
                matched &= constraintEvaluator.apply("name", value);
                BlockUtils.setValue(block.getFieldVector("name"), row, value);
            }

            if (matched && fields.containsKey("instance_hours")) {
                Integer value = clusterSummary.getNormalizedInstanceHours();
                matched &= constraintEvaluator.apply("instance_hours", value);
                BlockUtils.setValue(block.getFieldVector("instance_hours"), row, value);
            }

            if (matched && fields.containsKey("state")) {
                String value = clusterSummary.getStatus().getState();
                matched &= constraintEvaluator.apply("state", value);
                BlockUtils.setValue(block.getFieldVector("state"), row, value);
            }

            if (matched && fields.containsKey("state_code")) {
                String value = clusterSummary.getStatus().getStateChangeReason().getCode();
                matched &= constraintEvaluator.apply("state_code", value);
                BlockUtils.setValue(block.getFieldVector("state_code"), row, value);
            }

            if (matched && fields.containsKey("state_msg")) {
                String value = clusterSummary.getStatus().getStateChangeReason().getMessage();
                matched &= constraintEvaluator.apply("state_msg", value);
                BlockUtils.setValue(block.getFieldVector("state_msg"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("autoscaling_role")) {
                String value = cluster.getAutoScalingRole();
                matched &= constraintEvaluator.apply("autoscaling_role", value);
                BlockUtils.setValue(block.getFieldVector("autoscaling_role"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("custom_ami")) {
                String value = cluster.getCustomAmiId();
                matched &= constraintEvaluator.apply("custom_ami", value);
                BlockUtils.setValue(block.getFieldVector("custom_ami"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("instance_collection_type")) {
                String value = cluster.getInstanceCollectionType();
                matched &= constraintEvaluator.apply("instance_collection_type", value);
                BlockUtils.setValue(block.getFieldVector("instance_collection_type"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("log_uri")) {
                String value = cluster.getLogUri();
                matched &= constraintEvaluator.apply("log_uri", value);
                BlockUtils.setValue(block.getFieldVector("log_uri"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("master_public_dns")) {
                String value = cluster.getMasterPublicDnsName();
                matched &= constraintEvaluator.apply("master_public_dns", value);
                BlockUtils.setValue(block.getFieldVector("master_public_dns"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("release_label")) {
                String value = cluster.getReleaseLabel();
                matched &= constraintEvaluator.apply("release_label", value);
                BlockUtils.setValue(block.getFieldVector("release_label"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("running_ami")) {
                String value = cluster.getRunningAmiVersion();
                matched &= constraintEvaluator.apply("running_ami", value);
                BlockUtils.setValue(block.getFieldVector("running_ami"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("scale_down_behavior")) {
                String value = cluster.getScaleDownBehavior();
                matched &= constraintEvaluator.apply("scale_down_behavior", value);
                BlockUtils.setValue(block.getFieldVector("scale_down_behavior"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("service_role")) {
                String value = cluster.getServiceRole();
                matched &= constraintEvaluator.apply("service_role", value);
                BlockUtils.setValue(block.getFieldVector("service_role"), row, value);
            }

            if (cluster != null && matched && fields.containsKey("applications")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("applications");
                List<String> values = cluster.getApplications().stream()
                        .map(next -> next.getName() + ":" + next.getVersion()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, values);
            }

            if (cluster != null && matched && fields.containsKey("tags")) {
                //TODO: apply constraint for complex type
                ListVector vector = (ListVector) block.getFieldVector("tags");
                List<String> interfaces = cluster.getTags().stream()
                        .map(next -> next.getKey() + ":" + next.getValue()).collect(Collectors.toList());
                BlockUtils.setComplexValue(vector, row, FieldResolver.DEFAULT, interfaces);
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
