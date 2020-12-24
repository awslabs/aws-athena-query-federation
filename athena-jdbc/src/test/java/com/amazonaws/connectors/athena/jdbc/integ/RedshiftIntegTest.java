/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.connectors.athena.jdbc.integ;

import com.amazonaws.athena.connector.integ.data.ConnectorVpcAttributes;
import com.amazonaws.athena.connector.integ.stacks.ConnectorWithVpcStack;
import com.amazonaws.athena.connector.integ.IntegrationTestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.SecretValue;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcAttributes;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.redshift.Cluster;
import software.amazon.awscdk.services.redshift.ClusterType;
import software.amazon.awscdk.services.redshift.Login;
import software.amazon.awscdk.services.redshift.NodeType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class RedshiftIntegTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftIntegTest.class);

    private static final String REDSHIFT_DB_NAME = "redshift_movies";
    private static final String REDSHIFT_DB_PORT = "5439";
    private static final String REDSHIFT_DB_USERNAME = "integusername";
    private static final String REDSHIFT_DB_PASSWORD = "IntegPassword1";

    private final String lambdaFunctionName;
    private final String clusterName;
    private final String clusterEndpoint;
    private final String connectionString;
    private final String connectionStringTag;

    public RedshiftIntegTest()
    {
        final UUID uuid = UUID.randomUUID();

        lambdaFunctionName = getLambdaFunctionName();
        clusterName = "redshift-" + uuid;
        clusterEndpoint = clusterName + ".c9afbdb0synx.us-east-1.redshift.amazonaws.com";
        connectionString = String.format("redshift://jdbc:redshift://%s:%s/%s?user=%s&password=%s",
                clusterEndpoint, REDSHIFT_DB_PORT, REDSHIFT_DB_NAME, REDSHIFT_DB_USERNAME, REDSHIFT_DB_PASSWORD);
        connectionStringTag = lambdaFunctionName + "_connection_string";
    }

    /**
     * Sets up the access policy for the Lambda connector to multiple connector-specific AWS services (e.g. DynamoDB,
     * Elasticsearch etc...)
     * @return A policy document object.
     */
    @Override
    protected Optional<PolicyDocument> getConnectorAccessPolicy()
    {
        // No connector-specific policy document needed
        return Optional.empty();
    }

    /**
     * Sets the environment variables for the Lambda function.
     */
    @Override
    protected void setConnectorEnvironmentVars(final Map environmentVars)
    {
        environmentVars.put("default", connectionString);
        environmentVars.put(connectionStringTag, connectionString);
    }

    /**
     * Sets up the Redshift Table's CloudFormation stack.
     * @param stack The current CloudFormation stack.
     */
    @Override
    protected void setUpStackData(final Stack stack)
    {
        ConnectorVpcAttributes vpcAttributes = ((ConnectorWithVpcStack) stack).getConnectorVpcAttributes();

        Cluster.Builder.create(stack, "RedshiftCluster")
                .publiclyAccessible(Boolean.TRUE)
                .removalPolicy(RemovalPolicy.DESTROY)
                .encrypted(Boolean.FALSE)
                .port(Integer.parseInt(REDSHIFT_DB_PORT))
                .clusterName(clusterName)
                .clusterType(ClusterType.SINGLE_NODE)
                .nodeType(NodeType.DC2_LARGE)
                .numberOfNodes(1)
                .defaultDatabaseName(REDSHIFT_DB_NAME)
                .masterUser(Login.builder()
                        .masterUsername(REDSHIFT_DB_USERNAME)
                        .masterPassword(SecretValue.plainText(REDSHIFT_DB_PASSWORD))
                        .build())
                .vpc(Vpc.fromVpcAttributes(stack, "RedshiftVpcConfig", VpcAttributes.builder()
                        .vpcId(vpcAttributes.getVpcId())
                        .privateSubnetIds(vpcAttributes.getPrivateSubnetIds())
                        .availabilityZones(vpcAttributes.getAvailabilityZones())
                        .build()))
                .securityGroups(Collections.singletonList(SecurityGroup
                        .fromSecurityGroupId(stack, "RedshiftVpcSecurityGroup", vpcAttributes.getSecurityGroupId())))
                .build();
    }

    /**
     * This connector supports a VPC configuration.
     * @return true
     */
    @Override
    protected boolean connectorSupportsVpcConfig()
    {
        return true;
    }

    /**
     * Insert rows into the newly created DDB table.
     */
    @Override
    protected void setUpTableData()
    {
        logger.info("----------------------------------------------------");
//        logger.info("Setting up DB table: {}", tableName);
        logger.info("Redshift Cluster Endpoint: {}", clusterEndpoint);
        logger.info("----------------------------------------------------");
// redshift-73d78d54-1435-48ad-979e-81741a5d00f3
// redshift-73d78d54-1435-48ad-979e-81741a5d00f3.c9afbdb0synx.us-east-1.redshift.amazonaws.com:5439/redshift_movies
    }

    @Test
    public void listDatabasesIntegTest()
    {
        logger.info("--------------------------------------");
        logger.info("Executing listDatabasesIntegTest");
        logger.info("--------------------------------------");

        List dbNames = listDatabases();
        logger.info("Databases: {}", dbNames);
        assertTrue("DB not found.", dbNames.contains("public"));
    }
}
