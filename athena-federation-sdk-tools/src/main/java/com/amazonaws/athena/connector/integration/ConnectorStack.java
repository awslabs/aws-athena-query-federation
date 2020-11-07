/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awscdk.core.CfnParameter;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.CfnParametersCodeProps;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Sets up the CloudFormation stack necessary for a Lambda Connector.
 */
public class ConnectorStack extends Stack
{
    private final String spillBucket;
    private final String s3Key;
    private final String functionName;
    private final String functionHandler;
    private final PolicyDocument connectorAccessPolicy;
    private final Map environmentVariables;

    public ConnectorStack(final Construct scope, final String id, final String spillBucket, final String s3Key,
                          final String functionName, final String functionHandler,
                          final PolicyDocument connectorAccessPolicy, final Map environmentVariables)
    {
        super(scope, id);

        this.spillBucket = spillBucket;
        this.s3Key = s3Key;
        this.functionName = functionName;
        this.functionHandler = functionHandler;
        this.connectorAccessPolicy = connectorAccessPolicy;
        this.environmentVariables = environmentVariables;

        setConnectorStack();
    }

    /**
     * Sets up the Connector's CloudFormation stack.
     */
    private void setConnectorStack()
    {
        Function.Builder.create(this, "LambdaConnector")
                .functionName(functionName)
                .role(getIamRole())
                .code(Code.fromCfnParameters(CfnParametersCodeProps.builder()
                        .bucketNameParam(CfnParameter.Builder.create(this, "BucketName")
                                .defaultValue(spillBucket)
                                .build())
                        .objectKeyParam(CfnParameter.Builder.create(this, "BucketKey")
                                .defaultValue(s3Key)
                                .build())
                        .build()))
                .handler(functionHandler)
                .runtime(new Runtime("java8"))
                .memorySize(Integer.valueOf(3008))
                .timeout(Duration.seconds(Integer.valueOf(900)))
                .environment(environmentVariables)
                .build();
    }

    /**
     * Sets up the IAM role for the Lambda function.
     * @return IAM Role object.
     */
    private Role getIamRole()
    {
        return Role.Builder.create(this, "ConnectorConfigRole")
                .assumedBy(ServicePrincipal.Builder.create("lambda.amazonaws.com").build())
                .inlinePolicies(ImmutableMap.of(
                        "ConnectorAccessPolicy", connectorAccessPolicy,
                        "GlueAthenaS3AccessPolicy", getGlueAthenaS3AccessPolicy(),
                        "S3BucketAccessPolicy", getS3SpillBucketAccessPolicy()))
                .build();
    }

    /**
     * Sets up Glue, Athena, and S3 access policy for the Lambda connector.
     * @return A policy document object.
     */
    private PolicyDocument getGlueAthenaS3AccessPolicy()
    {
        List<String> statementActionsPolicy = new ArrayList<>();
        statementActionsPolicy.add("glue:GetTableVersions");
        statementActionsPolicy.add("glue:GetPartitions");
        statementActionsPolicy.add("glue:GetTables");
        statementActionsPolicy.add("glue:GetTableVersion");
        statementActionsPolicy.add("glue:GetDatabases");
        statementActionsPolicy.add("glue:GetTable");
        statementActionsPolicy.add("glue:GetPartition");
        statementActionsPolicy.add("glue:GetDatabase");
        statementActionsPolicy.add("athena:GetQueryExecution");
        statementActionsPolicy.add("s3:ListAllMyBuckets");

        return PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(statementActionsPolicy)
                        .resources(ImmutableList.of("*"))
                        .effect(Effect.ALLOW)
                        .build()))
                .build();
    }

    /**
     * Sets up the S3 spill-bucket access policy for the Lambda connector.
     * @return A policy document object.
     */
    private PolicyDocument getS3SpillBucketAccessPolicy()
    {
        List<String> statementActionsPolicy = new ArrayList<>();
        statementActionsPolicy.add("s3:GetObject");
        statementActionsPolicy.add("s3:ListBucket");
        statementActionsPolicy.add("s3:GetBucketLocation");
        statementActionsPolicy.add("s3:GetObjectVersion");
        statementActionsPolicy.add("s3:PutObject");
        statementActionsPolicy.add("s3:PutObjectAcl");
        statementActionsPolicy.add("s3:GetLifecycleConfiguration");
        statementActionsPolicy.add("s3:PutLifecycleConfiguration");
        statementActionsPolicy.add("s3:DeleteObject");

        return PolicyDocument.Builder.create()
                .statements(ImmutableList.of(PolicyStatement.Builder.create()
                        .actions(statementActionsPolicy)
                        .resources(ImmutableList.of(
                                String.format("arn:aws:s3:::%s", spillBucket),
                                String.format("arn:aws:s3:::%s/*", spillBucket)))
                        .effect(Effect.ALLOW)
                        .build()))
                .build();
    }
}
