/*-
 * #%L
 * Amazon Athena Query Federation Integ Test
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
package com.amazonaws.athena.connector.integ.stacks;

import com.amazonaws.athena.connector.integ.data.ConnectorPackagingAttributes;
import com.amazonaws.athena.connector.integ.data.ConnectorStackAttributes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awscdk.core.CfnParameter;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.athena.CfnDataCatalog;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Sets up the CloudFormation stack necessary for a Lambda Connector.
 */
public class ConnectorStack extends Stack
{
    private final String s3Bucket;
    private final String s3Key;
    private final String functionHandler;
    private final String functionName;
    private final Optional<PolicyDocument> connectorAccessPolicy;
    private final Map environmentVariables;

    public ConnectorStack(Builder builder)
    {
        super(builder.scope, builder.id);

        this.s3Bucket = builder.connectorPackagingAttributes.getS3Bucket();
        this.s3Key = builder.connectorPackagingAttributes.getS3Key();
        this.functionHandler = builder.connectorPackagingAttributes.getLambdaFunctionHandler();
        this.functionName = builder.functionName;
        this.connectorAccessPolicy = builder.connectorAccessPolicy;
        this.environmentVariables = builder.environmentVariables;
    }

    /**
     * Initialize the stack by building the Lambda function and Athena data catalog.
     */
    protected void initialize()
    {
        buildLambdaFunction();
        buildAthenaDataCatalog();
    }

    /**
     * Builds the Connector's CloudFormation stack for the lambda function.
     */
    private void buildLambdaFunction()
    {
        createLambdaFunction().build();
    }

    /**
     * Creates the Lambda function stack resource.
     * @return Lambda function Builder.
     */
    protected Function.Builder createLambdaFunction()
    {
        return Function.Builder.create(this, "LambdaConnector")
                .functionName(functionName)
                .role(buildIamRole())
                .code(Code.fromCfnParameters(CfnParametersCodeProps.builder()
                        .bucketNameParam(CfnParameter.Builder.create(this, "BucketName")
                                .defaultValue(s3Bucket)
                                .build())
                        .objectKeyParam(CfnParameter.Builder.create(this, "BucketKey")
                                .defaultValue(s3Key)
                                .build())
                        .build()))
                .handler(functionHandler)
                .runtime(new Runtime("java8"))
                .memorySize(Integer.valueOf(3008))
                .timeout(Duration.seconds(Integer.valueOf(900)))
                .environment(environmentVariables);
    }

    /**
     * Builds the Connector's CloudFormation stack to register the lambda function with Athena.
     */
    private void buildAthenaDataCatalog()
    {
        createAthenaDataCatalog().build();
    }

    /**
     * Creates the Athena data catalog stack resource.
     * @return Athena data catalog Builder.
     */
    protected CfnDataCatalog.Builder createAthenaDataCatalog()
    {
        return CfnDataCatalog.Builder.create(this, "AthenaDataCatalog")
                .name(functionName)
                .type("LAMBDA")
                .parameters(ImmutableMap.of("function", "arn:aws:lambda:function:" + functionName));
    }

    /**
     * Builds the IAM role for the Lambda function.
     * @return IAM Role object.
     */
    private Role buildIamRole()
    {
        return createIamRole().build();
    }

    /**
     * Creates the IAM role stack resource.
     * @return IAM role Builder.
     */
    protected Role.Builder createIamRole()
    {
        Map<String, PolicyDocument> policies = new HashMap<>();

        setAccessPolicies(policies);

        return Role.Builder.create(this, "ConnectorConfigRole")
                .assumedBy(ServicePrincipal.Builder.create("lambda.amazonaws.com").build())
                .inlinePolicies(policies);
    }

    /**
     * Sets the access policies used by the Lambda function.
     * @param policies A map of access policies.
     */
    protected void setAccessPolicies(Map<String, PolicyDocument> policies)
    {
        policies.put("GlueAthenaS3AccessPolicy", getGlueAthenaS3AccessPolicy());
        policies.put("S3SpillBucketAccessPolicy", getS3SpillBucketAccessPolicy());
        connectorAccessPolicy.ifPresent(policyDocument -> policies.put("ConnectorAccessPolicy", policyDocument));
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
                                String.format("arn:aws:s3:::%s", s3Bucket),
                                String.format("arn:aws:s3:::%s/*", s3Bucket)))
                        .effect(Effect.ALLOW)
                        .build()))
                .build();
    }

    public static class Builder
    {
        private final Construct scope;
        private final String id;
        private final String functionName;
        private final Optional<PolicyDocument> connectorAccessPolicy;
        private final Map environmentVariables;
        private final ConnectorPackagingAttributes connectorPackagingAttributes;

        public Builder(ConnectorStackAttributes attributes)
        {
            this.scope = attributes.getScope();
            this.id = attributes.getId();
            this.functionName = attributes.getLambdaFunctionName();
            this.connectorAccessPolicy = attributes.getConnectorAccessPolicy();
            this.environmentVariables = attributes.getEnvironmentVariables();
            this.connectorPackagingAttributes = attributes.getConnectorPackagingAttributes();
        }

        public Stack build()
        {
            ConnectorStack stack = new ConnectorStack(this);
            stack.initialize();
            return stack;
        }
    }
}
