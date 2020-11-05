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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

public class ConnectorStack extends Stack
{
    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    private final String spillBucket;
    private final String s3Key;
    private final Role iamRole;
    private final String functionName;
    private final String functionHandler;
    private final Map environmentVariables;

    private ConnectorStack(Builder builder)
    {
        super(builder.scope, builder.id);

        spillBucket = builder.spillBucket;
        s3Key = builder.s3Key;

        iamRole = setIamRole();
        functionName = builder.functionName;
        functionHandler = builder.functionHandler;
        environmentVariables = builder.environmentVariables;

        setLambdaFunction();
    }

    private Role setIamRole()
    {
        List<String> statementActionsPolicy0 = new ArrayList<>();
        statementActionsPolicy0.add("dynamodb:DescribeTable");
        statementActionsPolicy0.add("dynamodb:ListSchemas");
        statementActionsPolicy0.add("dynamodb:ListTables");
        statementActionsPolicy0.add("dynamodb:Query");
        statementActionsPolicy0.add("dynamodb:Scan");
        statementActionsPolicy0.add("glue:GetTableVersions");
        statementActionsPolicy0.add("glue:GetPartitions");
        statementActionsPolicy0.add("glue:GetTables");
        statementActionsPolicy0.add("glue:GetTableVersion");
        statementActionsPolicy0.add("glue:GetDatabases");
        statementActionsPolicy0.add("glue:GetTable");
        statementActionsPolicy0.add("glue:GetPartition");
        statementActionsPolicy0.add("glue:GetDatabase");
        statementActionsPolicy0.add("athena:GetQueryExecution");
        statementActionsPolicy0.add("s3:ListAllMyBuckets");

        PolicyStatement statement0 = PolicyStatement.Builder.create()
                .actions(statementActionsPolicy0)
                .resources(ImmutableList.of("*"))
                .effect(Effect.ALLOW)
                .build();

        PolicyDocument document0 = PolicyDocument.Builder.create()
                .statements(ImmutableList.of(statement0))
                .build();

        List<String> statementActionsPolicy1 = new ArrayList<>();
        statementActionsPolicy1.add("s3:GetObject");
        statementActionsPolicy1.add("s3:ListBucket");
        statementActionsPolicy1.add("s3:GetBucketLocation");
        statementActionsPolicy1.add("s3:GetObjectVersion");
        statementActionsPolicy1.add("s3:PutObject");
        statementActionsPolicy1.add("s3:PutObjectAcl");
        statementActionsPolicy1.add("s3:GetLifecycleConfiguration");
        statementActionsPolicy1.add("s3:PutLifecycleConfiguration");
        statementActionsPolicy1.add("s3:DeleteObject");

        PolicyStatement statement1 = PolicyStatement.Builder.create()
                .actions(statementActionsPolicy1)
                .resources(ImmutableList.of(
                        String.format("arn:aws:s3:::%s", spillBucket),
                        String.format("arn:aws:s3:::%s/*", spillBucket)))
                .effect(Effect.ALLOW)
                .build();

        PolicyDocument document1 = PolicyDocument.Builder.create()
                .statements(ImmutableList.of(statement1))
                .build();

        Role role = Role.Builder.create(this, "ConnectorConfigRole")
                .assumedBy(ServicePrincipal.Builder.create("lambda.amazonaws.com").build())
                .inlinePolicies(ImmutableMap.of(
                        "ConnectorConfigRolePolicy0", document0,
                        "ConnectorConfigRolePolicy1", document1))
                .build();

        return role;
    }

    private void setLambdaFunction()
    {
        Function.Builder.create(this, "LambdaConnector")
                .functionName(functionName)
                .role(iamRole)
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

    public static class Builder
    {
        private final Construct scope;
        private final String id;
        private String spillBucket;
        private String s3Key;
        private String functionName;
        private String functionHandler;
        private Map environmentVariables;

        public Builder(final Construct scope, final String id)
        {
            this.scope = scope;
            this.id = id;
        }

        public Builder withSpillBucket(final String spillBucket, final String s3Key)
        {
            this.spillBucket = spillBucket;
            this.s3Key = s3Key;

            return this;
        }

        public Builder withFunctionProperties(final String functionName, final String functionHandler,
                                              final Map<String, String> environmentVariables)
        {
            this.functionName = functionName;
            this.functionHandler = functionHandler;
            this.environmentVariables = environmentVariables;

            return this;
        }

        public ConnectorStack build()
        {
            return new ConnectorStack(this);
        }
    }
}
