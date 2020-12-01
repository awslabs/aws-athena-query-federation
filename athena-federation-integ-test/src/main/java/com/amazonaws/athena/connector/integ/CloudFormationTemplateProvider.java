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
package com.amazonaws.athena.connector.integ;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.services.iam.PolicyDocument;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class CloudFormationTemplateProvider
{
    private static final Logger logger = LoggerFactory.getLogger(IntegrationTestBase.class);

    private static final String CF_TEMPLATE_NAME = "packaged.yaml";
    private static final String LAMBDA_CODE_URI_TAG = "CodeUri:";
    private static final String LAMBDA_SPILL_BUCKET_PREFIX = "s3://";
    private static final String LAMBDA_HANDLER_TAG = "Handler:";
    private static final String LAMBDA_HANDLER_PREFIX = "Handler: ";
    private static final String LAMBDA_SPILL_BUCKET_TAG = "spill_bucket";
    private static final String LAMBDA_SPILL_PREFIX_TAG = "spill_prefix";
    private static final String LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG = "disable_spill_encryption";

    private final String cloudFormationStackName;
    private final App theApp;
    private final ObjectMapper objectMapper;
    private final String lambdaFunctionName;
    private final PolicyDocument accessPolicy;
    private final Map environmentVars;

    private String lambdaFunctionHandler;
    private String spillBucket;
    private String s3Key;

    public CloudFormationTemplateProvider(String stackName)
    {
        setupLambdaFunctionInfo();

        final UUID randomUuid = UUID.randomUUID();
        theApp = new App();
        objectMapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
        cloudFormationStackName = "integration-" + stackName + "-" + randomUuid;
        lambdaFunctionName = stackName.toLowerCase() + "_" + randomUuid.toString().replace('-', '_');
        accessPolicy = getAccessPolicy();
        environmentVars = getEnvironmentVars();
    }

    /**
     * Sets several variables needed in the creation of the CF Stack (e.g. spillBucket, s3Key, lambdaFunctionHandler).
     * @throws RuntimeException CloudFormation template (packaged.yaml) was not found.
     */
    private void setupLambdaFunctionInfo()
            throws RuntimeException
    {
        try {
            for (String line : Files.readAllLines(Paths.get(CF_TEMPLATE_NAME), StandardCharsets.UTF_8)) {
                if (line.contains(LAMBDA_CODE_URI_TAG)) {
                    spillBucket = line.substring(line.indexOf(LAMBDA_SPILL_BUCKET_PREFIX) +
                            LAMBDA_SPILL_BUCKET_PREFIX.length(), line.lastIndexOf('/'));
                    s3Key = line.substring(line.lastIndexOf('/') + 1);
                }
                else if (line.contains(LAMBDA_HANDLER_TAG)) {
                    lambdaFunctionHandler = line.substring(line.indexOf(LAMBDA_HANDLER_PREFIX) +
                            LAMBDA_HANDLER_PREFIX.length());
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Lambda connector has not been packaged via `sam package` (see README).", e);
        }

        logger.info("Spill Bucket: [{}], S3 Key: [{}], Handler: [{}]", spillBucket, s3Key, lambdaFunctionHandler);
    }

    /**
     * Must be overridden to facilitate getting the lambda function's IAM access policy. The latter sets up
     * access to multiple connector-specific AWS services (e.g. DynamoDB, Elasticsearch etc...)
     * @return A policy document object.
     */
    protected abstract PolicyDocument getAccessPolicy();

    /**
     * Must be overridden to facilitate the setting of the lambda function's environment variables key-value pairs
     * (e.g. "spill_bucket":"myspillbucket"). See individual connector for expected environment variables. This method
     * can be a no-op in the extending class since some environment variables are set by default (spill_bucket,
     * spill_prefix, and disable_spill_encryption).
     */
    protected abstract void setEnvironmentVars(final Map environmentVars);

    /**
     * Must be overridden (can be a no-op) to facilitate the creation of a connector-specific CloudFormation stack
     * resource (e.g. DB table) using AWS CDK.
     * @param stack The current CloudFormation stack.
     */
    protected abstract void setSpecificResource(final Stack stack);

    /**
     * Gets the CloudFormation stack name.
     * @return The stack's name.
     */
    protected String getStackName()
    {
        return cloudFormationStackName;
    }

    /**
     * Gets the name of the lambda function generated by the Integration-Test module.
     * @return The name of the lambda function.
     */
    protected String getLambdaFunctionName()
    {
        return lambdaFunctionName;
    }

    /**
     * Gets the CloudFormation stack template.
     * @return The stack template as String.
     */
    protected String getTemplate()
    {
        return generateTemplate();
    }

    /**
     * Gets the CloudFormation template (generated programmatically using AWS CDK).
     * @return CloudFormation stack template.
     */
    private String generateTemplate()
    {
        final Stack stack = generateStack();
        final String stackTemplate = objectMapper
                .valueToTree(theApp.synth().getStackArtifact(stack.getArtifactId()).getTemplate())
                .toPrettyString();
        logger.info("CloudFormation Template:\n{}: {}", cloudFormationStackName, stackTemplate);

        return stackTemplate;
    }

    /**
     * Generate the CloudFormation stack programmatically using AWS CDK.
     * @return CloudFormation stack object.
     */
    private Stack generateStack()
    {
        final Stack stack = new ConnectorStack(theApp, cloudFormationStackName, spillBucket, s3Key,
                lambdaFunctionName, lambdaFunctionHandler, accessPolicy, environmentVars);
        setSpecificResource(stack);

        return stack;
    }

    /**
     * Gets the specific connectors' environment variables.
     * @return A Map containing the environment variables key-value pairs.
     */
    private Map getEnvironmentVars()
    {
        final Map<String, String> environmentVars = new HashMap<>();
        // Have the connector set specific environment variables first.
        setEnvironmentVars(environmentVars);

        // Check for missing spill_bucket
        if (!environmentVars.containsKey(LAMBDA_SPILL_BUCKET_TAG)) {
            // Add missing spill_bucket environment variable
            environmentVars.put(LAMBDA_SPILL_BUCKET_TAG, spillBucket);
        }

        // Check for missing spill_prefix
        if (!environmentVars.containsKey(LAMBDA_SPILL_PREFIX_TAG)) {
            // Add missing spill_prefix environment variable
            environmentVars.put(LAMBDA_SPILL_PREFIX_TAG, "athena-spill");
        }

        // Check for missing disable_spill_encryption environment variable
        if (!environmentVars.containsKey(LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG)) {
            // Add missing disable_spill_encryption environment variable
            environmentVars.put(LAMBDA_DISABLE_SPILL_ENCRYPTION_TAG, "false");
        }

        return environmentVars;
    }
}
