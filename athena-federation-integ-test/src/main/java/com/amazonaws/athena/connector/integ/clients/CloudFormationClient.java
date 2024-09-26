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
package com.amazonaws.athena.connector.integ.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.internal.collections.Pair;
import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Stack;
import software.amazon.awssdk.services.cloudformation.model.Capability;
import software.amazon.awssdk.services.cloudformation.model.CreateStackRequest;
import software.amazon.awssdk.services.cloudformation.model.CreateStackResponse;
import software.amazon.awssdk.services.cloudformation.model.DeleteStackRequest;
import software.amazon.awssdk.services.cloudformation.model.DescribeStackEventsRequest;
import software.amazon.awssdk.services.cloudformation.model.DescribeStackEventsResponse;
import software.amazon.awssdk.services.cloudformation.model.ResourceStatus;
import software.amazon.awssdk.services.cloudformation.model.StackEvent;

import java.util.List;

/**
 * Responsible for creating the CloudFormation stack needed to test the connector, and unwinding it once testing is
 * done.
 */
public class CloudFormationClient
{
    private static final Logger logger = LoggerFactory.getLogger(CloudFormationClient.class);

    private static final long sleepTimeMillis = 5000L;

    private final String stackName;
    private final String stackTemplate;
    private final software.amazon.awssdk.services.cloudformation.CloudFormationClient cloudFormationClient;

    public CloudFormationClient(Pair<App, Stack> stackPair)
    {
        this(stackPair.first(), stackPair.second());
    }

    public CloudFormationClient(App theApp, Stack theStack)
    {
        stackName = theStack.getStackName();
        ObjectMapper objectMapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
        stackTemplate = objectMapper
                .valueToTree(theApp.synth().getStackArtifact(theStack.getArtifactId()).getTemplate())
                .toPrettyString();
        this.cloudFormationClient = software.amazon.awssdk.services.cloudformation.CloudFormationClient.create();
    }

    /**
     * Creates a CloudFormation stack to build the infrastructure needed to run the integration tests (e.g., Database
     * instance, Lambda function, etc...). Once the stack is created successfully, the lambda function is registered
     * with Athena.
     */
    public void createStack()
    {
        logger.info("------------------------------------------------------");
        logger.info("Create CloudFormation stack: {}", stackName);
        logger.info("------------------------------------------------------");
        // logger.info(stackTemplate);

        CreateStackRequest createStackRequest = CreateStackRequest.builder()
                .stackName(stackName)
                .templateBody(stackTemplate)
                .disableRollback(true)
                .capabilities(Capability.CAPABILITY_NAMED_IAM)
                .build();
        processCreateStackRequest(createStackRequest);
    }

    /**
     * Processes the creation of a CloudFormation stack including polling of the stack's status while in progress.
     * @param createStackRequest Request used to generate the CloudFormation stack.
     * @throws RuntimeException The CloudFormation stack creation failed.
     */
    private void processCreateStackRequest(CreateStackRequest createStackRequest)
            throws RuntimeException
    {
        // Create CloudFormation stack.
        CreateStackResponse response = cloudFormationClient.createStack(createStackRequest);
        logger.info("Stack ID: {}", response.stackId());

        DescribeStackEventsRequest describeStackEventsRequest = DescribeStackEventsRequest.builder()
                .stackName(createStackRequest.stackName())
                .build();
        DescribeStackEventsResponse describeStackEventsResponse;

        // Poll status of stack until stack has been created or creation has failed
        while (true) {
            describeStackEventsResponse = cloudFormationClient.describeStackEvents(describeStackEventsRequest);
            StackEvent event = describeStackEventsResponse.stackEvents().get(0);
            String resourceId = event.logicalResourceId();
            ResourceStatus resourceStatus = event.resourceStatus();
            logger.info("Resource Id: {}, Resource status: {}", resourceId, resourceStatus);
            if (!resourceId.equals(event.stackName()) ||
                    resourceStatus.equals(ResourceStatus.CREATE_IN_PROGRESS)) {
                try {
                    Thread.sleep(sleepTimeMillis);
                    continue;
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage(), e);
                }
            }
            else if (resourceStatus.equals(ResourceStatus.CREATE_FAILED)) {
                throw new RuntimeException(getCloudFormationErrorReasons(describeStackEventsResponse.stackEvents()));
            }
            break;
        }
    }

    /**
     * Provides a detailed error message when the CloudFormation stack creation fails.
     * @param stackEvents The list of CloudFormation stack events.
     * @return String containing the formatted error message.
     */
    private String getCloudFormationErrorReasons(List<StackEvent> stackEvents)
    {
        StringBuilder errorMessageBuilder =
                new StringBuilder("CloudFormation stack creation failed due to the following reason(s):\n");

        stackEvents.forEach(stackEvent -> {
            if (stackEvent.resourceStatus().equals(ResourceStatus.CREATE_FAILED)) {
                String errorMessage = String.format("Resource: %s, Reason: %s\n",
                        stackEvent.logicalResourceId(), stackEvent.resourceStatusReason());
                errorMessageBuilder.append(errorMessage);
            }
        });

        return errorMessageBuilder.toString();
    }

    /**
     * Deletes a CloudFormation stack, and the lambda function registered with Athena.
     */
    public void deleteStack()
    {
        logger.info("------------------------------------------------------");
        logger.info("Delete CloudFormation stack: {}", stackName);
        logger.info("------------------------------------------------------");

        try {
            DeleteStackRequest request = DeleteStackRequest.builder().stackName(stackName).build();
            cloudFormationClient.deleteStack(request);
        }
        catch (Exception e) {
            logger.error("Something went wrong... Manual resource cleanup may be needed!!!", e);
        }
        finally {
            cloudFormationClient.close();
        }
    }
}
