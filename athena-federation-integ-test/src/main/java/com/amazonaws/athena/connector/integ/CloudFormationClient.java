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

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.Capability;
import com.amazonaws.services.cloudformation.model.CreateStackRequest;
import com.amazonaws.services.cloudformation.model.CreateStackResult;
import com.amazonaws.services.cloudformation.model.DeleteStackRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackEventsRequest;
import com.amazonaws.services.cloudformation.model.DescribeStackEventsResult;
import com.amazonaws.services.cloudformation.model.StackEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Responsible for creating the CloudFormation stack needed to test the connector, and unwinding it once testing is
 * done.
 */
public class CloudFormationClient
{
    private static final Logger logger = LoggerFactory.getLogger(CloudFormationClient.class);

    private static final String CF_CREATE_RESOURCE_IN_PROGRESS_STATUS = "CREATE_IN_PROGRESS";
    private static final String CF_CREATE_RESOURCE_FAILED_STATUS = "CREATE_FAILED";
    private static final long sleepTimeMillis = 5000L;

    private final String stackName;
    private final AmazonCloudFormation client;

    public CloudFormationClient(String stackName)
    {
        this.stackName = stackName;
        this.client = AmazonCloudFormationClientBuilder.defaultClient();
    }

    /**
     * Creates a CloudFormation stack to build the infrastructure needed to run the integration tests (e.g., Database
     * instance, Lambda function, etc...). Once the stack is created successfully, the lambda function is registered
     * with Athena.
     * @param template
     */
    protected void createStack(String template)
    {
        logger.info("------------------------------------------------------");
        logger.info("Create CloudFormation stack: {}", stackName);
        logger.info("------------------------------------------------------");

        CreateStackRequest createStackRequest = new CreateStackRequest()
                .withStackName(stackName)
                .withTemplateBody(template)
                .withDisableRollback(true)
                .withCapabilities(Capability.CAPABILITY_NAMED_IAM);
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
        CreateStackResult result = client.createStack(createStackRequest);
        logger.info("Stack ID: {}", result.getStackId());

        DescribeStackEventsRequest describeStackEventsRequest = new DescribeStackEventsRequest()
                .withStackName(createStackRequest.getStackName());
        DescribeStackEventsResult describeStackEventsResult;

        // Poll status of stack until stack has been created or creation has failed
        while (true) {
            describeStackEventsResult = client.describeStackEvents(describeStackEventsRequest);
            StackEvent event = describeStackEventsResult.getStackEvents().get(0);
            String resourceId = event.getLogicalResourceId();
            String resourceStatus = event.getResourceStatus();
            logger.info("Resource Id: {}, Resource status: {}", resourceId, resourceStatus);
            if (!resourceId.equals(event.getStackName()) ||
                    resourceStatus.equals(CF_CREATE_RESOURCE_IN_PROGRESS_STATUS)) {
                try {
                    Thread.sleep(sleepTimeMillis);
                    continue;
                }
                catch (InterruptedException e) {
                    throw new RuntimeException("Thread.sleep interrupted: " + e.getMessage(), e);
                }
            }
            else if (resourceStatus.equals(CF_CREATE_RESOURCE_FAILED_STATUS)) {
                throw new RuntimeException(getCloudFormationErrorReasons(describeStackEventsResult.getStackEvents()));
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
            if (stackEvent.getResourceStatus().equals(CF_CREATE_RESOURCE_FAILED_STATUS)) {
                String errorMessage = String.format("Resource: %s, Reason: %s\n",
                        stackEvent.getLogicalResourceId(), stackEvent.getResourceStatusReason());
                errorMessageBuilder.append(errorMessage);
            }
        });

        return errorMessageBuilder.toString();
    }

    /**
     * Deletes a CloudFormation stack, and the lambda function registered with Athena.
     */
    protected void deleteStack()
    {
        logger.info("------------------------------------------------------");
        logger.info("Delete CloudFormation stack: {}", stackName);
        logger.info("------------------------------------------------------");

        try {
            DeleteStackRequest request = new DeleteStackRequest().withStackName(stackName);
            client.deleteStack(request);
        }
        catch (Exception e) {
            logger.error("Something went wrong... Manual resource cleanup may be needed!!!", e);
        }
    }
}
