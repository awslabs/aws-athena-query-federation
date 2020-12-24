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

import com.amazonaws.athena.connector.integ.data.ConnectorStackAttributes;
import com.amazonaws.athena.connector.integ.stacks.ConnectorStack;
import com.amazonaws.athena.connector.integ.stacks.ConnectorWithVpcStack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.core.Stack;

/**
 * Responsible for creating different types of Connector CloudFormation stacks depending on whether the connector
 * supports a VPC config or not.
 */
public class ConnectorStackFactory
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectorStackFactory.class);

    private final ConnectorStackAttributes attributes;
    private final boolean isSupportsVpcConfig;

    public ConnectorStackFactory(final ConnectorStackAttributes attributes)
    {
        this.attributes = attributes;
        this.isSupportsVpcConfig = attributes.getConnectorVpcAttributes().isPresent();
    }

    /**
     * Gets the Connector's CloudFormation stack.
     * @return Connector's stack: ConnectorWithVpcStack (supports a VPC config), or ConnectorStack (default).
     */
    public Stack getStack()
    {
        if (isSupportsVpcConfig) {
            logger.info("Stack Factory is using ConnectorWithVpcStack stack.");
            return new ConnectorWithVpcStack.Builder(attributes).build();
        }

        logger.info("Stack Factory is using ConnectorStack stack.");
        return new ConnectorStack.Builder(attributes).build();
    }
}
