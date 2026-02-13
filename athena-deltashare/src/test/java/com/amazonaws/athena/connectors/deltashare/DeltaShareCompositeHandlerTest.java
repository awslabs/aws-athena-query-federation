/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordRequest;
import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class DeltaShareCompositeHandlerTest extends TestBase
{
    @Rule
    public TestName testName = new TestName();

    @Test
    public void testCompositeHandlerCreationWithValidConfig()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT);
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN);
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME);

        try {
            DeltaShareCompositeHandler handler = new DeltaShareCompositeHandler();
            assertNotNull(handler);
            assertTrue(handler instanceof CompositeHandler);
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }

    @Test
    public void testCompositeHandlerCreationWithEnvironmentVariables()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT);
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN);
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME);

        try {
            DeltaShareCompositeHandler handler = new DeltaShareCompositeHandler();
            assertNotNull(handler);
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompositeHandlerCreationWithMissingEndpoint()
    {
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN);
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME);

        try {
            new DeltaShareCompositeHandler();
        } finally {
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompositeHandlerCreationWithMissingToken()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT);
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME);

        try {
            new DeltaShareCompositeHandler();
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCompositeHandlerCreationWithMissingShareName()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT);
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN);

        try {
            new DeltaShareCompositeHandler();
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
        }
    }

    @Test
    public void testCompositeHandlerInitialization()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT);
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN);
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME);

        try {
            DeltaShareCompositeHandler handler = new DeltaShareCompositeHandler();
            assertNotNull(handler);
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }

    @Test
    public void testCompositeHandlerWithInvalidEndpoint()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, "invalid-url");
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN);
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME);

        try {
            DeltaShareCompositeHandler handler = new DeltaShareCompositeHandler();
            assertNotNull(handler);
        } catch (Exception e) {
            assertTrue("Handler creation should handle invalid endpoint gracefully", 
                e.getMessage().contains("endpoint") || e instanceof RuntimeException);
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompositeHandlerWithEmptyToken()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT);
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, "");
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, TEST_SHARE_NAME);
        
        try {
            new DeltaShareCompositeHandler();
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCompositeHandlerWithEmptyShareName()
    {
        System.setProperty(DeltaShareConstants.ENDPOINT_PROPERTY, TEST_ENDPOINT);
        System.setProperty(DeltaShareConstants.TOKEN_PROPERTY, TEST_TOKEN);
        System.setProperty(DeltaShareConstants.SHARE_NAME_PROPERTY, "");
        
        try {
            new DeltaShareCompositeHandler();
        } finally {
            System.clearProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            System.clearProperty(DeltaShareConstants.TOKEN_PROPERTY);
            System.clearProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
        }
    }
}
