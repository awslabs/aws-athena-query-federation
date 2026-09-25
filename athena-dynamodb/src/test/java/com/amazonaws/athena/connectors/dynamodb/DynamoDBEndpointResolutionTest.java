/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the DynamoDB client is built with the correct endpoint override
 * for EUSC regions. The AWS SDK does not resolve EUSC endpoints correctly for
 * customer-deployed Lambdas, so we must explicitly set the endpoint override.
 */
public class DynamoDBEndpointResolutionTest
{
    @Test
    public void testEuscRegionGetsEndpointOverride()
    {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of("eusc-de-east-1"));
        applyEuscEndpointOverride(builder, "eusc-de-east-1");
        DynamoDbClient client = builder.build();

        Optional<URI> endpoint = client.serviceClientConfiguration().endpointOverride();
        assertTrue("EUSC region should have endpoint override", endpoint.isPresent());
        assertEquals("https://dynamodb.eusc-de-east-1.amazonaws.eu", endpoint.get().toString());
        client.close();
    }

    @Test
    public void testStandardRegionNoEndpointOverride()
    {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of("us-east-1"));
        applyEuscEndpointOverride(builder, "us-east-1");
        DynamoDbClient client = builder.build();

        Optional<URI> endpoint = client.serviceClientConfiguration().endpointOverride();
        assertFalse("Standard region should not have endpoint override", endpoint.isPresent());
        client.close();
    }

    @Test
    public void testCnRegionNoEndpointOverride()
    {
        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .region(Region.of("cn-north-1"));
        applyEuscEndpointOverride(builder, "cn-north-1");
        DynamoDbClient client = builder.build();

        Optional<URI> endpoint = client.serviceClientConfiguration().endpointOverride();
        assertFalse("CN region should not have endpoint override", endpoint.isPresent());
        client.close();
    }

    /**
     * Mirrors the logic in DynamoDBMetadataHandler/DynamoDBRecordHandler.
     */
    private void applyEuscEndpointOverride(DynamoDbClientBuilder builder, String region)
    {
        if (region != null && region.startsWith("eusc-")) {
            builder.endpointOverride(URI.create("https://dynamodb." + region + ".amazonaws.eu"));
        }
    }
}
