/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v6;

import com.amazonaws.athena.connector.lambda.data.ColumnStatistic;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableStatisticsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableStatisticsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDeTest;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetTableStatisticsRequestSerDeV6Test extends TypedSerDeTest<FederationRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(GetTableStatisticsRequestSerDeV6Test.class);

    @Before
    public void beforeTest() throws IOException {
        String expectedSerDeFile = utils.getResourceOrFail("serde/v6", "GetTableStatisticsRequest.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();

        //Object
        // Setup test data
        TableName tableName = new TableName("db", "table");
        expected = new GetTableStatisticsRequest(
                federatedIdentity,
                "FAKE_QUERY_ID",
                "testCatalog",
                tableName
        );
    }

    @Test
    public void serialize() throws Exception {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        mapperV6.writeValue(outputStream, expected);

        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("serialize: serialized text[{}]", actual);

        assertEquals(expectedSerDeText, actual);
        expected.close();

        logger.info("serialize: exit");
    }

    @Test
    public void deserialize() throws IOException {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        GetTableStatisticsRequest actual = (GetTableStatisticsRequest) mapperV6.readValue(input, FederationRequest.class);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }
}
