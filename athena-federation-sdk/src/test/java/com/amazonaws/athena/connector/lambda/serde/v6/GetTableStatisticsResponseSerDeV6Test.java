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
import com.amazonaws.athena.connector.lambda.metadata.GetTableStatisticsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDeTest;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;
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
import static org.junit.jupiter.api.Assertions.*;

public class GetTableStatisticsResponseSerDeV6Test extends TypedSerDeTest<FederationResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(GetTableStatisticsResponseSerDeV6Test.class);
    private GetTableStatisticsResponse originalResponse;

    @Before
    public void beforeTest() throws IOException {
        String expectedSerDeFile = utils.getResourceOrFail("serde/v6", "GetTableStatisticsResponse.json");
        expectedSerDeText = utils.readAllAsString(expectedSerDeFile).trim();

        //Object
        // Setup test data
        TableName tableName = new TableName("db", "table");
        ColumnStatistic columnStat1 = new ColumnStatistic(
                Optional.of(1.0),
                Optional.of(100.0),
                Optional.empty(),
                Optional.of(20.0)
        );
        ColumnStatistic columnStat2 = new ColumnStatistic(
                Optional.of(0.0009),
                Optional.empty(),
                Optional.empty(),
                Optional.of(20.0)
        );

        originalResponse = new GetTableStatisticsResponse(
                "testCatalog",
                tableName,
                Optional.of(123456.0),
                Optional.of(987654.0),
                Map.of("col1", columnStat1, "col2", columnStat2)
        );

        expected = originalResponse;
    }

    @Test
    public void serialize() throws Exception {
        logger.info("serialize: enter");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        mapperV6.writeValue(outputStream, expected);

        String actual = new String(outputStream.toByteArray(), JsonEncoding.UTF8.getJavaName());
        logger.info("serialize: serialized text[{}]", actual);

        assertTrue(compareJsonIgnoringColumnOrder(actual, expectedSerDeText));
        expected.close();

        logger.info("serialize: exit");
    }

    @Test
    public void deserialize() throws IOException {
        logger.info("deserialize: enter");
        InputStream input = new ByteArrayInputStream(expectedSerDeText.getBytes());

        GetTableStatisticsResponse actual = (GetTableStatisticsResponse) mapperV6.readValue(input, FederationResponse.class);

        logger.info("deserialize: deserialized[{}]", actual);

        assertEquals(expected, actual);

        logger.info("deserialize: exit");
    }

    private boolean compareJsonIgnoringColumnOrder(String json1, String json2) throws Exception {
        JsonNode root1 = mapperV6.readTree(json1);
        JsonNode root2 = mapperV6.readTree(json2);

        // Compare all fields except columnsStatistics
        if (!compareJsonNodesExcept(root1, root2, "columnsStatistics")) {
            return false;
        }

        // Compare columnsStatistics separately
        JsonNode stats1 = root1.get("columnsStatistics");
        JsonNode stats2 = root2.get("columnsStatistics");

        if (stats1.size() != stats2.size()) {
            return false;
        }

        // Compare each column's statistics
        Iterator<String> fieldNames = stats1.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            if (!stats2.has(fieldName) || !stats1.get(fieldName).equals(stats2.get(fieldName))) {
                return false;
            }
        }

        return true;
    }

    private boolean compareJsonNodesExcept(JsonNode node1, JsonNode node2, String exceptField) {
        if (node1.getClass() != node2.getClass()) {
            return false;
        }

        if (node1.isObject()) {
            ObjectNode obj1 = (ObjectNode) node1;
            ObjectNode obj2 = (ObjectNode) node2;

            if (obj1.size() != obj2.size()) {
                return false;
            }

            Iterator<String> fieldNames = obj1.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                if (fieldName.equals(exceptField)) {
                    continue;
                }
                if (!obj2.has(fieldName) || !compareJsonNodesExcept(obj1.get(fieldName), obj2.get(fieldName), exceptField)) {
                    return false;
                }
            }
            return true;
        }

        return node1.equals(node2);
    }
}
