/*-
 * #%L
 * athena-cloudwatch-metrics
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
package com.amazonaws.athena.connectors.cloudwatch.metrics;

import com.amazonaws.services.cloudwatch.model.Dimension;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class DimensionSerDeTest
{
    private static final Logger logger = LoggerFactory.getLogger(DimensionSerDeTest.class);
    private static final String EXPECTED_SERIALIZATION = "{\"dimensions\":[{\"name\":\"dim_name\",\"value\":\"dim_val\"}" +
            ",{\"name\":\"dim_name1\",\"value\":\"dim_val1\"},{\"name\":\"dim_name2\",\"value\":\"dim_val2\"}]}";

    @Test
    public void serializeTest()
    {
        List<Dimension> expected = new ArrayList<>();
        expected.add(new Dimension().withName("dim_name").withValue("dim_val"));
        expected.add(new Dimension().withName("dim_name1").withValue("dim_val1"));
        expected.add(new Dimension().withName("dim_name2").withValue("dim_val2"));
        String actualSerialization = DimensionSerDe.serialize(expected);
        logger.info("serializeTest: {}", actualSerialization);
        List<Dimension> actual = DimensionSerDe.deserialize(actualSerialization);
        assertEquals(EXPECTED_SERIALIZATION, actualSerialization);
        assertEquals(expected, actual);
    }
}
