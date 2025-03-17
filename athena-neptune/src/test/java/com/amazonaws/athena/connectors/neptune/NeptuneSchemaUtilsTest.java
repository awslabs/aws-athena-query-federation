/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneSchemaUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneSchemaUtilsTest.class);

    private final String COMPONENT_TYPE = "vertex";

    @Test
    public void getSchemaFromResults() {
        logger.info("getSchemaFromResults - enter");
        List<String> listObj = new ArrayList<>();
        listObj.add("Test");
        listObj.add("Test1");

        Map<String, Object> mapObj = new HashMap<>();
        mapObj.put("col1", "String");
        mapObj.put("col2", 1);
        mapObj.put("col3", 10.33);
        mapObj.put("col4", true);
        mapObj.put("col5", new BigInteger("12345678901234567890"));
        mapObj.put("col6", new Date());
        mapObj.put("col7", listObj);
        mapObj.put("col8", null);

        Map<String, Object> objectMap = Collections.unmodifiableMap(mapObj);
        Schema schema = NeptuneSchemaUtils.getSchemaFromResults(objectMap, COMPONENT_TYPE, "test");

        assertEquals(schema.getFields().size(), objectMap.size());
        assertEquals("Utf8", schema.findField("col1").getType().toString());
        assertEquals("Int(32, true)", schema.findField("col2").getType().toString());
        assertEquals("FloatingPoint(DOUBLE)", schema.findField("col3").getType().toString());
        assertEquals("Bool", schema.findField("col4").getType().toString());
        assertEquals("Int(64, true)", schema.findField("col5").getType().toString());
        assertEquals("Date(MILLISECOND)", schema.findField("col6").getType().toString());
        assertEquals("Utf8", schema.findField("col7").getType().toString());
        assertEquals("Utf8", schema.findField("col8").getType().toString());

        assertEquals(COMPONENT_TYPE, schema.getCustomMetadata().get(Constants.SCHEMA_COMPONENT_TYPE));
        logger.info("getSchemaFromResults - exit");
    }
}
