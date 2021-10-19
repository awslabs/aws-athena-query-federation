/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.athena.connectors.elasticsearch;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class is used to test the ElasticsearchGlueTypeMapper class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchGlueTypeMapperTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchGlueTypeMapperTest.class);

    @Test
    public void getFieldTest()
    {
        logger.info("getFieldTest: enter");

        ElasticsearchGlueTypeMapper glueTypeMapper = new ElasticsearchGlueTypeMapper();
        Field field;

        // Test scaling factor with decimal point.
        field = glueTypeMapper.getField("myscaled", "SCALED_FLOAT(100.0)");
        logger.info("Field: {}, metadata: {}", field, field.getMetadata());
        assertEquals("Field's name is invalid: ", "myscaled", field.getName());
        assertEquals("Field's type is invalid: ", Types.MinorType.BIGINT.getType(), field.getType());
        assertEquals("Field's scaling factor is invalid: ",
                "100.0", field.getMetadata().get("scaling_factor"));

        // Test scaling factor w/o decimal point.
        field = glueTypeMapper.getField("myscaled", "SCALED_FLOAT(100)");
        logger.info("Field: {}, metadata: {}", field, field.getMetadata());
        assertEquals("Field's name is invalid: ", "myscaled", field.getName());
        assertEquals("Field's type is invalid: ", Types.MinorType.BIGINT.getType(), field.getType());
        assertEquals("Field's scaling factor is invalid: ",
                "100", field.getMetadata().get("scaling_factor"));

        // Test an INT field
        field = glueTypeMapper.getField("myscaled", "INT");
        logger.info("Field: {}, metadata: {}", field, field.getMetadata());
        assertEquals("Field's name is invalid: ", "myscaled", field.getName());
        assertEquals("Field's type is invalid: ", Types.MinorType.INT.getType(), field.getType());
        assertTrue("Fields metadata is invalid: ", field.getMetadata().isEmpty());

        logger.info("getFieldTest: exit");
    }
}
