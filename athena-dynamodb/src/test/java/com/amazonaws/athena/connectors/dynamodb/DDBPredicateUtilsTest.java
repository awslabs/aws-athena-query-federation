/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Tests DynamoDB utility methods relating to predicate handling.
 */
@RunWith(MockitoJUnitRunner.class)
public class DDBPredicateUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(DDBPredicateUtilsTest.class);

    @Test
    public void testAliasColumn()
    {
        logger.info("testAliasColumn - enter");

        assertEquals("Unexpected alias column value!", "#column_1",
                DDBPredicateUtils.aliasColumn("column_1"));
        assertEquals("Unexpected alias column value!", "#column__1",
                DDBPredicateUtils.aliasColumn("column__1"));
        assertEquals("Unexpected alias column value!", "#column_1",
                DDBPredicateUtils.aliasColumn("column-1"));
        assertEquals("Unexpected alias column value!", "#column_1_f3F",
                DDBPredicateUtils.aliasColumn("column-$1`~!@#$%^&*()-=+[]{}\\|;:'\",.<>/?f3F"));

        logger.info("testAliasColumn - exit");
    }
}
