/*-
 * #%L
 * athena-cloudwatch
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CloudwatchTableNameTest {
    private static final String LOG_GROUP = "TestGroup";
    private static final String LOG_STREAM = "TestStream";

    @Test
    public void constructorAndGetters_withValidInput_setsLogGroupAndLogStreamNames() {
        CloudwatchTableName tableName = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        assertEquals(LOG_GROUP, tableName.getLogGroupName());
        assertEquals(LOG_STREAM, tableName.getLogStreamName());
    }

    @Test
    public void toTableName_withValidInput_mapsLogGroupToSchemaAndLogStreamToTable() {
        CloudwatchTableName tableName = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        TableName result = tableName.toTableName();
        assertEquals(LOG_GROUP, result.getSchemaName());
        assertEquals(LOG_STREAM, result.getTableName());
    }

    @Test
    public void toString_withValidInput_formatsWithLogGroupAndLogStreamNames() {
        CloudwatchTableName tableName = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        String expected = "CloudwatchTableName{" +
                "logGroupName='" + LOG_GROUP + '\'' +
                ", logStreamName='" + LOG_STREAM + '\'' +
                '}';
        assertEquals(expected, tableName.toString());
    }

    @Test
    public void equals_withEqualAndUnequalObjects_matchesOnBothFieldsAndRejectsNullAndDifferentType() {
        CloudwatchTableName tableName1 = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        CloudwatchTableName tableName2 = new CloudwatchTableName(LOG_GROUP, LOG_STREAM);
        CloudwatchTableName tableName3 = new CloudwatchTableName("DifferentGroup", LOG_STREAM);
        CloudwatchTableName tableName4 = new CloudwatchTableName(LOG_GROUP, "DifferentStream");

        assertEquals(tableName1, tableName2);
        assertNotEquals(tableName1, tableName3);
        assertNotEquals(tableName1, tableName4);
        assertNotEquals(null, tableName1);
        assertNotEquals(new Object(), tableName1);
    }
}
