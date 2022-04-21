/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connector.lambda.domain.Split;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME;

public class SaphanaQueryStringBuilderTest
{
    @Test
    public void testQueryBuilder()
    {
        Split split = Mockito.mock(Split.class);
        String expectedString1 = " FROM \"default\".\"table\" PARTITION (p0) ";
        String expectedString2 = " FROM \"default\".\"schema\".\"table\" PARTITION (p0) ";
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "p0"));
        Mockito.when(split.getProperty(Mockito.eq(BLOCK_PARTITION_COLUMN_NAME))).thenReturn("p0");
        SaphanaQueryStringBuilder builder = new SaphanaQueryStringBuilder("'");
        String fromClauseWithSplit1 = builder.getFromClauseWithSplit("default", "", "table", split);
        String fromClauseWithSplit2 = builder.getFromClauseWithSplit("default", "schema", "table", split);
        Assert.assertEquals(expectedString1, fromClauseWithSplit1);
        Assert.assertEquals(expectedString2, fromClauseWithSplit2);
    }
    @Test
    public void testGetPartitionWhereClauses()
    {
        List<String> expectedPartitionWhereClauseList1 = new ArrayList<>();
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(BLOCK_PARTITION_COLUMN_NAME, "p0"));
        Mockito.when(split.getProperty(Mockito.eq(BLOCK_PARTITION_COLUMN_NAME))).thenReturn("p0");

        SaphanaQueryStringBuilder builder = new SaphanaQueryStringBuilder("'");
        List<String> partitionWhereClauseList1 = builder.getPartitionWhereClauses(split);
        Assert.assertEquals(expectedPartitionWhereClauseList1, partitionWhereClauseList1);
    }
}
