/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

import com.amazonaws.athena.connector.lambda.domain.Split;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.Arrays;

public class Db2QueryStringBuilderTest {
    @Mock
    Split split;

    @Test
    public void testQueryBuilder()
    {
        Split split = Mockito.mock(Split.class);
        Db2QueryStringBuilder builder = new Db2QueryStringBuilder("'");
        Assert.assertEquals(" FROM 'default'.'table' ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM 'default'.'schema'.'table' ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        Db2QueryStringBuilder builder = new Db2QueryStringBuilder("'");
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(Mockito.eq("PARTITION_NUMBER"))).thenReturn("0");
        Mockito.when(split.getProperty(Mockito.eq("PARTITIONING_COLUMN"))).thenReturn("PC");
        Assert.assertEquals(Arrays.asList(" DATAPARTITIONNUM(PC) = 0"), builder.getPartitionWhereClauses(split));
    }
}
