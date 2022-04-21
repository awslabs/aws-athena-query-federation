/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;
import com.amazonaws.athena.connector.lambda.domain.Split;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SnowflakeQueryStringBuilderTest
{
    @Mock
    Split split;

    @Test
    public void testQueryBuilderNew()
    {
        Split split = Mockito.mock(Split.class);
        SnowflakeQueryStringBuilder builder = new SnowflakeQueryStringBuilder("'");
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p1-p2-p3-p4-p5");
        builder.getFromClauseWithSplit("default", "", "table", split);
        builder.appendLimitOffset(split);
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        SnowflakeQueryStringBuilder builder = new SnowflakeQueryStringBuilder("'");
        List<String> fromClauseWithSplit = builder.getPartitionWhereClauses(split);
        List<String> expected = new ArrayList<>();
        Assert.assertEquals(expected, fromClauseWithSplit);
    }
}

