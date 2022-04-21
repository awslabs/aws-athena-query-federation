/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.lambda.domain.Split;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Collections;

public class SynapseQueryStringBuilderTest
{
    SynapseQueryStringBuilder builder = new SynapseQueryStringBuilder("'");

    @Test
    public void testQueryBuilder()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");

        builder.getFromClauseWithSplit("default", "", "table", split);
        builder.getFromClauseWithSplit("default", "schema", "table", split);
        Assert.assertEquals(" FROM 'default'.'table' ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM 'default'.'schema'.'table' ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");
        Assert.assertEquals(new ArrayList<>(), builder.getPartitionWhereClauses(split));

        Split split1 = Mockito.mock(Split.class);
        Mockito.when(split1.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn("id");
        Mockito.when(split1.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("");
        Mockito.when(split1.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("100000");
        Assert.assertEquals(Collections.singletonList("id <= 100000"), builder.getPartitionWhereClauses(split1));

        Split split2 = Mockito.mock(Split.class);
        Mockito.when(split2.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn("id");
        Mockito.when(split2.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("100000");
        Mockito.when(split2.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("300000");
        Assert.assertEquals(Collections.singletonList("id > 100000 and id <= 300000"), builder.getPartitionWhereClauses(split2));

        Split split3 = Mockito.mock(Split.class);
        Mockito.when(split3.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn("id");
        Mockito.when(split3.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("300000");
        Mockito.when(split3.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("");
        Assert.assertEquals(Collections.singletonList("id > 300000"), builder.getPartitionWhereClauses(split3));

        Split split4 = Mockito.mock(Split.class);
        Mockito.when(split4.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn("id");
        Mockito.when(split4.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("");
        Mockito.when(split4.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("");
        Assert.assertEquals(Collections.emptyList(), builder.getPartitionWhereClauses(split4));
    }
}
