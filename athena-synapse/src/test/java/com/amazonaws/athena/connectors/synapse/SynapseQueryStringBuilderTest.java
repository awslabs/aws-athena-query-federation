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

import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import org.junit.Test;
import org.mockito.Mockito;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Collections;

import static com.amazonaws.athena.connectors.synapse.SynapseConstants.QUOTE_CHARACTER;

public class SynapseQueryStringBuilderTest
{
    SynapseQueryStringBuilder builder = new SynapseQueryStringBuilder(QUOTE_CHARACTER, new SynapseFederationExpressionParser(QUOTE_CHARACTER));

    @Test
    public void testQueryBuilder()
    {
        Split split = Split.newBuilder().putProperties("partition", "p0").build();

        builder.getFromClauseWithSplit("default", "", "table", split);
        builder.getFromClauseWithSplit("default", "schema", "table", split);
        Assert.assertEquals(" FROM \"default\".\"table\" ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM \"default\".\"schema\".\"table\" ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        Split split = Split.newBuilder().putProperties("partition", "p0").build();
        Assert.assertEquals(new ArrayList<>(), builder.getPartitionWhereClauses(split));

        Split split1 = Split.newBuilder()
            .putProperties(SynapseMetadataHandler.PARTITION_COLUMN, "id")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, "")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, "100000")
            .build();
        Assert.assertEquals(Collections.singletonList("id <= 100000"), builder.getPartitionWhereClauses(split1));

        Split split2 = Split.newBuilder()
            .putProperties(SynapseMetadataHandler.PARTITION_COLUMN, "id")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, "100000")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, "300000")
            .build();
        Assert.assertEquals(Collections.singletonList("id > 100000 and id <= 300000"), builder.getPartitionWhereClauses(split2));

        Split split3 = Split.newBuilder()
            .putProperties(SynapseMetadataHandler.PARTITION_COLUMN, "id")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, "300000")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, "")
            .build();
        Assert.assertEquals(Collections.singletonList("id > 300000"), builder.getPartitionWhereClauses(split3));

        Split split4 = Split.newBuilder()
            .putProperties(SynapseMetadataHandler.PARTITION_COLUMN, "id")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, "")
            .putProperties(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, "")
            .build();
        Assert.assertEquals(Collections.emptyList(), builder.getPartitionWhereClauses(split4));
    }
}
