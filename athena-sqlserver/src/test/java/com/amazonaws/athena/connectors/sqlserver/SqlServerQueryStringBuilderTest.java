/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import org.junit.Test;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Collections;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;

public class SqlServerQueryStringBuilderTest
{
    static {
        System.setProperty("aws.region", "us-east-1");
    }

    @Test
    public void testQueryBuilder()
    {
        Split split = Split.newBuilder().putProperties("partition", "p0").build();
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Assert.assertEquals(" FROM \"default\".\"table\" ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM \"default\".\"schema\".\"table\" ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));

        Split split = Split.newBuilder().putProperties("partition", "0").build();
        Assert.assertEquals(new ArrayList<>(), builder.getPartitionWhereClauses(split));

        Split split1 = Split.newBuilder()
            .putProperties(SqlServerMetadataHandler.PARTITION_FUNCTION, "pf")
            .putProperties(SqlServerMetadataHandler.PARTITIONING_COLUMN, "col")
            .putProperties(SqlServerMetadataHandler.PARTITION_NUMBER, "1")
            .build();
        Assert.assertEquals(Collections.singletonList(" $PARTITION.pf(col) = 1"), builder.getPartitionWhereClauses(split1));

    }
}
