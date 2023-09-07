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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import org.junit.Test;
import org.mockito.Mockito;
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
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Assert.assertEquals(" FROM \"default\".\"table\" ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM \"default\".\"schema\".\"table\" ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("0");
        Assert.assertEquals(new ArrayList<>(), builder.getPartitionWhereClauses(split));

        Split split1 = Mockito.mock(Split.class);
        Mockito.when(split1.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)).thenReturn("pf");
        Mockito.when(split1.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN)).thenReturn("col");
        Mockito.when(split1.getProperty(SqlServerMetadataHandler.PARTITION_NUMBER)).thenReturn("1");
        Assert.assertEquals(Collections.singletonList(" $PARTITION.pf(col) = 1"), builder.getPartitionWhereClauses(split1));

    }

    @Test
    public void testLimitClause()
    {
        Split split = Mockito.mock(Split.class);
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getLimit()).thenReturn(5L);
        Assert.assertEquals("", builder.appendLimitOffset(split, constraints));
    }

}
