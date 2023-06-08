/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.lambda.proto.domain.Split;
import org.junit.Test;
import org.testng.Assert;

import java.util.ArrayList;

import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2Constants.QUOTE_CHARACTER;

public class DataLakeQueryStringBuilderTest
{
    static {
        System.setProperty("aws.region", "us-east-1");
    }

    @Test
    public void testQueryBuilder()
    {
        Split split = Split.newBuilder().putProperties("partition", "p0").build();
        DataLakeGen2QueryStringBuilder builder = new DataLakeGen2QueryStringBuilder(QUOTE_CHARACTER, new DataLakeGen2FederationExpressionParser(QUOTE_CHARACTER));
        Assert.assertEquals(" FROM \"default\".\"table\" ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM \"default\".\"schema\".\"table\" ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        DataLakeGen2QueryStringBuilder builder = new DataLakeGen2QueryStringBuilder(QUOTE_CHARACTER, new DataLakeGen2FederationExpressionParser(QUOTE_CHARACTER));

        Split split = Split.newBuilder().putProperties("partition", "p0").build();
        Assert.assertEquals(new ArrayList<>(), builder.getPartitionWhereClauses(split));
    }
}
